# ============================================
# EMR ETL Job: project6-gold.py
# ============================================

# ------------------------------------------------------------------------------------------------------------------------
# Job Parameters
# ------------------------------------------------------------------------------------------------------------------------
# "sparkSubmitParameters": "--jars  s3://project6-jars/delta-core_2.12-2.1.0.jar,
#                                   s3://project6-jars/hadoop-aws-3.3.4.jar,
#                                   s3://project6-jars/aws-java-sdk-bundle-1.12.538.jar 
#                           --conf  spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension 
#                           --conf  spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog 
#                           --conf  spark.sql.sources.partitionOverwriteMode=dynamic 
#                           --conf  spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore"
# ------------------------------------------------------------------------------------------------------------------------



# ============================================
# EMR ETL Job: project6-gold.py
# ============================================

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, weekofyear, when, count, sum as _sum,
    hour, dayofweek, regexp_extract, lit
)
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank

# =========================================================
# Logger setup
# =========================================================
logger = logging.getLogger("project6-silver-gold-emr")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("[%(levelname)s] | %(asctime)s | %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# =========================================================
# Custom Exceptions
# =========================================================
class GoldReadError(Exception):
    pass

class GoldWriteError(Exception):
    pass

# =========================================================
# Job parameters
# =========================================================
if len(sys.argv) != 3:
    logger.error("❌ Usage: spark-submit project6-gold.py <SILVER_BUCKET> <GOLD_BUCKET>")
    sys.exit(1)

SILVER_BUCKET = sys.argv[1]
GOLD_BUCKET = sys.argv[2]

logger.info(f"🗑️ Silver bucket: {SILVER_BUCKET}")
logger.info(f"🏆 Gold bucket: {GOLD_BUCKET}")

# =========================================================
# Spark session
# =========================================================
spark = SparkSession.builder.appName("project6-silver-gold-emr").getOrCreate()
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set(
    "spark.delta.logStore.class",
    "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore"
)

# =========================================================
# Static Channel Dimension
# =========================================================
channels_df = spark.createDataFrame(
    [("facebook",), ("youtube",), ("tiktok",), ("others",)],
    ["channel"]
)

# =========================================================
# Read Silver tables
# =========================================================
try:
    fact_events = spark.read.format("delta").load(f"s3://{SILVER_BUCKET}/fact_events")
    fact_marketing = spark.read.format("delta").load(f"s3://{SILVER_BUCKET}/fact_marketing")
    logger.info("✅ Silver tables loaded successfully")
except Exception as e:
    raise GoldReadError(str(e))

# =========================================================
# Enrich Events
# =========================================================
events_enriched = (
    fact_events
    .filter(col("data_event") == "invitee.created")
    .withColumn(
        "channel",
        when(col("data_payload_scheduled_event_event_type") ==
             "https://api.calendly.com/event_types/d639ecd3-8718-4068-955a-436b10d72c78", "facebook")
        .when(col("data_payload_scheduled_event_event_type") ==
              "https://api.calendly.com/event_types/dbb4ec50-38cd-4bcd-bbff-efb7b5a6f098", "youtube")
        .when(col("data_payload_scheduled_event_event_type") ==
              "https://api.calendly.com/event_types/bb339e98-7a67-4af2-b584-8dbf95564312", "tiktok")
        .otherwise("others")
    )
    .withColumn("booking_date", to_date(col("data_payload_scheduled_event_start_time")))
)

# =========================================================
# GOLD 1: Daily Bookings by Channel
# =========================================================
all_dates = events_enriched.select("booking_date").distinct()
all_channels_dates = channels_df.crossJoin(all_dates)

daily_counts = (
    events_enriched
    .groupBy("booking_date", "channel")
    .agg(count("*").alias("bookings_count"))
)

gold_daily_bookings = (
    all_channels_dates
    .join(daily_counts, ["booking_date", "channel"], "left")
    .fillna({"bookings_count": 0})
    .withColumn("booking_week", weekofyear(col("booking_date")))
)

# =========================================================
# GOLD 2: CPB by Channel
# =========================================================
real_channels = ["facebook", "youtube", "tiktok"]

# Bookings count per channel
bookings_by_channel = events_enriched.groupBy("channel").agg(count("*").alias("total_bookings_count"))

# Spend for real channels
spend_real = fact_marketing.groupBy("channel").agg(_sum("spend").alias("total_spend"))

# Spend for "others" = total spend of channels NOT in real_channels
others_spend_sum = fact_marketing.filter(~col("channel").isin(real_channels)) \
    .agg(_sum("spend").alias("total_spend")) \
    .collect()[0]["total_spend"] or 0.0

others_spend = spark.createDataFrame([(others_spend_sum, "others")], ["total_spend", "channel"])

# Combine all spend
spend_by_channel = spend_real.unionByName(others_spend, allowMissingColumns=True)

# Merge bookings & spend, fill missing channels
cpb_by_channel = (
    channels_df
    .join(bookings_by_channel, "channel", "left")
    .join(spend_by_channel, "channel", "left")
    .fillna({"total_bookings_count": 0, "total_spend": 0.0})
    .withColumn(
        "cpb",
        when(col("total_bookings_count") == 0, 0)
        .otherwise(col("total_spend") / col("total_bookings_count"))
    )
)

# =========================================================
# GOLD 3: Channel Attribution (Leaderboard)
# =========================================================
volume_window = Window.orderBy(col("total_bookings_count").desc())
cpb_window = Window.orderBy(col("cpb").asc_nulls_last())

gold_channel_attribution = (
    cpb_by_channel
    .withColumn("rank_by_volume", dense_rank().over(volume_window))
    .withColumn("rank_by_cpb", dense_rank().over(cpb_window))
)

# =========================================================
# GOLD 4: Booking Time Analysis
# =========================================================
gold_booking_time = (
    events_enriched
    .withColumn("booking_hour", hour(col("data_payload_scheduled_event_start_time")))
    .withColumn("booking_day_of_week", dayofweek(col("data_payload_scheduled_event_start_time")))
    .groupBy("booking_hour", "booking_day_of_week", "channel")
    .agg(count("*").alias("bookings_count"))
    .join(channels_df, "channel", "right")
    .fillna({"bookings_count": 0})
)

# =========================================================
# GOLD 5: Meeting Load per Employee
# =========================================================
gold_meeting_load = (
    events_enriched
    .withColumn(
        "user_id",
        regexp_extract(
            col("data_payload_scheduled_event_event_memberships_user"),
            r"users/([a-zA-Z0-9-]+)",
            1
        )
    )
    .groupBy("user_id")
    .agg(count("*").alias("total_meetings_count"))
)

# =========================================================
# Write Gold tables (Delta)
# =========================================================
gold_tables = {
    "gold_daily_bookings_by_channel": gold_daily_bookings,
    "gold_cpb_by_channel": cpb_by_channel,
    "gold_channel_attribution": gold_channel_attribution,
    "gold_booking_time_analysis": gold_booking_time,
    "gold_meeting_load_per_employee": gold_meeting_load,
}

try:
    for table_name, df in gold_tables.items():
        path = f"s3://{GOLD_BUCKET}/{table_name}"
        df.write.format("delta").mode("overwrite").save(path)
        logger.info(f"💾 Wrote Delta table: {table_name}")

except Exception as e:
    raise GoldWriteError(str(e))

# =========================================================
# Register Gold Delta Tables in Glue Catalog
# =========================================================
logger.info("📚 Registering Gold Delta tables in Glue Catalog")

GOLD_DB = "project6_gold_db"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DB}")

for table_name in gold_tables.keys():
    table_path = f"s3://{GOLD_BUCKET}/{table_name}"
    logger.info(f"📝 Registering table: {GOLD_DB}.{table_name}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_DB}.{table_name}
        USING DELTA
        LOCATION '{table_path}'
        TBLPROPERTIES (
            'delta.minReaderVersion'='2',
            'delta.minWriterVersion'='5'
        )
    """)

logger.info("✅ All Gold tables successfully written and registered in Glue Catalog")
