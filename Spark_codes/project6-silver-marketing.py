# ============================================
# EMR ETL Job: project6-silver-marketing.py
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

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import DoubleType, StringType

# =========================================================
# Logger setup
# =========================================================
logger = logging.getLogger("project6-bronze-silver")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("[%(levelname)s] | %(asctime)s | %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# =========================================================
# Custom Exceptions
# =========================================================
class BronzeReadError(Exception):
    pass

class SilverWriteError(Exception):
    pass

# =========================================================
# Job parameters
# =========================================================
if len(sys.argv) != 4:
    logger.error("❌ Usage: spark-submit project6-bronze-silver.py <BRONZE_BUCKET> <BRONZE_PREFIX> <SILVER_BUCKET>")
    sys.exit(1)

BRONZE_BUCKET = sys.argv[1]
BRONZE_PREFIX = sys.argv[2]
SILVER_BUCKET = sys.argv[3]

logger.info(f"🧺 Bronze bucket: {BRONZE_BUCKET}")
logger.info(f"🗑️ Silver bucket: {SILVER_BUCKET}")

# =========================================================
# Spark session
# =========================================================
spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

# Delta configs
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")

# =========================================================
# Read Bronze data
# =========================================================
bronze_path = f"s3://{BRONZE_BUCKET}/{BRONZE_PREFIX}"
logger.info(f"📖 Reading Bronze data from {bronze_path}")

try:
    bronze_df = spark.read.option("multiline", "true").json(bronze_path)
    logger.info(f"✅ Bronze records: {bronze_df.count()}")
except Exception as e:
    logger.error(f"❌ Failed to read Bronze data: {str(e)}")
    raise BronzeReadError(str(e))

# =========================================================
# Silver transformations
# =========================================================
logger.info("🔀 Applying Silver transformations")

silver_df = (
    bronze_df
    .withColumn("channel", regexp_replace(col("channel"), "_paid_ads", ""))
    .withColumn("date", col("date").cast(StringType()))
    .withColumn("spend_date", col("date").cast(StringType()))
    .withColumn("spend", col("spend").cast(DoubleType()))
    .select("date", "spend_date", "channel", "spend")
)

silver_df = silver_df.dropna(subset=["date", "spend_date", "channel", "spend"])
logger.info(f"✅ Silver cleaned records: {silver_df.count()}")

# =========================================================
# Write Silver Delta table
# =========================================================
silver_path = f"s3://{SILVER_BUCKET}/fact_marketing"
logger.info(f"💾 Writing Silver Delta table to {silver_path}")

try:
    silver_df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("date") \
        .save(silver_path)

    logger.info("✅ Silver Delta table written successfully")

except Exception as e:
    logger.error(f"❌ Failed to write Silver Delta data: {str(e)}")
    raise SilverWriteError(str(e))

logger.info("✅ EMR Bronze → Silver job completed successfully")
