# ============================================
# EMR ETL Job: project6-silver-events.py
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
from pyspark.sql.functions import col, input_file_name, regexp_extract, explode_outer
from pyspark.sql.types import StructType, ArrayType

# =========================================================
# Logger setup
# =========================================================
logger = logging.getLogger("project6-silver-events")
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
    logger.error("❌ Usage: spark-submit project6-silver-events.py <BRONZE_BUCKET> <BRONZE_PREFIX> <SILVER_BUCKET>")
    sys.exit(1)

BRONZE_BUCKET = sys.argv[1]
BRONZE_PREFIX = sys.argv[2]  # e.g., "events/"
SILVER_BUCKET = sys.argv[3]

logger.info(f"🧺 Bronze bucket: {BRONZE_BUCKET}")
logger.info(f"🗑️ Silver bucket: {SILVER_BUCKET}")

# =========================================================
# Spark session
# =========================================================
spark = SparkSession.builder.appName("EventsBronzeToSilver").getOrCreate()

# Delta configs
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")

# =========================================================
# Read Bronze data
# =========================================================
bronze_path = f"s3://{BRONZE_BUCKET}/{BRONZE_PREFIX}"
logger.info(f"📖 Reading Bronze event data from {bronze_path}")

try:
    bronze_df = spark.read.option("multiline", "true").json(bronze_path)

    # Extract event_date from folder path
    bronze_df = bronze_df.withColumn(
        "event_date",
        regexp_extract(input_file_name(), r"event_date=([0-9\-]+)", 1)
    )

    logger.info(f"✅ Bronze event records: {bronze_df.count()}")

except Exception as e:
    logger.error(f"❌ Failed to read Bronze event data: {str(e)}")
    raise BronzeReadError(str(e))

# =========================================================
# Recursive flatten function
# =========================================================
def flatten_df(df):
    while True:
        complex_cols = [f.name for f in df.schema.fields 
                        if isinstance(f.dataType, (StructType, ArrayType))]
        if not complex_cols:
            break
        for c in complex_cols:
            dtype = dict(df.dtypes)[c]
            if dtype.startswith("struct"):
                expanded = [col(f"{c}.{k}").alias(f"{c}_{k}") for k in df.select(c + ".*").columns]
                df = df.select("*", *expanded).drop(c)
            elif dtype.startswith("array"):
                # Explode arrays safely
                df = df.withColumn(c, explode_outer(col(c)))
    return df

# =========================================================
# Flatten Bronze events
# =========================================================
logger.info("🔀 Flattening Bronze event data")
flattened_df = flatten_df(bronze_df)
logger.info(f"✅ Silver events flattened records: {flattened_df.count()}")

# =========================================================
# Write Silver Delta table
# =========================================================
silver_path = f"s3://{SILVER_BUCKET}/fact_events"
logger.info(f"💾 Writing Silver Delta table to {silver_path}")

try:
    flattened_df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("event_date") \
        .save(silver_path)

    logger.info("✅ Silver Delta table written successfully")

except Exception as e:
    logger.error(f"❌ Failed to write Silver Delta data: {str(e)}")
    raise SilverWriteError(str(e))

logger.info("✅ EMR Bronze → Silver events job completed successfully")
