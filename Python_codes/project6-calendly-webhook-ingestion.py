# =========================================================
# Lambda Function: project6-calendl-webhook-ingestion.py
# =========================================================

# ------------------------
# Environment variables
# ------------------------
# Key: BRONZE_BUCKET
# Value: project6-bronze-bucket
# ------------------------

import sys
import logging
import boto3
import re
import requests
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql import Row
from pyspark.sql.functions import lit

# ------------------------
# Job Parameters
# ------------------------
args = getResolvedOptions(sys.argv, ["BRONZE_BUCKET"])
BRONZE_BUCKET = args["BRONZE_BUCKET"]

# ------------------------
# Spark / Glue Context
# ------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

s3 = boto3.client("s3")

# ------------------------
# Logging
# ------------------------
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] | %(asctime)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger()

# ------------------------
# Constants
# ------------------------
INDEX_URL = (
    "https://dea-data-bucket.s3.us-east-1.amazonaws.com/"
    "calendly_spend_data/file_index.json"
)

DATA_URL = (
    "https://dea-data-bucket.s3.us-east-1.amazonaws.com/"
    "calendly_spend_data/{file_name}"
)

# ------------------------
# Load index
# ------------------------
logger.info("📄 Loading file index")
files = requests.get(INDEX_URL).json()["files"]

# ------------------------
# Save index file to Bronze
# ------------------------
logger.info("💾 Saving file_index.json to Bronze")

index_content = requests.get(INDEX_URL).text
s3.put_object(
    Bucket=BRONZE_BUCKET,
    Key="marketing/file_index.json",
    Body=index_content,
    ContentType="application/json"
)

rows = []

# ------------------------
# Read & normalize data
# ------------------------
for file_name in files:
    spend_date = file_name.replace("spend_data_", "").replace(".json", "")
    data = requests.get(DATA_URL.format(file_name=file_name)).json()

    for r in data:
        rows.append(
            Row(
                date=r["date"],
                channel=r["channel"],
                spend=float(r["spend"]),
                spend_date=spend_date
            )
        )

if not rows:
    raise Exception("❌ No marketing data found")

final_df = spark.createDataFrame(rows)
logger.info(f"✅ Total records: {final_df.count()}")

# ------------------------
# Write Bronze (date-first)
# ------------------------
(
    final_df
    .coalesce(1)    # one file per day
    .write
    .mode("overwrite")
    .partitionBy("spend_date")
    .json(f"s3://{BRONZE_BUCKET}/marketing/")
)

# ------------------------
# Rename JSON files to include date
# ------------------------
logger.info("✏️ Renaming Spark output files")

dates = (
    final_df
    .select("spend_date")
    .distinct()
    .rdd.flatMap(lambda x: x)
    .collect()
)

for spend_date in dates:
    prefix = f"marketing/spend_date={spend_date}/"
    response = s3.list_objects_v2(Bucket=BRONZE_BUCKET, Prefix=prefix)

    for obj in response.get("Contents", []):
        key = obj["Key"]
        if re.search(r"part-.*\.json$", key):
            new_key = key.replace(
                "part-00000",
                f"spend-date-{spend_date}"
            )
            s3.copy_object(
                Bucket=BRONZE_BUCKET,
                CopySource={"Bucket": BRONZE_BUCKET, "Key": key},
                Key=new_key
            )
            s3.delete_object(Bucket=BRONZE_BUCKET, Key=key)
            logger.info(f"Renamed {key} → {new_key}")

# ------------------------
# Cleanup $folder$ artifacts
# ------------------------
logger.info("🧹 Cleaning up $folder$ artifacts")

paginator = s3.get_paginator("list_objects_v2")
for page in paginator.paginate(Bucket=BRONZE_BUCKET, Prefix="marketing/"):
    to_delete = [
        {"Key": obj["Key"]}
        for obj in page.get("Contents", [])
        if "$folder$" in obj["Key"]
    ]
    if to_delete:
        s3.delete_objects(
            Bucket=BRONZE_BUCKET,
            Delete={"Objects": to_delete}
        )

logger.info("🎯 Bronze job completed successfully")
