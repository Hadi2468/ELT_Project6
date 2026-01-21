# ============================================
# Glue ETL Job: project6-bronze.py
# ============================================

# ------------------------
# Job Parameters
# ------------------------
# Key: --BRONZE_BUCKET
# Value: project6-bronze-bucket
# ------------------------

import sys
import json
import logging
import boto3
import requests
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql import Row

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
COMPANIES = [
    "youtube_paid_ads",
    "facebook_paid_ads",
    "tiktok_paid_ads"
]

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
index_response = requests.get(INDEX_URL)
index_response.raise_for_status()
files = index_response.json()["files"]

# ------------------------
# Save index file to Bronze
# ------------------------
logger.info("💾 Saving file_index.json to Bronze")

s3.put_object(
    Bucket=BRONZE_BUCKET,
    Key="marketing/file_index.json",
    Body=index_response.text,
    ContentType="application/json"
)

# ------------------------
# Build dataset (ONE row per company per day)
# ------------------------
records = []

for file_name in files:
    spend_date = file_name.replace("spend_data_", "").replace(".json", "")
    logger.info(f"📥 Processing {file_name}")

    data = requests.get(DATA_URL.format(file_name=file_name)).json()

    for company in COMPANIES:
        row = next(
            r for r in data
            if r["channel"] == company and r["date"] == spend_date
        )

        records.append(Row(
            date=row["date"],
            channel=row["channel"],
            spend=row["spend"]
        ))

df = spark.createDataFrame(records)

logger.info(f"✅ Total records prepared: {df.count()}")

# ------------------------
# Write ONE JSON ARRAY per day (flat)
# ------------------------
dates = sorted({r["date"] for r in df.collect()})

for date in dates:
    logger.info(f"💾 Writing spend-date-{date}.json")

    daily_df = df.filter(df.date == date)

    json_array = [
        json.loads(line)
        for line in daily_df.toJSON().collect()
    ]

    s3_key = f"marketing/spend-date-{date}.json"

    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=s3_key,
        Body=json.dumps(json_array, indent=2),
        ContentType="application/json"
    )

    logger.info(f"✅ Written {s3_key}")

# ------------------------
# Cleanup $folder$ artifacts (safety)
# ------------------------
logger.info("🧹 Cleaning up $folder$ artifacts")

paginator = s3.get_paginator("list_objects_v2")
for page in paginator.paginate(Bucket=BRONZE_BUCKET, Prefix="marketing/"):
    keys = [
        {"Key": obj["Key"]}
        for obj in page.get("Contents", [])
        if "$folder$" in obj["Key"]
    ]
    if keys:
        s3.delete_objects(Bucket=BRONZE_BUCKET, Delete={"Objects": keys})

logger.info("🎯 Job completed successfully")