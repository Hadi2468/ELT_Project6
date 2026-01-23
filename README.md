# ELT_Project 6: Marketing & Calendly Data Engineering

## 🧠 Overview
This project implements a **production-ready ELT data engineering pipeline** using **AWS Glue, Delta Lake, and Amazon S3** to process marketing spend data and Calendly booking events.

The pipeline follows a **Bronze → Silver → Gold** architecture with explicit data contracts, data quality checks, and incremental processing to ensure reliability, scalability, and analytics readiness.
### The pipeline enables:
- Ingestion of raw Calendly webhook and marketing spend data (🟤 Bronze layer)
- Data cleaning, validation, transformation, and deduplication (⚪ Silver layer)
- Curated, analytics-ready Delta tables (🟡 Gold layer)
- Incremental processing, late-arriving data handling, and reliable orchestration via Step Functions 

## 🎯 Business Objective  
Organizations need insights into marketing campaigns and meeting scheduling to optimize spend and improve conversions.  

This project addresses:    
✔️ Tracking daily Calendly bookings from Calendly per marketing channel  
✔️ Calculating Cost Per Booking (CPB) to identify cost-efficient campaigns  
✔️ Understanding booking trends, channel attribution, and employee workload  
✔️ Providing a robust, Delta Lake-based data pipeline for analytics and dashboards  

## 🧩 Architecture
![System-Design](project6_system_design.png)
```text
Calendly Webhooks & Marketing Spend Data
   ↓ (Incremental ETL)
AWS Glue
   ↓
Bronze Layer (Raw JSON in S3)
   ↓
Silver Layer (Validated, Flattened, Cleaned, Deduplicated)
   ↓
Gold Layer (Curated Delta Lake Tables)
   ↓
Analytics & Visualization (Athena + Streamlit)
```
## 🛢️ Data Sources
1️⃣ **Calendly Webhooks:**  
Event-based booking data (`invitee.created`) including:
- Attendee information
- Scheduled event details
- Timezone and timestamps
- UTM parameters for attribution
 
📄 Sample Invitee Event: [6705dded-8f97-4cea-8d12-491e10d8bcd5.json](6705dded-8f97-4cea-8d12-491e10d8bcd5.json)


2️⃣ **Marketing Spend Data:**  
Daily spend per campaign in JSON from S3.  

Supported channels:
- Facebook Paid Ads
- YouTube Paid Ads
- TikTok Paid Ads

📄 Sample marketing spend record (YouTube)
```json
{
   "date": "2026-01-14",
   "channel": "youtube_paid_ads",
   "spend": 827.67
}
```

## ➡️ Data Schemas (Data Contracts)
### 🟫 Bronze Layer – Raw Events (Append-Only)
- event_created_at: timestamp
- invitee_id: string
- invitee_email: string
- scheduled_event_start_time: timestamp
- event_type: string
- utm_source: string
- utm_campaign: string
- raw_payload: struct (JSON)

Bronze data preserves source-system fidelity and is not mutated.

### ⬜ Silver Layer – Cleaned & Validated
- event_date: date (Partition Column)
- invitee_id: string (Primary Key)
- booking_date: date
- channel: string
- employee_email: string
- event_start_time: timestamp
- ingestion_date: date
- is_valid_record: boolean
 
#### Applied transformations:
- Type casting and schema enforcement
- Null handling and field standardization
- Deduplication using Primary key
- Data validation rules

### 🟨 Gold Layer – Analytics Ready
- booking_date: date (Partition Column)
- channel: string
- booking_hour: timestamp
- booking_week: int
- total_bookings: int
- total_spend: double
- cost_per_booking: double

Designed for BI tools, dashboards, and ad-hoc SQL queries.


## ✅ Data Quality Checks
Data quality validations are enforced primarily in the Silver and Gold layers:
- Not-null checks on primary keys (invitee_id)
- Timestamp sanity checks (event_time ≤ ingestion_time)
- Deduplication of duplicate booking events
- Spend values ≥ 0
- Booking counts ≥ 0

Invalid or rejected records can be isolated for monitoring and troubleshooting.


## 🧩 Partition Strategy (Delta Lake)
Delta tables are partitioned by booking_date to:
- Minimize data scanned during time-based queries
- Improve Spark and Athena query performance
- Support efficient incremental loads and reprocessing

Partitioning is intentionally kept low-cardinality to avoid small-file and metadata issues.

## ⏱️ Late-Arriving Data Handling

Late-arriving Calendly events are handled using:
- Separation of event time and ingestion time
- Incremental processing windows (e.g., reprocessing last N days)
- Delta Lake MERGE (UPSERT) operations in Silver and Gold layers

This design allows historical partitions to be updated without full reloads.


## 📊 Key Metrics & Business Insights
✅ Daily Calls Booked by Source – count of Calendly bookings per source per day  
✅ Cost Per Booking (CPB) by Channel – total spend divided by total bookings  
✅ Booking Trends Over Time – monitor daily/weekly booking volume per source  
✅ Channel Attribution – attribute bookings to campaigns via UTM parameters  
✅ Booking Volume by Time Slot / Day of Week – identify peak booking periods  
✅ Employee Meeting Load – weekly average meetings per employee  


## 📄 Sample Data
```
Calendly Webhook Event (Bronze Layer):     invitee_created_202601114.json   
Flattened & Cleaned Event (Silver Layer):  silver_event_202601114.delta   
Curated Delta Table (Gold Layer):          gold_event_202601114.delta
``` 

## 🌟 Results
☑️ Near real-time streaming ingestion of Calendly events   
☑️ End-to-end ELT pipeline successfully implemented  
☑️ Reliable Bronze → Silver → Gold transformations   
☑️ Explicit schemas and data quality enforcement  
☑️ Incremental and late-arriving data support  
☑️ Interactive Streamlit dashboard for analytics  
☑️ Serverless orchestration with Step Functions  
☑️ CI/CD pipeline for automated deployment  


## 🔜 Future Enhancements
- Automated anomaly detection on booking trends  
- Data quality monitoring dashboards  
- SLA alerts for pipeline failures or data delays  


## 🧑🏻‍💻 Author
**Hadi Hosseini**  
Data Engineer | AI/ML Engineer | Biomedical Data Scientist  
➡️ www.linkedin.com/in/hadi468
