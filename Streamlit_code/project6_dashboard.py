# ========================================================
# Project 6 Streamlit Dashboard (Gold Layer via Pandas)
# ========================================================

import streamlit as st
import pandas as pd
import plotly.express as px
import altair as alt
import json
import boto3
from botocore.exceptions import ClientError

# ============================================
# ⚙️ Streamlit Page Layout
# ============================================
st.set_page_config(page_title="Project6 Dashboard", layout="wide")
st.title("📊 Marketing & Booking Analytics")
st.markdown("---")

# ============================================
# Sidebar
# ============================================
st.sidebar.header("☰ Metrics")
menu = st.sidebar.radio(
    "🔍 **Select Dashboard**",
    [
        "CI/CD Configuration",
        "Daily Calls Booked by Channel",
        "CPB by Channel",
        "Channel Attribution Leaderboard",
        "Booking Time Analysis",
        "Meeting Load per Employee"
    ]
)

# ============================================
# 0️⃣ Check menu option
# ============================================
# ============================================
# Trigger Project 6 Step Function
# ============================================
def trigger_state_machine(state_machine_arn, input_dict=None):
    try:
        sf_client = boto3.client("stepfunctions", region_name="us-east-1")

        response = sf_client.start_execution(
            stateMachineArn=state_machine_arn,
            input=json.dumps(input_dict or {})  # must be JSON string
        )

        st.success(f"✅ Project 6 orchestration started!")
        st.info(f"Execution ARN: {response['executionArn']}")

        return response["executionArn"]

    except ClientError as e:
        st.error(f"❌ Failed to start state machine: {e}")
        return None
    

if menu == "CI/CD Configuration":
    st.header("⚙️ CI/CD Configuration")
    col1, col2, col3 = st.columns(3)
    with col2:
        if st.button("▶️ Run Project 6 ETL Pipeline"):
            state_machine_arn = "arn:aws:states:us-east-1:000185048470:stateMachine:project6-state-machine"
            trigger_state_machine(state_machine_arn)
    st.markdown("---")


# ============================================
# Paths to Gold Parquet files (S3)
# ============================================
GOLD_BUCKET = "project6-gold-bucket"

PATHS = {
    "daily": f"s3://{GOLD_BUCKET}/gold_daily_bookings_by_channel/",
    "cpb": f"s3://{GOLD_BUCKET}/gold_cpb_by_channel/",
    "channel": f"s3://{GOLD_BUCKET}/gold_channel_attribution/",
    "booking_time": f"s3://{GOLD_BUCKET}/gold_booking_time_analysis/",
    "meeting_load": f"s3://{GOLD_BUCKET}/gold_meeting_load_per_employee/"
}

# ============================================
# Load Parquet Tables from S3
# ============================================
@st.cache_data
def load_data(path):
    # Use s3fs to access S3 bucket
    df = pd.read_parquet(path, engine="pyarrow")
    return df

# Load all tables
daily_df = load_data(PATHS["daily"])
cpb_df = load_data(PATHS["cpb"])
channel_df = load_data(PATHS["channel"])
booking_time_df = load_data(PATHS["booking_time"])
meeting_df = load_data(PATHS["meeting_load"])



# ============================================
# DASHBOARD PAGES
# ============================================

# ============================================
# 1️⃣ Daily Calls Booked by Channel
# ============================================
if menu == "Daily Calls Booked by Channel":
    st.subheader("🗓️ Daily Calls Booked by Channel")
    st.dataframe(daily_df)

    daily_pivot = daily_df.pivot_table(
        index="booking_date",
        columns="channel",
        values="bookings_count",
        aggfunc="sum",
        fill_value=0
    )

    # Define the desired order
    channel_order = ["facebook", "youtube", "tiktok", "others"]

    # Reorder columns according to channel_order
    daily_pivot = daily_pivot.reindex(columns=channel_order)

    channels = daily_pivot.columns.tolist()

    selected_channels = st.multiselect(
        "Select channels",
        options=channels,
        default=channels
    )

    if selected_channels:
        filtered_df = daily_pivot[selected_channels].reset_index()

        fig = px.line(
            filtered_df,
            x="booking_date",
            y=selected_channels,
            markers=True,
            color_discrete_map={
                "facebook": "blue",
                "youtube": "red",
                "tiktok": "green",
                "others": "orange"
            }
        )

        fig.update_layout(
            legend_title_text="Channel (click to hide/show)",
            hovermode="x unified",
            xaxis_title="Date",
            yaxis_title="Bookings"
        )

        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Please select at least one channel.")



# ============================================
# 2️⃣ CPB by Channel
# ============================================
elif menu == "CPB by Channel":
    st.subheader("💰 Cost per Booking (CPB) by Channel")

    # Reorder cpb_df according to the desired channel order
    channel_order = ["facebook", "youtube", "tiktok", "others"]
    cpb_df["channel"] = pd.Categorical(cpb_df["channel"], categories=channel_order, ordered=True)
    cpb_df = cpb_df.sort_values("channel").reset_index(drop=True)

    st.dataframe(cpb_df)

    cpb_chart = (
        alt.Chart(cpb_df)
        .mark_bar()
        .encode(
            x=alt.X("channel:N", title="Channel"),
            y=alt.Y("cpb:Q", title="Cost per Booking"),
            tooltip=["channel", "total_spend", "total_bookings_count", "cpb"],
            color=alt.Color(
                "channel:N",
                scale=alt.Scale(
                    domain=channel_order,
                    range=["blue", "red", "green", "orange"]
                ),
                legend=None
            )
        )
    )

    st.altair_chart(cpb_chart, use_container_width=True)

   
# ============================================
# 3️⃣ Channel Attribution Leaderboard
# ============================================
elif menu == "Channel Attribution Leaderboard":
    st.subheader("🎬 Channel Attribution Leaderboard")

    st.dataframe(
        channel_df.sort_values(
            ["rank_by_volume", "rank_by_cpb"]
        )
    )

# ============================================
# 4️⃣ Booking Time Analysis
# ============================================
 
elif menu == "Booking Time Analysis":
    st.subheader("⏱️ Booking Time Analysis")

    day_map = {
        1: "Sunday",
        2: "Monday",
        3: "Tuesday",
        4: "Wednesday",
        5: "Thursday",
        6: "Friday",
        7: "Saturday"
    }

    booking_time_df["day_name"] = booking_time_df["booking_day_of_week"].map(day_map)
    
    week_order = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
    days = [day for day in week_order if day in booking_time_df["day_name"].unique()]

    selected_days = st.multiselect(
        "Select days of week",
        options=days,
        default=days
    )

    filtered_df = booking_time_df[
        booking_time_df["day_name"].isin(selected_days)
    ]

    fig = px.bar(
        filtered_df,
        x="booking_hour",
        y="bookings_count",
        color="day_name",
        barmode="group"
    )

    fig.update_layout(
        legend_title_text="Day (click to hide/show)",
        xaxis_title="Hour of Day",
        yaxis_title="Bookings"
    )

    st.plotly_chart(fig, use_container_width=True)

# ============================================
# 5️⃣ Meeting Load per Employee
# ============================================
elif menu == "Meeting Load per Employee":
    st.subheader("👨‍💼 Meeting Load per Employee")

    meeting_sorted = meeting_df.sort_values(
        "total_meetings_count",
        ascending=False
    )

    st.dataframe(meeting_sorted)

    fig = px.bar(
        meeting_sorted,
        x="user_id",
        y="total_meetings_count",
        color_discrete_sequence=["magenta"]
    )

    fig.update_layout(
        xaxis_title="User ID",
        yaxis_title="Total Meetings"
    )

    st.plotly_chart(fig, use_container_width=True)

# ============================================
# Footer
# ============================================
st.markdown("---")
st.markdown("Built on **AWS | Delta Lake | Streamlit**")
st.markdown("© 2026 Hadi Hosseini")
