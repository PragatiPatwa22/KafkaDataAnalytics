
import streamlit as st
import pandas as pd

# Kafka is optional for the online demo
try:
    from kafka import KafkaConsumer
    KAFKA_AVAILABLE = True
except ImportError:
    KafkaConsumer = None
    KAFKA_AVAILABLE = False


# ------------------------------------
# 1. Page Configuration
# ------------------------------------
st.set_page_config(
    page_title="Kafka Data Analytics",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Apache Kafka Data Analytics Dashboard")
st.markdown(
    "Analyze student performance using **Apache Kafka, Python, Pandas, and Streamlit**."
)


# ------------------------------------
# 2. Sample Data for Online Demo
# ------------------------------------
def get_sample_data():
    return pd.DataFrame({
        "Student ID": [101, 102, 103, 104, 105, 106],
        "Marks": [85, 72, 91, 65, 88, 76]
    })


# ------------------------------------
# 3. Read Data from Kafka
# ------------------------------------
@st.cache_data(ttl=10)
def get_kafka_data():
    if not KAFKA_AVAILABLE:
        return pd.DataFrame(columns=["Student ID", "Marks"])

    consumer = None
    records = []

    try:
        consumer = KafkaConsumer(
            "analytics-data",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=5000,
            value_deserializer=lambda x: x.decode("utf-8")
        )

        for message in consumer:
            try:
                parts = message.value.split(",")

                student_id = int(parts[0].split("=")[1])
                marks = int(parts[1].split("=")[1])

                if 0 <= marks <= 100:
                    records.append({
                        "Student ID": student_id,
                        "Marks": marks
                    })

            except (ValueError, IndexError):
                continue

    except Exception:
        return pd.DataFrame(columns=["Student ID", "Marks"])

    finally:
        if consumer is not None:
            consumer.close()

    return pd.DataFrame(records)


# ------------------------------------
# 4. Select Data Source
# ------------------------------------
st.sidebar.header("Data Source")

data_source = st.sidebar.radio(
    "Choose how to display data:",
    ["Online Demo Data", "Local Kafka Data"]
)

if st.sidebar.button("🔄 Refresh Data"):
    get_kafka_data.clear()
    st.rerun()

if data_source == "Online Demo Data":
    df = get_sample_data()
    st.info(
        "Demo mode: displaying sample student data. "
        "These records are not being read live from Kafka."
    )
else:
    df = get_kafka_data()

    if not KAFKA_AVAILABLE:
        st.error(
            "The kafka-python package is not installed. "
            "Check requirements.txt and the Streamlit deployment logs."
        )
    elif df.empty:
        st.warning(
            "No records received. The cloud app cannot automatically "
            "connect to Kafka running on your own computer. "
            "Use Online Demo Data for the college presentation."
        )


# ------------------------------------
# 5. Display Analytics
# ------------------------------------
if not df.empty:

    df["Result"] = df["Marks"].apply(
        lambda marks: "Pass" if marks >= 40 else "Fail"
    )

    st.subheader("📈 Performance Overview")

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Total Records", len(df))
    col2.metric("Average Marks", f"{df['Marks'].mean():.2f}")
    col3.metric("Highest Marks", int(df["Marks"].max()))
    col4.metric("Lowest Marks", int(df["Marks"].min()))

    st.divider()

    # Student Records
    st.subheader("📋 Student Records")
    st.dataframe(df, use_container_width=True)

    # Marks Chart
    st.subheader("📊 Student Marks Chart")
    chart_data = df.set_index("Student ID")[["Marks"]]
    st.bar_chart(chart_data)

    # Pass / Fail Analysis
    st.subheader("✅ Pass/Fail Analysis")

    pass_count = int((df["Result"] == "Pass").sum())
    fail_count = int((df["Result"] == "Fail").sum())

    col1, col2 = st.columns(2)
    col1.metric("Students Passed", pass_count)
    col2.metric("Students Failed", fail_count)

    result_counts = (
        df["Result"]
        .value_counts()
        .reindex(["Pass", "Fail"], fill_value=0)
    )
    st.bar_chart(result_counts)

    # High Scorers
    st.subheader("🎯 Students Scoring 75 or Above")

    high_scorers = df[df["Marks"] >= 75]
    st.write(
        f"Number of records scoring 75 or above: {len(high_scorers)}"
    )
    st.dataframe(high_scorers, use_container_width=True)

    # Highest Scorer
    st.subheader("🏆 Highest Scorer")

    highest_marks = df["Marks"].max()
    toppers = df[df["Marks"] == highest_marks]

    st.write(f"Highest marks achieved: **{highest_marks}**")
    st.dataframe(toppers, use_container_width=True)

    # Download CSV
    st.subheader("📥 Download Analytics Data")

    csv_data = df.to_csv(index=False).encode("utf-8")

    st.download_button(
        label="Download CSV Report",
        data=csv_data,
        file_name="student_analytics.csv",
        mime="text/csv"
    )

    st.success("Analytics dashboard loaded successfully!")

st.divider()
st.caption(
    "Educational project using Apache Kafka, Python, Pandas, and Streamlit. "
    "Online Demo Data uses sample records; Local Kafka Data requires a "
    "reachable Kafka broker."
)
