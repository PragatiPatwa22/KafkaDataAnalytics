
import streamlit as st
import pandas as pd
from kafka import KafkaConsumer

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
    "Analyze student performance using **Apache Kafka, Python, and Streamlit**."
)

# ------------------------------------
# 2. Read Data from Kafka
# ------------------------------------
@st.cache_data(ttl=10)
def get_kafka_data():
    consumer = KafkaConsumer(
        "analytics-data",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
        value_deserializer=lambda x: x.decode("utf-8")
    )

    records = []

    try:
        for message in consumer:
            try:
                parts = message.value.split(",")

                student_id = int(parts[0].split("=")[1])
                marks = int(parts[1].split("=")[1])

                # Validate marks
                if 0 <= marks <= 100:
                    records.append({
                        "Student ID": student_id,
                        "Marks": marks
                    })

            except (ValueError, IndexError):
                continue
    finally:
        consumer.close()

    return pd.DataFrame(records)


# ------------------------------------
# 3. Refresh Dashboard
# ------------------------------------
if st.button("🔄 Refresh Data"):
    get_kafka_data.clear()
    st.rerun()

# ------------------------------------
# 4. Display Dashboard
# ------------------------------------
try:
    df = get_kafka_data()

    if not df.empty:

        # Pass/Fail classification
        df["Result"] = df["Marks"].apply(
            lambda marks: "Pass" if marks >= 40 else "Fail"
        )

        # Summary metrics
        st.subheader("📈 Performance Overview")

        col1, col2, col3, col4 = st.columns(4)

        col1.metric("Total Records", len(df))
        col2.metric("Average Marks", f"{df['Marks'].mean():.2f}")
        col3.metric("Highest Marks", int(df["Marks"].max()))
        col4.metric("Lowest Marks", int(df["Marks"].min()))

        st.divider()

        # Student records
        st.subheader("📋 Student Records")
        st.dataframe(df, use_container_width=True)

        # Marks chart
        st.subheader("📊 Student Marks Chart")

        chart_data = df.set_index("Student ID")[["Marks"]]
        st.bar_chart(chart_data)

        # Pass and fail analysis
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

        # Students who scored 75 or above
        st.subheader("🎯 Students Scoring 75 or Above")

        high_scorers = df[df["Marks"] >= 75]

        st.write(
            f"Number of records scoring 75 or above: {len(high_scorers)}"
        )

        st.dataframe(high_scorers, use_container_width=True)

        # Highest scorer
        st.subheader("🏆 Highest Scorer")

        highest_marks = df["Marks"].max()
        toppers = df[df["Marks"] == highest_marks]

        st.write(f"Highest marks achieved: **{highest_marks}**")
        st.dataframe(toppers, use_container_width=True)

        # Download data
        st.subheader("📥 Download Analytics Data")

        csv_data = df.to_csv(index=False).encode("utf-8")

        st.download_button(
            label="Download CSV Report",
            data=csv_data,
            file_name="student_analytics.csv",
            mime="text/csv"
        )

        st.success("Analytics loaded successfully from Kafka!")

    else:
        st.warning("No valid student records found in the Kafka topic.")

except Exception as e:
    st.error("Unable to connect to Kafka.")
    st.write(
        "Check that the Kafka server is running on localhost:9092 "
        "and the analytics-data topic exists."
    )
    st.caption(f"Technical details: {e}")
