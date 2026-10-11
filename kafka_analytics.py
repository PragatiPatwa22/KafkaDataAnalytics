
from kafka import KafkaConsumer
import pandas as pd
import matplotlib.pyplot as plt

# 1. Connect to Kafka and read student records
print("Connecting to Kafka...")

consumer = KafkaConsumer(
    "analytics-data",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    consumer_timeout_ms=10000,
    value_deserializer=lambda x: x.decode("utf-8")
)

records = []

# 2. Collect and process the messages
print("Reading student records from Kafka...")

for message in consumer:
    try:
        parts = message.value.split(",")

        student_id = int(parts[0].split("=")[1])
        marks = int(parts[1].split("=")[1])

        records.append({
            "student_id": student_id,
            "marks": marks
        })

    except (ValueError, IndexError):
        print("Skipping invalid record:", message.value)

consumer.close()

# 3. Analyze the data
if records:
    df = pd.DataFrame(records)

    print("\n========== STUDENT MARKS ANALYTICS ==========")
    print(df.to_string(index=False))

    average_marks = df["marks"].mean()
    highest_marks = df["marks"].max()
    lowest_marks = df["marks"].min()
    passed_students = (df["marks"] >= 75).sum()

    print("\n========== ANALYTICS RESULTS ==========")
    print(f"Total students: {len(df)}")
    print(f"Average marks: {average_marks:.2f}")
    print(f"Highest marks: {highest_marks}")
    print(f"Lowest marks: {lowest_marks}")
    print(f"Students scoring 75 or above: {passed_students}")

    # 4. Create a bar chart
    plt.figure(figsize=(8, 5))

    plt.bar(
        df["student_id"].astype(str),
        df["marks"]
    )

    plt.title("Student Marks Analytics")
    plt.xlabel("Student ID")
    plt.ylabel("Marks")
    plt.ylim(0, 100)
    plt.grid(axis="y", linestyle="--", alpha=0.4)

    plt.tight_layout()

    # 5. Save the chart
    plt.savefig("student_marks_chart.png", dpi=300)

    print("\nChart saved as student_marks_chart.png")

    # 6. Display the chart
    plt.show()

else:
    print("No valid student records received from Kafka.")
    print("Check that the topic contains student records.")
