
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "sales-data",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda data: json.loads(data.decode("utf-8")),
    consumer_timeout_ms=10000
)

total_records = 0
total_revenue = 0

print("Reading sales data from Kafka...\n")

try:
    for message in consumer:
        sale = message.value

        revenue = sale["quantity"] * sale["price"]
        total_records += 1
        total_revenue += revenue

        print(
            f"Product: {sale['product']} | "
            f"Quantity: {sale['quantity']} | "
            f"Revenue: ₹{revenue}"
        )

    print("\n--- SALES ANALYTICS SUMMARY ---")
    print("Total records:", total_records)
    print("Total revenue: ₹", total_revenue)

finally:
    consumer.close()
