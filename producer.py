
from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

products = ["Laptop", "Mobile", "Headphones", "Keyboard", "Mouse"]

print("Sending sales data to Kafka... Press Ctrl+C to stop.")

try:
    while True:
        sale = {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "product": random.choice(products),
            "quantity": random.randint(1, 5),
            "price": random.randint(500, 50000)
        }

        producer.send("sales-data", sale)
        producer.flush()

        print("Sent:", sale)
        time.sleep(2)

except KeyboardInterrupt:
    print("\nStopped sending sales data.")

finally:
    producer.close()
