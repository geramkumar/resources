import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

while True:
    # simulate late events sometimes
    delay_seconds = random.choice([0, 0, 0, 30, 60])

    event = {
        "order_id": random.randint(1, 100000),
        "amount": random.randint(100, 1000),
        "event_time": (datetime.utcnow() - timedelta(seconds=delay_seconds)).isoformat()
    }

    producer.send("orders", event)
    print(event)
    time.sleep(0.2)   # ~5 events/sec
