import json
import os
import random
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

TOPIC = os.getenv("TOPIC", "cell-tower-metrics")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TOWERS = ["TOWER_NORTH", "TOWER_SOUTH", "TOWER_EAST", "TOWER_WEST", "TOWER_CBD"]

producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)

print(f"Sending to topic: {TOPIC}  |  Press Ctrl+C to stop\n")

try:
    while True:
        tower = random.choice(TOWERS)
        message = {
            "tower_id": tower,
            "signal_dbm": random.randint(-110, -50),
            "connected_devices": random.randint(10, 500),
            "throughput_mbps": round(random.uniform(1, 100), 1),
            "network": random.choice(["4G", "5G"]),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        # The key tells Kafka which partition to use.
        # Same tower_id always goes to the same partition.
        producer.send(TOPIC, key=tower.encode(), value=json.dumps(message).encode())
        print(f"sent → key={tower}  signal={message['signal_dbm']} dBm  devices={message['connected_devices']}")

        time.sleep(1)

except KeyboardInterrupt:
    producer.flush()
    print("\nStopped.")
