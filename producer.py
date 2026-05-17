import json
import os
import random
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaProducer

# Load values from the .env file (like BOOTSTRAP_SERVERS, TOPIC)
load_dotenv()

# Read config from .env — if a value isn't there, use the fallback after the comma
TOPIC = os.getenv("TOPIC", "cell-tower-metrics")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")

# These are the 5 towers we're simulating
TOWERS = ["TOWER_NORTH", "TOWER_SOUTH", "TOWER_EAST", "TOWER_WEST", "TOWER_CBD"]

# Connect to Kafka — bootstrap_servers is just the address of the Kafka broker
# The producer will use this connection to send messages
producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)

print(f"Sending to topic: {TOPIC}  |  Press Ctrl+C to stop\n")

try:
    while True:
        # Pick a random tower to simulate a reading from
        tower = random.choice(TOWERS)

        # Build the message payload — this is the actual data we're sending
        message = {
            "tower_id": tower,
            "signal_dbm": random.randint(-110, -50),      # -50 is strong, -110 is weak
            "connected_devices": random.randint(10, 500),
            "throughput_mbps": round(random.uniform(1, 100), 1),
            "network": random.choice(["4G", "5G"]),
            "timestamp": datetime.now(timezone.utc).isoformat(),  # current time in UTC
        }

        # Send the message to Kafka
        # key   → tells Kafka which partition to use (same tower always goes to same partition)
        # value → the actual data, converted to JSON then to bytes (Kafka only speaks bytes)
        producer.send(
            TOPIC,
            key=tower.encode(),                    # string → bytes
            value=json.dumps(message).encode()     # dict → JSON string → bytes
        )

        print(f"sent → key={tower}  signal={message['signal_dbm']} dBm  devices={message['connected_devices']}")

        # Wait 1 second before sending the next message
        time.sleep(1)

except KeyboardInterrupt:
    # Wait for any unsent messages to finish before closing
    producer.flush()
    print("\nStopped.")
