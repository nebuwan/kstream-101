import json
import os

from dotenv import load_dotenv
from kafka import KafkaConsumer

# Load values from the .env file
load_dotenv()

# Read config from .env — if a value isn't there, use the fallback after the comma
TOPIC = os.getenv("TOPIC", "cell-tower-metrics")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
GROUP_ID = os.getenv("GROUP_ID", "tower-monitor")

# Connect to Kafka and subscribe to a topic
# group_id    → your bookmark name — Kafka uses this to remember where you left off
# auto_offset_reset → what to do on first connect when there's no bookmark yet
#                     "earliest" = start from the very first message ever sent
#                     "latest"   = only receive new messages going forward
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
)

print(f"Listening on topic: {TOPIC}  |  Press Ctrl+C to stop\n")

try:
    # This loop waits and blocks until a new message arrives
    # Each iteration gives you one message from Kafka
    for message in consumer:

        # message.value is raw bytes — convert it back to a Python dict
        data = json.loads(message.value)

        print(
            f"partition={message.partition}  "   # which partition (lane) this came from
            f"offset={message.offset}  "         # position of this message in the partition
            f"key={message.key.decode()}  |  "   # the tower name (bytes → string)
            f"signal={data['signal_dbm']} dBm  "
            f"devices={data['connected_devices']}  "
            f"network={data['network']}  "
            f"timestamp={data['timestamp']}"
        )

except KeyboardInterrupt:
    print("\nStopped.")
