import json
import os

from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

TOPIC = os.getenv("TOPIC", "cell-tower-metrics")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
GROUP_ID = os.getenv("GROUP_ID", "tower-monitor")

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
)

print(f"Listening on topic: {TOPIC}  |  Press Ctrl+C to stop\n")

try:
    for message in consumer:
        data = json.loads(message.value)
        print(
            f"partition={message.partition}  offset={message.offset}  "
            f"key={message.key.decode()}  |  "
            #f"key={message.key}  |  "
            f"signal={data['signal_dbm']} dBm  "
            f"devices={data['connected_devices']}  "
            f"network={data['network']}  "
            f"timestamp={data['timestamp']}"
        )

except KeyboardInterrupt:
    print("\nStopped.")
