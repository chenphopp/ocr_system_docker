import os
import time
import json
import uuid
from kafka import KafkaProducer

# -----------------------
# CONFIG
# -----------------------

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:29092")
INPUT_TOPIC = "receipt_input"
WATCH_DIRECTORY = "/app/source_folder"

print("🚀 Watcher Producer (polling) starting...")
print(f"Kafka Broker: {KAFKA_BROKER}")
print(f"Watching dir: {WATCH_DIRECTORY}")

# -----------------------
# KAFKA PRODUCER
# -----------------------

def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5,
            )
            print("✅ Kafka Producer connected")
            return producer
        except Exception as e:
            print("⏳ Waiting for Kafka...", e)
            time.sleep(3)


producer = create_producer()


# -----------------------
# MAIN POLLING LOOP
# -----------------------

def main():
    if not os.path.exists(WATCH_DIRECTORY):
        print(f"❌ Watch directory does not exist: {WATCH_DIRECTORY}")
        return

    processed = set()  # remember what we've sent

    print("👀 Polling for new image files...")

    while True:
        try:
            files = os.listdir(WATCH_DIRECTORY)
        except Exception as e:
            print(f"❌ Error listing directory: {e}")
            time.sleep(2)
            continue

        for name in files:
            lower = name.lower()
            if not lower.endswith((".jpg", ".jpeg", ".png")):
                continue

            full_path = os.path.join(WATCH_DIRECTORY, name)

            if full_path in processed:
                continue
            if not os.path.isfile(full_path):
                continue

            # small delay to ensure copy is complete
            time.sleep(0.2)

            file_id = str(uuid.uuid4())
            payload = {
                "file_id": file_id,
                "filename": name,
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "file_path": full_path,
            }

            try:
                producer.send(INPUT_TOPIC, payload)
                producer.flush()
                processed.add(full_path)
                print(f"📤 Sent: {name} (ID={file_id})")
            except Exception as e:
                print(f"❌ Failed to send {name}: {e}")

        time.sleep(1)  # poll every second


if __name__ == "__main__":
    main()