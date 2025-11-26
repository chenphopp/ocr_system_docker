import os
import time
import json
from kafka import KafkaConsumer

# -----------------------
# CONFIG
# -----------------------

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:29092")
OUTPUT_TOPIC = "receipt_output"
SINK_DIRECTORY = "/app/sink_folder"

print("🚀 Sink Writer starting...")
print(f"Kafka Broker : {KAFKA_BROKER}")
print(f"Topic        : {OUTPUT_TOPIC}")
print(f"Output dir   : {SINK_DIRECTORY}")


# -----------------------
# KAFKA HELPER
# -----------------------

def create_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                OUTPUT_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id="sink-writer-group",
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            )
            print("✅ Kafka Consumer connected")
            return consumer
        except Exception as e:
            print("⏳ Waiting for Kafka...", e)
            time.sleep(3)


# -----------------------
# MAIN LOOP
# -----------------------

def sink_writer_loop():
    # Ensure sink directory exists
    os.makedirs(SINK_DIRECTORY, exist_ok=True)
    print(f"📁 Sink directory ready: {SINK_DIRECTORY}")

    consumer = create_consumer()
    print("👂 Sink Writer listening for OCR results...")

    for msg in consumer:
        data = msg.value

        file_name = data.get("file_name", "unknown_receipt")
        file_id = data.get("file_id", None)

        # Use file_name + file_id (or timestamp) to make name unique
        safe_name = file_name.replace(" ", "_")
        if file_id:
            output_filename = f"{safe_name}_{file_id}.json"
        else:
            output_filename = f"{safe_name}_{int(time.time())}.json"

        output_path = os.path.join(SINK_DIRECTORY, output_filename)

        try:
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)

            print(f"💾 Saved: {output_filename}")
        except Exception as e:
            print(f"❌ ERROR writing {output_filename}: {e}")


if __name__ == "__main__":
    sink_writer_loop()
