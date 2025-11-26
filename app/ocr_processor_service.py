import os
import time
import json
import base64
import requests
from kafka import KafkaConsumer, KafkaProducer

# -----------------------
# CONFIG
# -----------------------

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:29092")
INPUT_TOPIC = "receipt_input"
OUTPUT_TOPIC = "receipt_output"

MISTRAL_API_KEY = os.environ.get("MISTRAL_API_KEY")
MISTRAL_API_ENDPOINT = "https://api.mistral.ai/v1/chat/completions"
MISTRAL_MODEL = "mistral-large-latest"

if not MISTRAL_API_KEY:
    print("❌ MISTRAL_API_KEY is missing.")
    exit(1)

print("🚀 OCR Processor (Mistral AI) starting...")
print(f"Kafka Broker : {KAFKA_BROKER}")
print(f"Input Topic  : {INPUT_TOPIC}")
print(f"Output Topic : {OUTPUT_TOPIC}")


# -----------------------
# TARGET SCHEMA EXAMPLE
# -----------------------

TARGET_SCHEMA_EXAMPLE = {
    "file_name": "Parking_Receipt",
    "topics": [
        "Parking",
        "Receipt",
        "City of Palo Alto",
        "Expiration Date",
        "Payment"
    ],
    "languages": "English",
    "ocr_contents": {
        "header": "PLACE FACE UP ON DASH",
        "city": "CITY OF PALO ALTO",
        "validity": "NOT VALID FOR ONSTREET PARKING",
        "expiration": {
            "date_time": "11:59 PM AUG 19, 2024"
        },
        "purchase": {
            "date_time": "01:34pm Aug 19, 2024"
        },
        "amounts": {
            "total_due": "$15.00",
            "total_paid": "$15.00"
        },
        "ticket": {
            "number": "00005883"
        },
        "serial_number": "520117260957",
        "setting": "Permit Machines",
        "machine_name": "Civic Center",
        "payment_info": {
            "card_number": "****-1224",
            "card_type": "Visa"
        },
        "instructions": {
            "display": "DISPLAY FACE UP ON DASH",
            "expiration": "PERMIT EXPIRES AT MIDNIGHT"
        }
    }
}


# -----------------------
# KAFKA HELPERS
# -----------------------

def create_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id="ocr-processor-group",
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            )
            print("✅ Kafka Consumer connected")
            return consumer
        except Exception as e:
            print("⏳ Waiting for Kafka Consumer...", e)
            time.sleep(3)


def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda x: json.dumps(x).encode("utf-8"),
            )
            print("✅ Kafka Producer connected")
            return producer
        except Exception as e:
            print("⏳ Waiting for Kafka Producer...", e)
            time.sleep(3)


# -----------------------
# IMAGE ENCODING
# -----------------------

def encode_image_to_base64(image_path: str) -> str | None:
    try:
        with open(image_path, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")
    except Exception as e:
        print(f"❌ Failed reading image {image_path}: {e}")
        return None

# -----------------------
# MISTRAL CALL
# -----------------------

def analyze_image_with_mistral(image_path: str) -> dict | None:
    base64_image = encode_image_to_base64(image_path)
    if not base64_image:
        return None

    example_json = json.dumps(TARGET_SCHEMA_EXAMPLE, ensure_ascii=False, indent=2)

    prompt = (
        "You are an OCR and structured information extraction engine.\n"
        "You receive an image of a receipt and MUST output ONLY a JSON object.\n\n"
        "IMPORTANT RULES:\n"
        "1. Use EXACTLY the same JSON structure (keys and nesting) as this example:\n"
        f"{example_json}\n\n"
        "2. Only change the values based on the content of the new receipt image.\n"
        "   - Do NOT add or remove keys.\n"
        "   - Keep the same structure: file_name, topics, languages, ocr_contents, etc.\n"
        "3. If some information is missing in the receipt, keep the key but set the value to null or an empty string.\n"
        "4. Return ONLY valid JSON. No comments, no explanations, no markdown.\n"
    )

    payload = {
        "model": MISTRAL_MODEL,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"},
                    },
                ],
            }
        ],
        "response_format": {"type": "json_object"},
        "temperature": 0.1,
    }

    headers = {
        "Authorization": f"Bearer {MISTRAL_API_KEY}",
        "Content-Type": "application/json",
    }

    try:
        print(f"   🔎 Sending image to Mistral: {os.path.basename(image_path)}")
        res = requests.post(MISTRAL_API_ENDPOINT, json=payload, headers=headers, timeout=60)
        res.raise_for_status()
        content = res.json()["choices"][0]["message"]["content"]
        return json.loads(content)
    except Exception as e:
        print("❌ Mistral OCR failed:", e)
        try:
            print("Raw response:", res.text)  # may not exist if request completely failed
        except Exception:
            pass
        return None


# -----------------------
# PROCESS LOOP
# -----------------------

def ocr_processor_loop():
    consumer = create_consumer()
    producer = create_producer()

    print("👂 OCR Processor listening...")

    for msg in consumer:
        msg_data = msg.value
        file_path = msg_data.get("file_path")
        file_id = msg_data.get("file_id")
        filename = msg_data.get("filename")

        if not file_path or not os.path.exists(file_path):
            print(f"⚠️ File missing: {file_path}")
            continue

        print(f"🤖 OCR Processing ID={file_id} | {filename}")

        result = analyze_image_with_mistral(file_path)

        if not result:
            print(f"❌ OCR failed for ID={file_id}")
            continue

        # Enrich / override some metadata
        result["file_name"] = os.path.splitext(filename)[0]
        result.setdefault("ocr_contents", {})
        result.setdefault("topics", [])
        result.setdefault("languages", "Unknown")

        result["file_id"] = file_id
        result["processed_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        result["source_path"] = file_path

        try:
            producer.send(OUTPUT_TOPIC, result)
            producer.flush()
            print(f"✅ Result published for {filename}")
        except Exception as e:
            print(f"❌ Kafka publish error: {e}")


if __name__ == "__main__":
    ocr_processor_loop()
