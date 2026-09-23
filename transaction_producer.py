import os
import time
import uuid
import random
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import StringSerializer

# Load credentials from .env
load_dotenv()

schema_str = """
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Transaction",
  "type": "object",
  "properties": {
    "transaction_id": {"type": "string"},
    "customer_id": {"type": "string"},
    "amount": {"type": "number"},
    "currency": {"type": "string"},
    "merchant_category": {"type": "string"},
    "country": {"type": "string"},
    "device_id": {"type": "string"},
    "event_time": {"type": "string", "format": "date-time"}
  },
  "required": ["transaction_id", "customer_id", "amount", "currency", "country", "device_id", "event_time"]
}
"""

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def main():
    # 1. Setup Schema Registry Client
    sr_conf = {
        'url': os.getenv('SCHEMA_REGISTRY_URL'),
        'basic.auth.user.info': f"{os.getenv('SCHEMA_REGISTRY_API_KEY')}:{os.getenv('SCHEMA_REGISTRY_API_SECRET')}"
    }
    schema_registry_client = SchemaRegistryClient(sr_conf)

    # 2. Setup JSON Serializer
    json_serializer = JSONSerializer(
        schema_str,
        schema_registry_client,
        # A simple to_dict function: our data is already a dict
        lambda obj, ctx: obj
    )

    # 3. Setup Producer
    producer_conf = {
        'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.getenv('KAFKA_API_KEY'),
        'sasl.password': os.getenv('KAFKA_API_SECRET'),
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': json_serializer
    }
    producer = SerializingProducer(producer_conf)

    topic = "transactions"
    print("Starting transaction producer. Press Ctrl+C to stop.")

    # 4. Generate Data Loop
    try:
        while True:
            # Datagen 'Users' generates userid like User_1, User_2...
            # We will generate transactions for a random user.
            user_num = random.randint(1, 9)
            customer_id = f"User_{user_num}"

            # 70% normal, 20% severe fraud (BLOCK), 10% mild fraud (REVIEW)
            rand_val = random.random()

            if rand_val < 0.20:
                # Severe Fraud (Triggers BLOCK - Score 75)
                amount = round(random.uniform(5000, 15000), 2)
                country = "Region_99"  # Mismatch
                device_id = f"device-NEW-{random.randint(100, 999)}"
                print(f"\n*** GENERATING SEVERE FRAUD TRANSACTION FOR {customer_id} ***")
            elif rand_val < 0.30:
                # Mild Fraud (Triggers REVIEW - Score 45-55)
                amount = round(random.uniform(5000, 8000), 2)  # Risk 30
                country = f"Region_{random.randint(1, 9)}"  # Might match, might not
                device_id = f"device-NEW-{random.randint(100, 999)}"  # Risk 25
                print(f"\n*** GENERATING MILD FRAUD TRANSACTION FOR {customer_id} ***")
            else:
                # Normal Transaction (Triggers APPROVE - Score 0)
                amount = round(random.uniform(10.0, 150.0), 2)  # Normal amount
                country = f"Region_{user_num}"  # Consistent region
                device_id = f"device-KNOWN-{user_num}"

            transaction = {
                "transaction_id": f"txn-{uuid.uuid4().hex[:8]}",
                "customer_id": customer_id,
                "amount": amount,
                "currency": "USD",
                "country": country,
                "device_id": device_id,
                "event_time": datetime.now(timezone.utc).isoformat()
            }

            producer.produce(
                topic=topic,
                key=customer_id,
                value=transaction,
                on_delivery=delivery_report
            )
            producer.poll(0)
            
            # Sleep 1-3 seconds between transactions
            time.sleep(random.uniform(1.0, 3.0))

    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()

if __name__ == '__main__':
    main()
