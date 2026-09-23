import os
import json
import google.generativeai as genai
from dotenv import load_dotenv
from confluent_kafka import Consumer, Producer

# Load env
load_dotenv()

# Configure Gemini
genai.configure(api_key=os.getenv('LLM_API_KEY'))
# Use flash for speed
model = genai.GenerativeModel('gemini-3.6-flash')

# Set up the Kafka Consumer
consumer_conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('KAFKA_API_KEY'),
    'sasl.password': os.getenv('KAFKA_API_SECRET'),
    'group.id': 'ai-fraud-agent',
    'auto.offset.reset': 'latest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe(['fraud-decisions-mat'])

# Set up the Kafka Producer for alerts
producer_conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('KAFKA_API_KEY'),
    'sasl.password': os.getenv('KAFKA_API_SECRET'),
}
producer = Producer(producer_conf)

import struct
import io
import fastavro
from confluent_kafka.schema_registry import SchemaRegistryClient

# Set up Schema Registry for fetching Avro schemas
sr_conf = {
    'url': os.getenv('SCHEMA_REGISTRY_URL'),
    'basic.auth.user.info': f"{os.getenv('SCHEMA_REGISTRY_API_KEY')}:{os.getenv('SCHEMA_REGISTRY_API_SECRET')}"
}
schema_registry_client = SchemaRegistryClient(sr_conf)
schema_cache = {}

def get_schema(schema_id):
    if schema_id not in schema_cache:
        schema_str = schema_registry_client.get_schema(schema_id).schema_str
        schema_cache[schema_id] = fastavro.parse_schema(json.loads(schema_str))
    return schema_cache[schema_id]

print("[START]")
# Kafka Consumer configuration
conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('KAFKA_API_KEY'),
    'sasl.password': os.getenv('KAFKA_API_SECRET'),
    'group.id': 'streamshield-ai-agent',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': 'false' # Manual commits for reliability
}

consumer = Consumer(conf)
consumer.subscribe(['fraud-decisions-mat'])

print("AI Agent listening to fraud-decisions-mat...")

try:
    while True:
        msg = consumer.poll(1.0)
        
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
            
        # Handle Flink upsert tombstones (retractions)
        if msg.value() is None:
            print("Received tombstone message (retraction). Skipping...")
            consumer.commit(asynchronous=False)
            continue
            
        try:
            payload = msg.value()
            
            # Check for magic byte (0x00) indicating Confluent Schema Registry Avro format
            if payload[0] == 0:
                schema_id = struct.unpack('>I', payload[1:5])[0]
                schema = get_schema(schema_id)
                avro_payload = payload[5:]
                
                with io.BytesIO(avro_payload) as f:
                    decision_data = fastavro.schemaless_reader(f, schema)
            else:
                # Fallback to plain JSON
                decision_data = json.loads(payload.decode('utf-8'))
            
            decision = decision_data.get('decision', 'UNKNOWN')

            # Step 1: Send immediate deterministic alert to security-alerts topic
            txn_id = decision_data.get('transaction_id')
            if not txn_id and msg.key():
                txn_id = ''.join(filter(lambda x: x.isprintable(), msg.key().decode('utf-8', errors='ignore')))
            elif not txn_id:
                txn_id = "unknown_txn"

            alert = {
                "transaction_id": txn_id,
                "customer_id": decision_data.get('customer_id', 'unknown'),
                "action": decision,
                "amount": decision_data.get('amount', 0),
                "total_risk_score": decision_data.get('total_risk_score', 0),
                "ai_explanation": "Pending AI Analysis..." if decision in ['BLOCK', 'REVIEW'] else f"Transaction processed normally. Risk score: {decision_data.get('total_risk_score', 0)} (Low)."
            }
            
            # Immediate publish (Enforcement path)
            producer.produce(
                'security-alerts',
                key=txn_id.encode('utf-8'),
                value=json.dumps(alert).encode('utf-8')
            )
            producer.flush()

            # Step 2: Asynchronous AI Enrichment (Only for BLOCK/REVIEW)
            if decision in ['BLOCK', 'REVIEW']:
                print(f"\\n[ALERT] {decision} DETECTED for {decision_data['customer_id']} [ALERT]")
                print(f"Risk Score: {decision_data['total_risk_score']} | Amount: ${decision_data['amount']}")
                
                # Ask Gemini to explain the decision
                prompt = f"""
                You are a fraud prevention AI. A transaction was just flagged.
                Customer: {decision_data['customer_id']}
                Amount: ${decision_data['amount']} ({decision_data['currency']})
                Transaction Country: {decision_data['txn_country']}
                Customer Home Region: {decision_data['home_country']}
                Device ID: {decision_data['device_id']}
                
                Risk Score Breakdown:
                Amount Risk: {decision_data['amount_risk']} (High if > 5000)
                Location Risk: {decision_data['location_risk']} (High if Txn Country != Home)
                Device Risk: {decision_data['device_risk']} (High if New Device)
                
                Total Score: {decision_data['total_risk_score']}
                Action Taken by Flink: {decision}
                
                Write a 2-sentence explanation for a human security analyst explaining exactly WHY this was blocked/reviewed based on the score breakdown.
                """
                max_retries = 3
                explanation = None
                
                for attempt in range(max_retries):
                    try:
                        response = model.generate_content(prompt)
                        explanation = response.text.strip()
                        break # Success! Break out of the retry loop
                    except Exception as api_err:
                        print(f"Gemini API Error on attempt {attempt + 1}: {api_err}")
                        if attempt < max_retries - 1:
                            sleep_time = 2 ** attempt # Exponential backoff: 1s, 2s
                            print(f"Retrying in {sleep_time} seconds...")
                            import time
                            time.sleep(sleep_time)
                        else:
                            print("Max retries reached. Using fallback explanation.")
                            explanation = f"Transaction flagged with score {decision_data['total_risk_score']} due to high-risk indicators."
                    
                print(f"[AI Analysis]: {explanation}")
                
                # Update the alert with the explanation
                alert["ai_explanation"] = explanation
                
                # Publish the updated enrichment
                producer.produce(
                    'security-alerts',
                    key=txn_id.encode('utf-8'),
                    value=json.dumps(alert).encode('utf-8')
                )
                producer.flush()
            else:
                print(f"[NORMAL] Approved transaction for {decision_data.get('customer_id')}")
            
            # Step 3: Commit offset only after successful processing and publishing
            consumer.commit(asynchronous=False)
                
        except Exception as e:
            print(f"Error processing message payload: {e}")

except KeyboardInterrupt:
    print("Stopping AI agent...")
finally:
    consumer.close()
