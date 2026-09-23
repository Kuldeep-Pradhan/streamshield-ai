import os
from dotenv import load_dotenv
from confluent_kafka.admin import AdminClient, NewTopic

# Load credentials from .env
load_dotenv()

# Set up the AdminClient configuration
conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('KAFKA_API_KEY'),
    'sasl.password': os.getenv('KAFKA_API_SECRET'),
}

# Create the AdminClient instance
admin_client = AdminClient(conf)

# Define the topics to create
# A single partition is enough for our hackathon demo, but setting 6 shows production readiness.
topics_to_create = [
    NewTopic("customer-profile", num_partitions=6, replication_factor=3),
    NewTopic("transactions", num_partitions=6, replication_factor=3),
    NewTopic("fraud-decisions", num_partitions=6, replication_factor=3),
    NewTopic("risk-events", num_partitions=6, replication_factor=3),
    NewTopic("security-alerts", num_partitions=6, replication_factor=3)
]

print("Connecting to Confluent Cloud to create topics...")
fs = admin_client.create_topics(topics_to_create)

for topic, f in fs.items():
    try:
        f.result()  # The result itself is None
        print(f"Topic '{topic}' created successfully.")
    except Exception as e:
        # Check if the topic already exists
        if "Topic_ALREADY_EXISTS" in str(e) or "already exists" in str(e).lower():
            print(f"Topic '{topic}' already exists.")
        else:
            print(f"Failed to create topic '{topic}': {e}")
