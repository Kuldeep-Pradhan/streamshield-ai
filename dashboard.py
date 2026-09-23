import streamlit as st
import os
import json
from confluent_kafka import Consumer
from dotenv import load_dotenv
import threading
import time

load_dotenv()

st.set_page_config(page_title="StreamShield AI", page_icon="🛡️", layout="wide")
st.title("🛡️ StreamShield AI: Real-Time Fraud Dashboard")

# Global state to survive browser refreshes
@st.cache_resource
def get_global_alerts():
    return {}

alerts_dict = get_global_alerts()

# Kafka Consumer configuration
@st.cache_resource
def get_consumer():
    conf = {
        'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.getenv('KAFKA_API_KEY'),
        'sasl.password': os.getenv('KAFKA_API_SECRET'),
        'group.id': 'streamlit-dashboard-group',
        'auto.offset.reset': 'earliest'
    }
    c = Consumer(conf)
    c.subscribe(['security-alerts'])
    return c

consumer = get_consumer()

# Fetch latest messages
def fetch_messages():
    msgs = consumer.consume(num_messages=5, timeout=1.0)
    for msg in msgs:
        if msg is None or msg.error():
            continue
        try:
            alert = json.loads(msg.value().decode('utf-8'))
            txn_id = alert.get('transaction_id')
            if txn_id:
                alerts_dict[txn_id] = alert
        except:
            pass

fetch_messages()

# Calculate counts
approved_count = sum(1 for a in alerts_dict.values() if a.get('action') == 'APPROVE')
review_count = sum(1 for a in alerts_dict.values() if a.get('action') == 'REVIEW')
blocked_count = sum(1 for a in alerts_dict.values() if a.get('action') == 'BLOCK')

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric(label="System Status", value="ACTIVE")
with col2:
    st.metric(label="Approved", value=approved_count)
with col3:
    st.metric(label="Needs Review", value=review_count)
with col4:
    st.metric(label="Blocked", value=blocked_count)

st.markdown("---")
st.subheader("🚨 Live transactions")

if not alerts_dict:
    st.info("No transactions detected recently. Monitoring stream...")
else:
    # Sort by descending order so newest are on top, limit to last 100 for performance
    alerts_list = list(alerts_dict.values())[::-1][:100]
    for alert in alerts_list:
        action = alert.get('action', 'UNKNOWN')
        amt = alert.get('amount', 0)
        score = alert.get('total_risk_score', 0)
        
        title = f"{action} - Customer: {alert.get('customer_id', 'Unknown')} (Txn: {alert.get('transaction_id', 'N/A')}) | Amount: ${amt} | Score: {score}"
        
        with st.expander(title, expanded=True):
            if action == 'BLOCK':
                st.error(f"**AI Analyst Explanation:**\n\n{alert.get('ai_explanation', '')}")
            elif action == 'REVIEW':
                st.warning(f"**AI Analyst Explanation:**\n\n{alert.get('ai_explanation', '')}")
            else:
                st.success(f"**Automated Decision:**\n\n{alert.get('ai_explanation', '')}")

# Auto-refresh mechanism (refreshes every 2 seconds)
time.sleep(2)
st.rerun()
