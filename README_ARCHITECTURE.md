# StreamShield AI: Real-Time Fraud Prevention Architecture

Welcome to **StreamShield AI**, our submission for the Confluent Hackathon! This project demonstrates a production-grade, real-time fraud detection engine powered by Confluent Cloud, Apache Flink, and Google Gemini AI.

## 🏗️ Architecture Flow

Our architecture takes advantage of Kafka's event streaming and Flink's stream processing to make deterministic decisions in milliseconds, while leveraging Generative AI to provide human-readable context for security analysts.

### 1. Data Generation (The "Mock" Core Banking System)
- **`customer-profile` (Topic):** We use Confluent's Datagen Source Connector to stream continuous, mock customer profile data (representing an HR or banking CRM system).
- **`transactions` (Topic):** A custom Python daemon continuously generates mock financial transactions, intelligently mapped to our customer profiles. About 10% of these are intentionally injected as "anomalous" (e.g., massive amounts, new devices, or country mismatch).

### 2. Stream Processing (The Deterministic Engine)
- **`fraud-decisions-mat` (Materialized Table):** Using Confluent Cloud's **Flink SQL**, we created a powerful `MATERIALIZED TABLE`. 
  - Flink continuously performs a `LEFT JOIN` on the streaming `transactions` and the stateful `customer-profile` tables.
  - It calculates a real-time **Risk Score** based on transaction amount, geographic mismatch (Transaction Country vs. Home Region), and device footprint.
  - Flink makes an instant, deterministic decision: **`BLOCK`** (Score >= 70), **`REVIEW`** (Score >= 40), or **`APPROVE`** (otherwise). 
  - *Why this matters:* By using a Materialized Table, we elegantly handle changelog streams and state updates natively, ensuring perfect accuracy even if data arrives out of order.

### 3. AI Enrichment (The Context Engine)
- **AI Agent (Python Consumer):** We built a lightweight Python consumer that listens *only* to the `fraud-decisions-mat` topic.
- **Cost-Effective AI:** Instead of sending every single transaction to the LLM (which is slow and expensive), the AI Agent only triggers when Flink outputs a `BLOCK` or `REVIEW`. 
- **Google Gemini 3.6 Flash:** The agent dynamically parses the Avro data from Flink, constructs a context-rich prompt detailing exactly *why* the transaction scored high, and asks Gemini to write a 2-sentence explanation for a human security analyst.

### 4. Alerting & UI (The Security Operations Center)
- **`security-alerts` (Topic):** The AI Agent publishes the final, enriched payload (containing the transaction details, the Flink action, and the AI explanation) to this topic.
- **Streamlit Dashboard:** A live Python dashboard consumes the `security-alerts` topic and displays the results in a beautiful, auto-refreshing UI for Security Operations teams to monitor in real time.

## 💡 Key Design Decisions
1. **Schema Registry:** All Flink outputs and Python generators use Confluent Schema Registry. Our AI agent dynamically fetches schemas based on the magic byte (0x00), ensuring robust deserialization even if the schema evolves.
2. **Separation of Concerns:** Flink handles the heavy lifting (joins, math, rules), while the LLM handles only what it's good at (language generation and context summarization). This guarantees millisecond latency for the actual fraud block, without waiting for an LLM response.
