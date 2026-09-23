Please review our completed architecture and code for the Confluent Hackathon. Our project is named **StreamShield AI**, and it is a real-time, AI-driven fraud prevention system. 

We need you to evaluate if this architecture meets all the requirements for a winning hackathon project, and if the code and Flink SQL schema design are production-grade.

### 1. Goal and Problem Statement
Traditional fraud detection relies on batch processing (which allows fraudulent money to move before being caught) and lacks human-readable context (making it hard for Security Operations to resolve alerts). We built a system that blocks fraud deterministically in milliseconds using Confluent Flink, and enriches the blocked transactions with asynchronous AI explanations using Google Gemini 3.6 Flash.

### 2. Architecture & Topics Design
We are using Confluent Cloud (AWS, us-east-1) with the following topic topology:

*   **`customer-profile` (Mock Data Source):** Continuously populated by the Confluent Datagen Source Connector (Users template). Represents a stateful HR/Banking CRM system.
*   **`transactions` (Mock Data Source):** A custom Python daemon producing JSON_SR financial transactions. It intelligently generates normal transactions (90%), Mild Fraud (5%), and Severe Fraud (10%) by manipulating the amount, country mismatch, and device ID.
*   **`fraud-decisions-mat` (Flink Materialized Table):** The core deterministic engine.
*   **`security-alerts` (Output Sink):** Contains the final, AI-enriched alerts.

### 3. Flink SQL Logic (The Core Engine)
We utilized a Confluent `CREATE MATERIALIZED TABLE` to act as an upserting, continuously updated state store. Flink performs a stream-to-stream `LEFT JOIN` on `transactions` and `customer-profile`. It calculates a risk score (0-100) based on:
*   Amount (> $5000 adds 30 pts)
*   Location Mismatch (Transaction Country != User Home Region adds 20 pts)
*   Device Footprint (New device adds 25 pts)

If the total score is `>= 70`, the action is `BLOCK`. If `>= 40`, the action is `REVIEW`. Otherwise, `APPROVE`. Flink outputs this perfectly in real time, handling all state retractions and changelog management natively because of the Materialized Table constraint (`PRIMARY KEY (transaction_id) NOT ENFORCED`).

### 4. The Python AI Agent (Cost-Optimized AI)
To prevent burning through LLM credits on the 90% of normal transactions, our Python consumer listens *only* to `fraud-decisions-mat`. 
*   It filters strictly for `BLOCK` and `REVIEW` decisions. 
*   It dynamically parses Flink's Avro output using `fastavro` and `SchemaRegistryClient` by checking for the 0x00 magic byte.
*   It passes the risk score breakdown to `gemini-3.6-flash`, asking it to write a 2-sentence explanation for a human Security Analyst.
*   It handles API `429 Quota Exceeded` errors gracefully by pausing for 60 seconds (respecting free-tier limits).
*   It publishes the final enriched JSON to `security-alerts`.

### 5. The UI Dashboard
We built a real-time `Streamlit` dashboard in Python. It consumes from the `security-alerts` topic from the earliest offset, caches the alerts globally to survive browser refreshes, and displays a beautiful, auto-refreshing UI with expanders showing the exact AI Analyst explanation alongside the blocked transaction ID and amount.

---

**Based on this architecture and implementation:**
1. Does this flow (Data -> Flink SQL Materialized Table -> Selective AI Enrichment -> Streamlit UI) represent a top-tier Confluent hackathon entry?
2. Are there any architectural flaws in our use of `CREATE MATERIALIZED TABLE` vs `CREATE TABLE` for the left join?
3. Does our strategy of strictly filtering normal transactions before the LLM step correctly address enterprise cost and latency concerns for GenAI?
