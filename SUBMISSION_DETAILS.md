# Confluent Developer Day - Hackathon Submission Details

Use this information to fill out the Google Form:

**Project Name:**
StreamShield AI

**What does your project do? (Description):**
StreamShield AI is a real-time, LLM-enriched fraud prevention system. It solves the critical trade-off between deterministic enforcement (which is fast but lacks context) and GenAI analysis (which is smart but too slow for inline blocking). Using a Confluent Flink Materialized Table, the system evaluates incoming transactions against live customer profiles (Datagen) to instantly calculate risk scores and block fraud. An asynchronous Python AI Agent consumes these decisions, publishes a 0-latency "Pending Analysis" alert to security analysts, queries Gemini for a detailed explanation of the Flink score breakdown, and then pushes an updated enrichment to a `security-alerts` topic. The final output is visualized on a real-time Streamlit dashboard.

**Architecture / Technologies Used:**
- **Confluent Cloud:** Kafka topics (`transactions`, `customer-profile`, `security-alerts`) and Schema Registry (Schema inferred from JSON).
- **Confluent Flink SQL:** Temporal joins and upsert Materialized Tables to evaluate risk inline without microservices.
- **Python / Confluent-Kafka:** Custom producers and consumers to integrate streaming with external APIs.
- **Google Gemini:** Asynchronous LLM enrichment.
- **Streamlit:** Live, deduplicated UI for security analysts.

**Link to Code Repository:**
*(Insert your GitHub link here once you push the code!)*

**Screenshots Required:**
You can upload the screenshots you shared earlier:
1. `media_1790142772416.png` (Confluent infer schema)
2. `media_1790142004606.png` (Streamlit Dashboard)
3. `media_1790108274651.png` (Flink workspace)
4. (Any other architectural diagrams you have)
