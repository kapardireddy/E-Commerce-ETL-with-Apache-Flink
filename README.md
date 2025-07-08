# E-commerce Real-Time ETL Pipeline with PyFlink, Kafka, PostgreSQL, and Elasticsearch

This project implements a real-time data pipeline that ingests synthetic sales transaction data from a simulated e-commerce environment using Kafka, processes it with Apache Flink (via PyFlink), and stores structured outputs in PostgreSQL and Elasticsearch. The stack is orchestrated using Docker Compose and designed for extensibility and analytics.

---

## Project Overview

- **Source**: Synthetic E-commerce Sales Data (via Python script using Faker)
- **Streaming Engine**: Apache Kafka
- **Stream Processing**: Apache Flink (PyFlink)
- **Storage**:
  - Structured aggregates in PostgreSQL
  - Raw transactions in Elasticsearch
- **Orchestration**: Docker Compose

---

## What the Pipeline Does

- Streams real-time sales transactions into Kafka
- Processes transactions in PyFlink:
  - Daily sales totals
  - Monthly sales totals
  - Sales by product category
- Sinks aggregates to PostgreSQL for analytics
- Sinks raw transaction data to Elasticsearch

---
## Key Features

- Real-time stream processing with PyFlink
- Full observability with PostgreSQL and Elasticsearch
- End-to-end pipeline with Docker Compose setup
- Resilient, restartable Kafka input simulation
- Modular and production-ready project structure

---

##  Setup Instructions

###  Prerequisites

- Python 3.8+
- Docker + Docker Compose
- Basic understanding of Flink, Kafka, and SQL

---

### Install Required Python Packages (For Kafka Producer)

```bash
cd producer
pip install -r requirements.txt
```

**Contents of `requirements.txt`:**

```text
faker
confluent-kafka
```

---

### Start All Services

In the project root:

```bash
docker-compose up --build
```

give some time for it to start
---

### Start Kafka Producer

From a separate terminal:

```bash
cd producer
python main.py
```

This will send fake e-commerce transactions to Kafka for 2 minutes.

---

### Run the PyFlink Job

Open a new terminal and login to the Flink JobManager container:

```bash
docker exec -it ecommerceetlwithapacheflink-jobmanager-1 bash
python3 /opt/flink/jobs/job.py
```

This  will:
- Read from Kafka
- Transform and aggregate data
- Write results to PostgreSQL and Elasticsearch

---

## Output & Verification

### PostgreSQL

Connect using a tool like pgAdmin or CLI:

```bash
docker exec -it postgres psql -U flinkuser -d flinkuser
```

Query the results:

```sql
SELECT * FROM sales_per_day;
SELECT * FROM sales_per_month;
SELECT * FROM sales_per_category;
```

### Elasticsearch

Access Elasticsearch at [http://localhost:9200](http://localhost:9200)

Query available indices:

```bash
curl -X GET "localhost:9200/_cat/indices?v"
```

---

## Future Improvements

- Add Kibana for real-time Elasticsearch visualizations
- Implement deduplication or late-event handling
- Persist enriched or joined data with product/customer streams
- Load long-term data into data lakes or cloud warehouses
- Integrate failure alerting and monitoring

---

## Author

Built by a Data Engineer focused on real-time, production-grade data pipelines.  
For questions or collaboration, reach out to **kapardi21@gmail.com**.

---