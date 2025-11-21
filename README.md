# Kafka Order System (Python)

## Overview
This project implements a Kafka-based system producing and consuming Avro order messages with:
- Real-time aggregation (running average of prices)
- Retry logic for transient failures
- Dead Letter Queue (DLQ) for permanent failures

## 🚀 Quick Start

### 1. Prerequisites
- Docker & Docker Compose
- Python 3.8+

### 2. Start Kafka Infrastructure
```
docker-compose up -d
```
### 3. Create Topics
```
chmod +x create_topics.sh
./create_topics.sh
```

### 4. Setup Python Environment
```
# Producer
cd producer
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# .venv\Scripts\activate  # Windows
pip install -r requirements.txt

# Consumer (new terminal)
cd consumer
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
### 5. Run the System
```
# Terminal 1 - Start Consumer
cd consumer
python consumer.py

# Terminal 2 - Start Producer
cd producer
python producer.py
```
