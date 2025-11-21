# Kafka Order Processing System

A real-time order processing system using Apache Kafka with Avro serialization, retry logic, and Dead Letter Queue (DLQ) implementation.

## 🎯 Requirements

- ✅ **Avro Serialization** - Using FastAvro for schema-based serialization
- ✅ **Real-time Aggregation** - Running average prices per product
- ✅ **Retry Logic** - Configurable retry attempts for transient failures
- ✅ **Dead Letter Queue** - Permanent failure handling with DLQ
- ✅ **Kafka Integration** - Producer/Consumer pattern with Python

## 🧩 Features

### Real-time aggregation
#### 🧑‍💻 Producer
<img width="1233" height="612" alt="Screenshot 2025-11-21 195550" src="https://github.com/user-attachments/assets/88cd34b3-0aca-45f5-95fc-789ed38802ba" />

#### 🧑‍💻 Consumer

<img width="1525" height="461" alt="Screenshot 2025-11-21 195422" src="https://github.com/user-attachments/assets/5218ea9d-37c0-4168-8a5f-f128441efc73" />

### Retry Logic

<img width="1536" height="495" alt="Screenshot 2025-11-21 195503" src="https://github.com/user-attachments/assets/f85e3db2-4317-48dd-a7fa-4aee413b6f00" />

### Dead Letter Queue

<img width="1536" height="166" alt="Screenshot 2025-11-21 1955031" src="https://github.com/user-attachments/assets/19f4d56d-7ec5-4136-b7c3-f704122419f6" />


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
