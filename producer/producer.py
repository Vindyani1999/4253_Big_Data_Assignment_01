#!/usr/bin/env python3
import io
import json
import time
import uuid
import random
from fastavro import schemaless_writer
from confluent_kafka import Producer, KafkaError

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "orders"

# Load schema
with open("../order.avsc", "r") as f:
    SCHEMA = json.load(f)

p = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

products = ["Item1", "Item2", "Item3", "Item4"]

def avro_serialize(record, schema):
    """Return bytes for avro record using schemaless_writer"""
    buf = io.BytesIO()
    schemaless_writer(buf, schema, record)
    return buf.getvalue()

def delivery_report(err, msg):
    if err is not None:
        print(f"[Producer] Delivery failed: {err}")
    else:
        print(f"[Producer] Delivered message to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")

def produce_order(record, max_attempts=5):
    attempt = 0
    backoff = 1.0
    while attempt < max_attempts:
        try:
            data_bytes = avro_serialize(record, SCHEMA)
            p.produce(TOPIC, value=data_bytes, callback=delivery_report)
            p.poll(0)  # serve delivery callbacks
            return True
        except Exception as e:
            attempt += 1
            print(f"[Producer] Transient error producing (attempt {attempt}/{max_attempts}): {e}")
            time.sleep(backoff)
            backoff *= 2
    print("[Producer] Failed to produce after retries. Giving up for this record.")
    return False

def gen_random_order():
    return {
        "orderId": str(uuid.uuid4()),
        "product": random.choice(products),
        "price": round(random.uniform(5.0, 120.0), 2)
    }

if __name__ == "__main__":
    print("Starting producer. Press Ctrl+C to stop.")
    try:
        while True:
            order = gen_random_order()
            print("[Producer] Sending:", order)
            produce_order(order)
            p.flush(timeout=5)  # wait for outstanding deliveries
            time.sleep(1)  # produce one per second
    except KeyboardInterrupt:
        print("Producer stopped by user.")
