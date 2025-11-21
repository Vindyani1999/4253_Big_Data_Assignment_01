#!/usr/bin/env python3
import io
import json
import time
from fastavro import schemaless_reader
from confluent_kafka import Consumer, Producer, KafkaError

KAFKA_BOOTSTRAP = "localhost:9092"
CONSUMER_GROUP = "order-consumers"
TOPIC = "orders"
DLQ_TOPIC = "orders-dlq"

# max retry attempts before sending to DLQ
MAX_RETRIES = 3

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": CONSUMER_GROUP,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
})
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

# load schema for deserialization
with open("../order.avsc", "r") as f:
    SCHEMA = json.load(f)

def avro_deserialize(data, schema):
    return schemaless_reader(io.BytesIO(data), schema)

def send_to_dlq(record, original_headers=None):
    payload = json.dumps(record).encode("utf-8")
    headers = original_headers or []
    producer.produce(DLQ_TOPIC, value=payload, headers=headers)
    producer.flush()
    print("[Consumer] Sent to DLQ:", record)

def requeue_with_retry(record_bytes, headers, next_retry):
    # attach/increment retry header - represent headers as list of (k, v)
    new_headers = []
    found = False
    if headers:
        for k, v in headers:
            if k == "retry":
                found = True
                new_headers.append(("retry", str(next_retry).encode()))
            else:
                new_headers.append((k, v))
    if not found:
        new_headers.append(("retry", str(next_retry).encode()))

    producer.produce(TOPIC, value=record_bytes, headers=new_headers)
    producer.flush()
    print(f"[Consumer] Requeued message with retry={next_retry}")

def process_record(record):
    # business logic: compute running average (this function just returns True/False)
    # Simulate a transient failure for illustration when price > 95
    if record["price"] > 95:
        # simulate transient error 50% time
        raise RuntimeError("Simulated processing error for high price")

    return True

running_total = 0.0
count = 0

def parse_retry_from_headers(headers):
    if not headers:
        return 0
    for k, v in headers:
        if k == "retry":
            try:
                return int(v.decode() if isinstance(v, bytes) else v)
            except Exception:
                return 0
    return 0


def main():
    global running_total, count
    consumer.subscribe([TOPIC])
    print("Consumer started. Listening for messages...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                # handle error
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print("Consumer error:", msg.error())
                    continue

            value = msg.value()
            headers = msg.headers() or []
            retry_count = parse_retry_from_headers(headers)

            try:
                record = avro_deserialize(value, SCHEMA)
            except Exception as e:
                print("[Consumer] Failed to deserialize message; sending to DLQ. Error:", e)
                # send raw bytes as dlq payload (or JSON if you prefer)
                send_to_dlq({"raw_bytes_hex": value.hex()}, headers)
                consumer.commit(message=msg)
                continue

            print("[Consumer] Received:", record, "retry_count=", retry_count)

            try:
                # process - may raise
                process_record(record)
                # update running average
                running_total += float(record["price"])
                count += 1
                avg = running_total / count if count else 0.0
                print(f"[Consumer] Processed orderId={record['orderId']}. Running average price = {avg:.2f}")

                consumer.commit(message=msg)  # commit after success

            except Exception as e:
                print("[Consumer] Processing error:", e)
                if retry_count < MAX_RETRIES:
                    # re-publish original bytes to the same topic with incremented retry header
                    next_retry = retry_count + 1
                    requeue_with_retry(value, headers, next_retry)
                    consumer.commit(message=msg)
                else:
                    # permanent failure -> DLQ
                    print("[Consumer] Max retries exceeded. Sending to DLQ.")
                    send_to_dlq(record, headers)
                    consumer.commit(message=msg)

    except KeyboardInterrupt:
        print("Consumer stopped by user.")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
