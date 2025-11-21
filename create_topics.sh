#!/bin/bash
# run from project root
docker exec -it $(docker ps --filter "ancestor=confluentinc/cp-kafka:7.5.0" -q) \
  kafka-topics --bootstrap-server localhost:9092 --create --topic orders --partitions 1 --replication-factor 1 || true

docker exec -it $(docker ps --filter "ancestor=confluentinc/cp-kafka:7.5.0" -q) \
  kafka-topics --bootstrap-server localhost:9092 --create --topic orders-dlq --partitions 1 --replication-factor 1 || true
