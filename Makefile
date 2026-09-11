.PHONY: up down restart status build rebuild init-all init-kafka init-mongodb logs logs-producer logs-consumer logs-kafka shell-consumer shell-producer clean

# ---------- Lifecycle ----------

up:
	docker-compose -f docker/docker-compose.yml up -d

down:
	docker-compose -f docker/docker-compose.yml down

restart: down up

# ---------- Build ----------

# Собрать образы продюсера и консьюмера
build:
	docker-compose -f docker/docker-compose.yml build producer consumer-rs

# Пересобрать без кеша (после серьёзных изменений в коде)
rebuild:
	docker-compose -f docker/docker-compose.yml build --no-cache producer consumer-rs
	docker-compose -f docker/docker-compose.yml up -d

# ---------- Status ----------

status:
	docker-compose -f docker/docker-compose.yml ps

# ---------- Init ----------

init-all: init-kafka init-mongodb

init-kafka:
	./scripts/init_kafka_topics.sh

init-mongodb:
	docker exec -i flowforge-mongodb mongosh -u admin -p password --authenticationDatabase admin flowforge < scripts/init_mongodb_indexes.js

# ---------- Logs ----------

logs:
	docker-compose -f docker/docker-compose.yml logs -f

logs-producer:
	docker-compose -f docker/docker-compose.yml logs -f producer

logs-consumer:
	docker-compose -f docker/docker-compose.yml logs -f consumer-rs

logs-kafka:
	docker-compose -f docker/docker-compose.yml logs -f kafka

# ---------- Shell ----------

shell-producer:
	docker exec -it flowforge-producer /bin/bash

shell-consumer:
	docker exec -it flowforge-consumer-rs /bin/sh

# ---------- Cleanup ----------

clean:
	docker-compose -f docker/docker-compose.yml down -v

clean-images:
	docker rmi flowforge-producer flowforge-consumer-rs 2>/dev/null || true
