.PHONY: up down restart init-logs init-all status clean

up:
	docker-compose -f docker/docker-compose.yml up -d

down:
	docker-compose -f docker/docker-compose.yml down

restart: down up

init-all: init-kafka init-clickhouse init-mongodb

init-kafka:
	./scripts/init_kafka_topics.sh

init-clickhouse:
	cat scripts/init_clickhouse_tables.sql | docker exec -i flowforge-clickhouse clickhouse-client --password=clickhouse

init-mongodb:
	docker exec -i flowforge-mongodb mongosh -u admin -p password --authenticationDatabase admin flowforge < scripts/init_mongodb_indexes.js

status:
	docker-compose -f docker/docker-compose.yml ps

clean:
	docker-compose -f docker/docker-compose.yml down -v

install-prod:
	pip install -r requirements/prod.txt

install-dev:
	pip install -r requirements/dev.txt
