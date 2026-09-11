FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements/producer.txt /app/requirements/producer.txt
RUN pip install --no-cache-dir -r /app/requirements/producer.txt

COPY src/producers/ /app/src/producers/
COPY config/ /app/config/

CMD ["python", "src/producers/moex_producer.py"]
