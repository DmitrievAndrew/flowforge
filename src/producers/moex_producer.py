#!/usr/bin/env python3
"""
Producer for MOEX ISS API.
Fetches candle data for specified tickers from a config file and sends to Kafka.
"""

import os
import time
import json
import logging
from typing import List, Dict, Any

import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
MOEX_BASE_URL = os.getenv("MOEX_BASE_URL", "https://iss.moex.com/iss")
TICKERS_FILE = os.getenv("TICKERS_FILE", "config/tickers.txt")
INTERVAL_SECONDS = int(os.getenv("MOEX_INTERVAL_SECONDS", "60"))
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_RAW_TRADES", "raw-trades")

def load_tickers(file_path: str) -> List[str]:
    tickers = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                tickers.append(line)
        if not tickers:
            logger.warning(f"No tickers loaded from {file_path}. Using fallback.")
            return ["SBER", "GAZP", "LKOH"]
        logger.info(f"Loaded {len(tickers)} tickers from {file_path}")
        return tickers
    except FileNotFoundError:
        logger.error(f"Tickers file {file_path} not found. Using fallback.")
        return ["SBER", "GAZP", "LKOH"]
    except Exception as e:
        logger.error(f"Error reading tickers file: {e}. Using fallback.")
        return ["SBER", "GAZP", "LKOH"]

def create_kafka_producer() -> KafkaProducer:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=5,
            request_timeout_ms=10000,
            api_version=(3, 5, 0),
        )
        logger.info(f"Kafka producer created for {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        raise

def fetch_candles(ticker: str, interval: int = 1) -> List[Dict[str, Any]]:
    url = f"{MOEX_BASE_URL}/engines/stock/markets/shares/boards/TQBR/securities/{ticker}/candles.json"
    params = {
        "from": time.strftime("%Y-%m-%d"),
        "till": time.strftime("%Y-%m-%d"),
        "interval": interval,
        "start": 0,
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        columns = data['candles']['columns']
        rows = data['candles']['data']
        candles = []
        for row in rows:
            candle = dict(zip(columns, row))
            candle['ticker'] = ticker
            candles.append(candle)
        logger.info(f"Sent candle for {ticker}: {candle['begin']}")
        return candles
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching candles for {ticker}: {e}")
        return []
    except KeyError as e:
        logger.error(f"Unexpected response structure for {ticker}: {e}")
        return []

def main():
    tickers = load_tickers(TICKERS_FILE)
    logger.info(f"Starting MOEX producer for tickers: {tickers}")
    logger.info(f"Interval: {INTERVAL_SECONDS}s, Kafka topic: {KAFKA_TOPIC}")

    producer = create_kafka_producer()

    while True:
        for ticker in tickers:
            logger.info(f"Fetching candles for {ticker}...")
            candles = fetch_candles(ticker)
            logger.info(f"Fetched {len(candles)} candles for {ticker}")
            if candles:
                for candle in candles:
                    candle['fetched_at'] = time.time()
                    try:
                        producer.send(KAFKA_TOPIC, value=candle)
                        logger.info(f"Sent candle for {ticker}: {candle['begin']}")
                    except Exception as e:
                        logger.error(f"Failed to send candle for {ticker}: {e}")
                producer.flush()  # гарантируем доставку
            else:
                logger.warning(f"No candles received for {ticker}")
        logger.info(f"Sleeping for {INTERVAL_SECONDS} seconds...")
        time.sleep(INTERVAL_SECONDS)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Producer interrupted by user.")
    except Exception as e:
        logger.exception(f"Unhandled exception: {e}")
    finally:
        if 'producer' in locals():
            producer.close()
        logger.info("Producer stopped.")