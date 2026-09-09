"""
Producer for MOEX ISS API.
Fetches candle data for specified tickers from a config file and sends to Kafka.
"""

import json
import logging
import os
import time
from typing import cast

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

_ = load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
MOEX_BASE_URL = os.getenv("MOEX_BASE_URL", "https://iss.moex.com/iss")
TICKERS_FILE = os.getenv("TICKERS_FILE", "config/tickers.txt")
INTERVAL_SECONDS = int(os.getenv("MOEX_INTERVAL_SECONDS", "60"))
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_RAW_TRADES", "raw-trades")

# Тип для свечи
CandleDict = dict[str, float | int | str]

# Глобальный продюсер для доступа в finally
producer: KafkaProducer | None = None


def load_tickers(file_path: str) -> list[str]:
    """Загружает список тикеров из файла, игнорируя пустые строки и комментарии."""
    tickers: list[str] = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
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
    except PermissionError:
        logger.error(f"Permission denied reading {file_path}. Using fallback.")
        return ["SBER", "GAZP", "LKOH"]
    except OSError as e:
        logger.error(f"OS error reading {file_path}: {e}. Using fallback.")
        return ["SBER", "GAZP", "LKOH"]


def create_kafka_producer() -> KafkaProducer:
    """Создаёт и возвращает продюсера Kafka."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=5,
            request_timeout_ms=10000,
        )
        logger.info(f"Kafka producer created for {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        raise


def fetch_candles(ticker: str, interval: int = 1) -> list[CandleDict]:
    """Получает свечи для указанного тикера с MOEX."""
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
        data = cast(dict[str, object], response.json())

        candles_data_raw = data.get("candles")
        if not isinstance(candles_data_raw, dict):
            logger.error(
                f"Unexpected response format for {ticker}: 'candles' is not a dict"
            )
            return []
        candles_data = cast(dict[str, object], candles_data_raw)

        columns_obj = candles_data.get("columns")
        rows_obj = candles_data.get("data")
        if not isinstance(columns_obj, list) or not isinstance(rows_obj, list):
            logger.error(f"Invalid columns or data format for {ticker}")
            return []

        columns = cast(list[str], columns_obj)
        rows = cast(list[list[float | int | str]], rows_obj)

        candles: list[CandleDict] = []
        for row in rows:
            if len(row) != len(columns):
                continue
            candle: CandleDict = {}
            for col, val in zip(columns, row):
                candle[col] = val  # type: ignore[assignment]
            candle["ticker"] = ticker
            candles.append(candle)
        return candles

    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching candles for {ticker}: {e}")
        return []
    except (KeyError, ValueError) as e:
        logger.error(f"Unexpected response structure for {ticker}: {e}")
        return []


def close_producer() -> None:
    if producer is not None:
        producer.close()
        logger.info("Producer closed.")


def main() -> None:
    """Основной цикл продюсера: опрос MOEX и отправка в Kafka."""
    global producer
    tickers = load_tickers(TICKERS_FILE)
    logger.info(f"Starting MOEX producer for tickers: {tickers}")
    logger.info(f"Interval: {INTERVAL_SECONDS}s, Kafka topic: {KAFKA_TOPIC}")

    producer = create_kafka_producer()

    while True:
        for ticker in tickers:
            logger.info(f"Fetching candles for {ticker}...")
            candles = fetch_candles(ticker)
            if candles:
                for candle in candles:
                    candle["fetched_at"] = time.time()
                    try:
                        _ = producer.send(KAFKA_TOPIC, value=candle)
                        logger.info(f"Sent candle for {ticker}: {candle['begin']}")
                    except Exception as e:  # noqa: BLE001
                        logger.error(f"Failed to send candle for {ticker}: {e}")
            else:
                logger.warning(f"No candles received for {ticker}")
        logger.info(f"Sleeping for {INTERVAL_SECONDS} seconds...")
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Producer interrupted by user.")
        close_producer()
    except Exception:
        logger.exception("Unhandled exception")
        close_producer()
    logger.info("Producer stopped.")
