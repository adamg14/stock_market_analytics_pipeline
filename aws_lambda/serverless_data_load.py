import json
import boto3
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os
import base64

# Load environment variables
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../.env"))

# constant environment variables
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_DATABASE = os.getenv("REDSHIFT_DATABASE")
REDSHIFT_ENDPOINT = os.getenv("REDSHIFT_ENDPOINT")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")
REDSHIFT_PORT= os.getenv("REDSHIFT_PORT")
print(REDSHIFT_USER)


def redshift_database_connection():
    return psycopg2.connect(
        host=REDSHIFT_ENDPOINT,
        port=REDSHIFT_PORT,
        dbname=REDSHIFT_DATABASE,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
        connect_timeout=10
    )


def lambda_handler(event, context=None):
    rows = []
    for record in event.get("Records", []):
        raw_b64 = record["kinesis"]["data"]
        payload = json.loads(base64.b64decode(raw_b64))
        rows.append(
            (
            payload["ticker"],
            payload["interval"],
            payload["currency"],
            payload["exchange_timezone"],
            payload["exchange"],
            payload["date_timestamp"],
            float(payload["open_price"]),
            float(payload["high"]),
            float(payload["low"]),
            float(payload["close"]),
            float(payload["volume"]),
            datetime.utcnow().isoformat()
            )
        )
    
    if not rows:
        return {"status": "no_records"}
    
    database_connection = redshift_database_connection()
    cursor = database_connection.cursor()
    try:
        execute_values(
            cursor,
            """INSERT INTO stock_prices (ticker, "interval", currency, exchange_timezone, exchange, date_timestamp, open_price, high, low, close, volume, processing_time) VALUES %s""",
            rows
        )
        return {"status": "db_load_success"}
    except:
        return {"status": "db_load_failed"}
    finally:
        database_connection.close()


if __name__ == '__main__':
    sample_payload = {
        "ticker": "AAPL",
        "interval": "1m",
        "currency": "USD",
        "exchange_timezone": "America/New_York",
        "exchange": "NASDAQ",
        "date_timestamp": "2025-08-11T14:30:00Z",
        "open_price": 220.12,
        "high": 221.00,
        "low": 219.50,
        "close": 220.44,
        "volume": 123456
    }
    mock_event = {
        "Records": [{
            "kinesis": {
                "data": base64.b64encode(json.dumps(sample_payload).encode()).decode()
            }
        }]
    }

    print(lambda_handler(mock_event))
