import json
import boto3
from datetime import datetime
import psycopg2
from dotenv import load_doenv
import os

# constant environment variables
REDSHIFT_ENDPOINT = os.getenv("REDSHIFT_JDBC_URL")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_DATABASE = os.getenv("REDSHIFT_DATABASE")

# Load environment variables
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../.env"))

def lamdba_handler(event, context):
    redshift_connection = psycopg2.connect(
        host = REDSHIFT_ENDPOINT,
        user=REDSHIFT_USER,
        password=REDSHIFT_USER,
        database=REDSHIFT_DATABASE
    )

    redshift_cursor = redshift_connection.cursor()

    for record in event["Records"]:
        data = json.loads(record["kinesis"]["data"])

        redshift_cursor.execute(f"""
            INSERT INTO stock_prices(ticker, interval, currency, exchange_timezone, exchange, date_timestamp, open_price, high, low, close, volume, processing_time) VALUES({data["ticker"]}, {data["interval"]}, {data["currency"]}, {data["exchange_timezone"]}, {data["exchange"]}, {data["date_timestamp"]}, {float(data["open_price"])}, {float(data["high"])}, {float(data["low"])}, {float(data["low"])}, {float(data["close"])}, {float(data["volume"])}, {data["processing_time"]}""")
        
    redshift_connection.commit()
    redshift_cursor.close()
    return {"status": "success"}


if __name__ == '__main':
    lamdba_handler()