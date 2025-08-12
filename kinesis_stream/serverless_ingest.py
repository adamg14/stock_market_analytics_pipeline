import os
import json
import logging
import urllib.request
import boto3


try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../.env"))
except Exception:
    pass


# CONSTANTS
KINESIS_STREAM = os.getenv("KINESIS_STREAM_NAME")
API_KEY = os.getenv("API_KEY")
TICKERS = [
    "META",
    "TSLA",
    "GS",
    "VOD"
    ]
OUTPUT_SIZE = 5

# logging 
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Kinesis connection
kinesis = boto3.client("kinesis", region_name="eu-north-1")


def get_stock_data(ticker):
    # gets the data from teh twelvedata API 
    # in the json format {'meta':{...}, 'values':[{...}, {...}, ...]}
    """Fetch stock data from API"""
    try:
        url = f"https://api.twelvedata.com/time_series?symbol={ticker}&interval=1min&apikey={API_KEY}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        print(response.json())
        return response.json()
    except Exception as e:
        logger.error(f"API request failed for {ticker}: {str(e)}")
        return None


def create_kinesis_record(ticker, data_point, meta):
    # formats the kinesis message in the correct format 
    return {
        'Data': json.dumps({
            'ticker': meta.get("symbol", ticker),
            'interval': meta.get("interval", "1min"),
            'currency': meta.get("currency", "USD"),
            'exchange_timezone': meta.get("exchange_timezone", "America/New_York"),
            'exchange': meta.get("exchange", "NASDAQ"),
            'date_timestamp': data_point["datetime"],
            'open_price': data_point["open"],
            'high': data_point["high"],
            'low': data_point["low"],
            'close': data_point["close"],
            'volume': data_point["volume"]
        }),
        'PartitionKey': ticker
    }


def batch_records():
    pass


def lambda_handler(event, context):
    pass



