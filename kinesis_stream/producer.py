import boto3
import requests
import json
import time
import os
import logging
from dotenv import load_dotenv
from botocore.config import Config

# Initialise logging
logging.basicConfig(
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../.env"))

# CONSTANTS
KINESIS_STREAM_NAME = "raw-data-streaming"
STOCK_TICKERS = ["META", "TSLA", "GS", "VOD"]
API_KEY = os.getenv("API_KEY")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
BATCH_SIZE = 100  
POLL_INTERVAL = 1800

# initialises Kinesis client
kinesis = boto3.client(
    'kinesis',
    region_name=AWS_REGION,
    config=Config(
        retries={
            "max_attempts": 3,
            "mode": "adaptive"
        }
    )
)

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

def send_to_kinesis(records):
    try:
        response = kinesis.put_records(
            Records=records,
            StreamName=KINESIS_STREAM_NAME
        )
        print("Kinesis Response:", response) 
        if response['FailedRecordCount'] > 0:
            for record in response['Records']:
                if 'ErrorCode' in record:
                    logger.error(f"Failed record: {record['ErrorCode']} - {record['ErrorMessage']}")
                    return False
        else:
            return True
    except Exception as e:
        logger.error(f"Kinesis write failed: {str(e)}")
        return False

def produce_events():
    """Main producer loop with batch processing"""
    while True:
        batch = []
        
        for ticker in STOCK_TICKERS:
            data = get_stock_data(ticker)
            if not data or 'values' not in data:
                continue
                
            meta = data.get('meta', {})
            for data_point in data['values']:
                record = create_kinesis_record(ticker, data_point, meta)
                batch.append(record)
                
                if len(batch) >= BATCH_SIZE:
                    if send_to_kinesis(batch):
                        batch = []  # Clear only on success
                    else:
                        logger.warning("Retaining failed batch for retry")
        
        # Send any remaining records
        if batch:
            send_to_kinesis(batch)
        
        time.sleep(300)

if __name__ == '__main__':
    
    logger.info(f"Starting producer for {', '.join(STOCK_TICKERS)}")
    try:
        produce_events()
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        raise