import os
import json
import logging
import urllib.request
import urllib.error
import urllib.parse
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
REGION = "eu-north-1"

# logging 
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Kinesis connection
kinesis = boto3.client("kinesis", region_name="eu-north-1")


def http_json(url, timeout=10):
    """This function makes a HTTP GET request to get the raw stock data from the API and then returns the response in a JSON format"""
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "lambda-stock-ingestor/1.0"}
    )

    with urllib.request.urlopen(request, timeout=timeout) as response:
        raw_reponse = response.read()
        return json.loads(raw_reponse.decode("utf-8"))


def get_stock_data(ticker):
    url = (
        "https://api.twelvedata.com/time_series"
        f"?symbol={urllib.parse.quote(ticker)}"
        f"&interval=1min&outputsize={OUTPUT_SIZE}&format=JSON&apikey={urllib.parse.quote(API_KEY)}"
    )

    try:
        data = http_json(url)
        if isinstance(data, dict) and data.get("status") == "error":
            logger.error(f"Twelve Data error for {ticker}: {data.get('message')}")
            return None
        elif "values" not in data or "meta" not in data:
            logger.error(f"Unexpected API shape for {ticker}: {data}")
            return None
        return data
    except urllib.error.HTTPError as e:
        logger.error(f"HTTP error for {ticker}: {e.code} {e.reason}")
    except urllib.error.URLError as e:
        logger.error(f"URL error for {ticker}: {e.reason}")
    except Exception as e:
        logger.exception(f"Unhandled error fetching {ticker}: {e}")
    return None


def kinesis_record(ticker, data, meta):
    """This function converts the raw JSON response from the API into a formatted AWS kinesis record"""
    payload = {
        "ticker": meta.get("symbol", ticker),
        "interval": meta.get("interval", "1min"),
        "currency": meta.get("currency", "USD"),
        "exchange_timezone": meta.get("exchange_timezone", "America/New_York"),
        "exchange": meta.get("exchange", "NASDAQ"),
        "date_timestamp": data.get("datetime"),
        "open_price": data.get("open"),
        "high": data.get("high"),
        "low": data.get("low"),
        "close": data.get("close"),
        "volume": data.get("volume"),
    }

    return {
        "Data": json.dumps(payload).encode("utf-8"),
        "PartitionKey": ticker,
    }


def chunk(list, size):
    for i in range(0, len(list), size):
        yield list[i : i + size]
    

def batch_records(stream_name, records):
    """Sends a batch of formatted data points to Kinesis simulatenously"""
    if not records:
        return None
    else:
        for batch in chunk(records, 500):
            response = kinesis.put_records(StreamName=stream_name, Records=batch)
            failed = response.get("FailedRecordCount")
            if failed:
                logger.error(
                        f"Kinesis error for PK={records.get('PartitionKey')}: "
                        f"{response.get('ErrorCode')} - {response.get('ErrorMessage')}"
                    )
            else:
                logger.info(
            f"PutRecords: total={len(batch)} failed={failed} request_id={response.get('ResponseMetadata', {}).get('RequestId')}"
            )

def lambda_handler(event, context):
    all_records = []
    
    for ticker in TICKERS:
        data = get_stock_data(ticker)
        if not data:
            continue
        meta = data.get("meta", {})
        values = data.get("values", [])

        for data in values:
            all_records.append(kinesis_record(ticker, data, meta))
    
    batch_records(KINESIS_STREAM, all_records)
    
    return {"statusCode": 200, "body": f"Published {len(all_records)} records to {KINESIS_STREAM}"}

