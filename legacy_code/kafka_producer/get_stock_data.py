from kafka import KafkaProducer
import requests, time, json
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../.env"))

STOCK_DATA_API_KEY = os.getenv("API_KEY")
STOCK_TICKERS = ["META", "TSLA", "GS", "VOD"]

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    request_timeout_ms=30000
)

def stock_data(TICKER):
    try:    
        api_url = f"https://api.twelvedata.com/time_series?symbol={TICKER}&interval=1min&apikey={ STOCK_DATA_API_KEY }&source=docs"
        return requests.get(api_url).json()
    except Exception as e:
        print(f"An error occurred when sending a request to the API: {e}")


def schedule_producer_events():
    while True:  
        for ticker in STOCK_TICKERS:
            api_response = stock_data(ticker)
            meta_data = api_response.get("meta", {})
            values = api_response.get("values", [])
            if not values:
                print("Error in API. No values sent for requests")
            else:
                for data_point in values:
                    payload = {
                        "ticker": meta_data.get("symbol", ticker),
                        "interval": meta_data.get("interval", "1min"),
                        "currency": meta_data.get("currency", "USD"),
                        "exchange_timezone": meta_data.get("exchange_timezone", "America/New_York"),
                        "exchange": meta_data.get("exchange", "NASDAQ"),

                        "datetime": data_point["datetime"],
                        "open": data_point["open"],
                        "high": data_point["high"],
                        "low": data_point["low"],
                        "close": data_point["close"],
                        "volume": data_point["volume"]
                    }
                    print(payload)
                    producer.send("stock_data", value=payload)
                    print("Stock data Kafka message sent.")
        # Time series API give the last half hour of time series data
        time.sleep(60 * 30)


if __name__ == '__main__':
    schedule_producer_events()
