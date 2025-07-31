from kafka import KafkaProducer
import requests, time, json

API_KEY = ""
STOCK_TICKERS = ["META", "TSLA", "GS", "VOD"]
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def stock_data(TICKER):
    api_url = f"https://api.twelvedata.com/time_series?symbol={TICKER}&interval=1min&apikey={ API_KEY }&source=docs"
    api_response = requests.get(api_url).json()
    producer.send(api_response)
    print(f"Price update event sent.")


def schedule_producer_events():
    for ticker in STOCK_TICKERS:
        stock_data(ticker)
    time.sleep(60)

while True:
    schedule_producer_events()

