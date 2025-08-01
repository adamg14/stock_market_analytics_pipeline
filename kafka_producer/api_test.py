import requests, time, json
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../.env"))

STOCK_DATA_API_KEY = os.getenv("API_KEY")
STOCK_TICKERS = ["META", "TSLA", "GS", "VOD"]

api_url = f"https://api.twelvedata.com/time_series?symbol=META&interval=1min&apikey={ STOCK_DATA_API_KEY }&source=docs"

api_response = requests.get(api_url).json()


print(api_response)
