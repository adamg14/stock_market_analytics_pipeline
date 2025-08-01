from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, window, to_timestamp
from pyspark.sql.types import StructType, StringType, FloatType

spark = SparkSession.builder.appName("StockDataStreamProcessor").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("ticker", StringType()) \
    .add("interval", StringType()) \
    .add("currency", StringType()) \
    .add("exchange_timezone", StringType()) \
    .add("exchange", StringType()) \
    .add("datetime", StringType()) \
    .add("open", FloatType()) \
    .add("high", FloatType()) \
    .add("low", FloatType()) \
    .add("close", FloatType()) \
    .add("volume", FloatType()) 

stock_df = StructType.readStream().format("kafka").option("kafka.bootstrap.servers", "localhost:29092").option("subscribe", "stock_data").load()

# CONVERT DATETIME COLUMN FROM STRINGTYPE TO DATETIME OBJECT
# code snippet: df = df.withColumn("timestamp_col", to_timestamp("string_datetime_col", "yyyy-MM-dd HH:mm:ss"))
print("hello world")

{'ticker': 'VOD', 'interval': '1min', 'currency': 'USD', 'exchange_timezone': 'America/New_York', 'exchange': 'NASDAQ', 'datetime': '2025-07-31 15:41:00', 'open': '10.81670', 'high': '10.82000', 'low': '10.81', 'close': '10.81500', 'volume': '9927'}