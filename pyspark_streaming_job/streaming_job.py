import sys
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    to_timestamp,
    current_timestamp
)
from pyspark.sql.types import (
    StructType,
    StringType,
    FloatType
)
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '../.env'))

current_dir = os.path.dirname(__file__)
project_root = os.path.abspath(os.path.join(current_dir, os.pardir))

if project_root not in sys.path:
    sys.path.insert(0, project_root)

from redshift_connection.connection import redshift_connection_params

# start the Spark session with the required packages to make a connection to the snowflake DB
spark = SparkSession.builder \
    .appName("KafkaStreaming") \
    .config("spark.jars.packages", 
           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
           "org.apache.kafka:kafka-clients:3.4.0") \
    .config("spark.jars.ivy", "/tmp/.ivy2") \
    .getOrCreate()



spark.sparkContext.setLogLevel("WARN")

# logging the version to the console for verification 
print(f"Spark session created successfully")
print(f"Spark version: {spark.version}")
print(f"Java version: {spark.sparkContext._jvm.System.getProperty('java.version')}")

# schema = StructType() \
#     .add("ticker", StringType()) \
#     .add("interval", StringType()) \
#     .add("currency", StringType()) \
#     .add("exchange_timezone", StringType()) \
#     .add("exchange", StringType()) \
#     .add("datetime", StringType()) \
#     .add("open", StringType()) \
#     .add("high", StringType()) \
#     .add("low", StringType()) \
#     .add("close", StringType()) \
#     .add("volume", StringType())

# # defining the streaming dataframe
# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "kafka:9092") \
#     .option("subscribe", "stock_data") \
#     .load()

# query = df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()