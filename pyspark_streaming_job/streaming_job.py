from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, window, to_timestamp, count
from pyspark.sql.types import StructType, StringType, FloatType
import time
import os
import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

if project_root not in sys.path:
    sys.path.insert(0, project_root)

from snowflake_connection.connection import snowflake_connection_params

packages = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
    "net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3"
]
# add before getOrCreate
# for latest updates only 
    # .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints/stock_data") \
spark = SparkSession.builder \
    .appName("StockDataStreamProcessor") \
    .config("spark.jars.packages",
            ","
            .join(packages)) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("ticker", StringType()) \
    .add("interval", StringType()) \
    .add("currency", StringType()) \
    .add("exchange_timezone", StringType()) \
    .add("exchange", StringType()) \
    .add("datetime", StringType()) \
    .add("open", StringType()) \
    .add("high", StringType()) \
    .add("low", StringType()) \
    .add("close", StringType()) \
    .add("volume", StringType()) 

# # change startingOffsets to latests for latest updates only
stock_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "stock_data") \
    .option("startingOffsets", "earliest") \
    .load()

# parse the json kafka message using PySpark
parsed_json = stock_df.select(
    from_json(col("value").cast("string"), schema).alias("parsed_data")
).select("parsed_data.*")

# converting columns to the correct data type
parsed_json = parsed_json \
    .withColumn(
        "datetime", 
        to_timestamp(col("datetime"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn(
        "open",
        col("open").cast(FloatType())
    ) \
    .withColumn(
        "low",
        col("low").cast(FloatType())
    ) \
    .withColumn(
        "close",
        col("close").cast(FloatType())
    ) \
    .withColumn(
        "volume",
        col("volume").cast(FloatType())
    )

# querying the number of rows to verify length of dataframe
count_query = parsed_json.agg(count("*").alias("total_rows")) \
    .writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("row_counts") \
    .start()

# check for the result of the above query
while True:
    try:
        result = spark.sql("SELECT * FROM row_counts").collect()
        if result:
            current_count = result[0]["total_rows"]
            print(f"Current row count: {current_count}")
            if current_count != 0:
                break
        else:
            print("Waiting for data... (0 rows processed so far)")
    except Exception as e:
        print(f"Error checking count: {str(e)}")
    
    time.sleep(5)

# querying the parsed json data to verify it fits with the schema
# ADD .option("checkpointLocation", "/tmp/checkpoints/stock_data_snowflake") before .start()

def write_to_snowflake(batch_df, batch_id):
    batch_df.write.format("snowflake") \
    .options(**snowflake_connection_params) \
    .option("dbtable", "RAW_PRICES") \
    .mode("append") \
    .save()

parsed_json.writeStream \
    .foreachBatch(write_to_snowflake) \
    .option("checkpointLocation", "/tmp/checkpoints/stk") \
    .start() \
    .awaitTermination()

print("DataFrame added to snowflake db.")
# CONVERT DATETIME COLUMN FROM STRINGTYPE TO DATETIME OBJECT
# code snippet: df = df.withColumn("timestamp_col", to_timestamp("string_datetime_col", "yyyy-MM-dd HH:mm:ss"))

