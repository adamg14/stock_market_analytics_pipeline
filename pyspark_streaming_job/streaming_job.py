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

from redshift_connection.connection import redshift_connection_params

# start the Spark session with the required packages to make a connection to the snowflake DB
spark = SparkSession.builder \
    .appName("StockDataStreamProcessor") \
    .config("spark.jars.packages",
            ",".join([
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
                "com.amazonaws:aws-java-sdk-bundle:1.12.430",
                "com.databricks:spark-redshift_2.12:4.0.2"
            ])) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# logging the version to the console for verification 
print(f"Spark session created successfully")
print(f"Spark version: {spark.version}")
print(f"Java version: {spark.sparkContext._jvm.System.getProperty('java.version')}")

# define the JSON schema of the Kafka Messages
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

# read from Kafka messages
df = (
    spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:29092") \
        .option("subscribe", "stock_data") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load() \
        .select(from_json(col("value").cast("string"), schema).alias("d")) \
        .select("d.*") \
        .withColumn("datetime_timestamp", to_timestamp("datetime","yyyy-MM-dd HH:mm:ss")) \
        .withColumn("open_price",  col("open").cast(FloatType())) \
        .withColumn("high",  col("high").cast(FloatType())) \
        .withColumn("low",   col("low").cast(FloatType())) \
        .withColumn("close", col("close").cast(FloatType())) \
        .withColumn("volume",col("volume").cast(FloatType())) \
        .select(
            "ticker",
            "interval",
            "currency",
            "exchange_timezone",
            "exchange",
            "date_timestamp",
            "open_price",
            "high",
            "low",
            "close",
            "volume"
        )
)

# write micro-batches to Snowflake
def write_to_redshift(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print(f"Batch {batch_id} has no new rows")
        return
    else:
        batch_df = batch_df.withColum("processing_time", current_timestamp())

        # spark-databricks redshift connector, stages the PySpark dataframe to S3 and runs copy
        (
            batch_df.write \
                .format("com.databricks.spark-redshift") \
                .option("url", redshift_connection_params["url"]) \
                .option("dtable", redshift_connection_params["temp_s3_directory"]) \
                .option("tempdir", redshift_connection_params["temp_s3_directory"]) \
                .option("aws_iam_role", redshift_connection_params["aws_iam_role"]) \
                .mode("append") \
                .save()
        )
        count = batch_df.count()
        print(f"Batch {batch_id} successfully wrote {count} rows into {redshift_connection_params["redshift_table"]}")
# start streaming query
print("Starting streaming query...")
query = (
    df.writeStream \
        .trigger(processingTime="10 seconds") \
        .foreachBatch(write_to_redshift) \
        .option("checkpointLocation", "/tmp/checkpoints/redshift_stream")
        .start()
)

try:
    print("Streaming job is running.")
    query.awaitTermination()
except KeyboardInterrupt:
    print("Stopping streaming job...")
    query.stop()
except Exception as e:
    print(f"Streaming job error: {e}")
    query.stop()
finally:
    spark.stop()
    print("Spark session stopped. Done!")