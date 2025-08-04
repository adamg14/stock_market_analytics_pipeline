import sys, os, time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StringType, FloatType
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '../.env'))

root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if root not in sys.path:
    sys.path.insert(0, root)

load_dotenv(os.path.join(root, ".env"))

from redshift_connection.connection import redshift_connection_params

# start the Spark session with the required packages to make a connection to the snowflake DB
spark = SparkSession.builder \
    .appName("StockDataStreamProcessor") \
    .config("spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
        "com.amazon.redshift:redshift-jdbc42:2.1.0.12,"
        "org.apache.spark:spark-avro_2.12:3.5.1") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.maxResultSize", "1g") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# loggin the version to the console for verification 
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
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "stock_data") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load() \
  .select(from_json(col("value").cast("string"), schema).alias("d")) \
  .select("d.*") \
  .withColumn("datetime", to_timestamp("datetime","yyyy-MM-dd HH:mm:ss")) \
  .withColumn("open",  col("open").cast(FloatType())) \
  .withColumn("high",  col("high").cast(FloatType())) \
  .withColumn("low",   col("low").cast(FloatType())) \
  .withColumn("close", col("close").cast(FloatType())) \
  .withColumn("volume",col("volume").cast(FloatType()))

# write micro-batches to Snowflake
def write_redshift(batch_df, batch_id):
    try:
        if batch_df.count() > 0:

            batch_df = batch_df.withColumn("processing_time", current_timestamp())

            print(f"Processing batch {batch_id} with {batch_df.count()} records...")
            batch_df.write \
                .format("jdbc") \
                .options(**redshift_connection_params) \
                .option("dbtable", os.getenv("REDSHIFT_TABLE")) \
                .mode("append") \
                .save()
            print(f"Batch {batch_id}: Successfully written {batch_df.count()} records added to Redshift.")
        else:
            print(f"Batch {batch_id}: No data to process")
    except Exception as e:
        # can also add failed batches to S3
        print(f"Batch {batch_id} error: {e}")

# start streaming query
print("Starting streaming query...")
query = df.writeStream \
    .trigger(processingTime="10 seconds") \
    .foreachBatch(write_redshift) \
    .option("checkpointLocation", "/tmp/checkpoints/redshift_stock_data") \
    .start()

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