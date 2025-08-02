from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, window, to_timestamp
from pyspark.sql.types import StructType, StringType, FloatType

spark = SparkSession.builder \
    .appName("StockDataStreamProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
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

# querying the parsed json data to verify it fits with the schema
query = parsed_json.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()

# CONVERT DATETIME COLUMN FROM STRINGTYPE TO DATETIME OBJECT
# code snippet: df = df.withColumn("timestamp_col", to_timestamp("string_datetime_col", "yyyy-MM-dd HH:mm:ss"))
print("hello world")

