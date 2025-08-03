from pyspark.sql import SparkSession


# successful test PySpark Connection
spark = SparkSession.builder.appName('test').config('spark.jars.packages', 'net.snowflake:spark-snowflake_2.12:3.0.0').getOrCreate()

print('Success!')

spark.stop()