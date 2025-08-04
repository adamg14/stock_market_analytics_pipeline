import findspark 
findspark.init()

from pyspark.sql import SparkSession 
spark = SparkSession.builder.appName("test").getOrCreate()

print("OK: ", spark.version)
spark.stop()