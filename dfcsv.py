from pyspark.sql import *
spark=SparkSession.builder.appName("test").master("local").getOrCreate()