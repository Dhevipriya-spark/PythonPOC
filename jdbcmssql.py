from pyspark.sql import *
from pyspark.sql import functions as F
import re
"""
For EMR to add dependencies create a s3 bucket and add jars to s3 bucket
 either 1.use --jars s3://dhevipriya/mssql in spark submit
 2. add the jar to /usr/lib/spark/jars

Below are errors if config  has jars path  as D:\BigData\drivers
1.java.io.FileNotFoundException: Jar D:\igdata\drivers not found
output:
sometimes D:\BigData\drivers doesnt work so change it to D:/BigData/drivers


2..No filesystem for scheme :D

output:
then add file:///  (file:///D:/Bigdata/drivers) from D:/Bigdata/drivers

3.SparkContext:Failed to add file:///D:/Bigdata/drivers to spark environment
java.lang.IllegalArgumentException:Directory D:\BigData\drivers is not allowed to add jar

output:
then change the path to file:///D:/Bigdata/drivers/sqljdbc42.jar

4. Extra spaces create problem in python.so be careful in spaces in between the lines
"""


spark = SparkSession.builder.appName("jdbcmssql").config("spark.jars","file:///D:/bigdata/drivers/sqljdbc42.jar").master("local").getOrCreate()
sc = spark.sparkContext
msurl = "jdbc:sqlserver://mdabdenmssql.ck6vgv6qae1n.us-east-2.rds.amazonaws.com:1433;databaseName=rafidb;"
msdf = spark.read.format("jdbc").option("user","msuername").option("password","mspassword")\
    .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")\
    .option("dbtable","dept").option("url",msurl).load()
msdf.show()