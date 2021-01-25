from pyspark.sql import *
from pyspark.sql import functions as F
import re
from pyspark.sql import functions as F
#https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/functions.html
spark = SparkSession.builder.appName("csvdata").master("local").getOrCreate()
data = "file:///D:\\bigdata\\datasets\\zips.json"
df = spark.read.format("json").load(data)
df.show()
df.createOrReplaceTempView("tab")
df.printSchema()
qry = "select state, city from tab group by state, city order by city asc "
res = spark.sql(qry)
res.printSchema()
res.show(5)

#abs ==Negative value gets converted positive value
qry1= "select _id id,city,F.abs(loc[0]) Long,loc[1] Lat,pop,state from tab where state ='NJ'"
res2 = spark.sql(qry1)
res2.show()