from pyspark.sql import *
from pyspark.sql import functions as F
import re

spark = SparkSession.builder.appName("csvdata").master("local").getOrCreate()
data="D:\\BigData\\Datasets\\us-500.csv"
#df1=spark.read.format("csv").option("header","true").load(data)
#df1.printSchema() #All string
#df1.show()
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
#df.printSchema() #All string except zip

###########SQL model#####################
df.createOrReplaceTempView("tab")
#res=spark.sql("select state,count(*) cnt from tab group by state order by cnt desc" )
#res.show()

########programming model (DSL operations)###############
#res=df.where(F.col("state")=="NY") #select * from tab where state='NY'
#res.show()
#res=df.filter(F.col("state")=="OH")
#res.show()
#res=df.select("state","city").filter(F.col("state")=="OH")
#res.show()
#res=df.groupBy(F.col("state")).count()
#res.show()
#res=df.groupBy(F.col("state")).count().orderBy(F.col("count"),assending=False)
#res.show()
#res=df.groupBy(F.col("state")).count().sort(F.col("count").desc())
#res.show()
res=spark.sql("select (SUBSTRING_INDEX(SUBSTR(email,INSTR(email, '@') + 1),'.',1)) as domain_name,count(*) cnt from tab group by domain_name order by cnt desc")
res.show()