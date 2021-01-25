from pyspark.sql import *
from pyspark.sql import functions as F
import re
spark = SparkSession.builder.appName("bankudf").master("local").getOrCreate()
data="D:\\BigData\\Datasets\\bank-full.csv"
df=spark.read.format("csv").option("header","true").option("inferschema","true").option("delimiter",";").load(data)
df.createOrReplaceTempView("tab")
df.printSchema()
res=spark.sql("select * from  tab where balance>70000 and  marital='married'")
#Domain specific language
res=df.where((F.col("balance")>70000) & (F.col("marital")=="married"))  # and
#res=df.where((F.col("balance")>70000) and (F.col("marital")=="married"))
#res= spark.sql("select *,concat(job,'-',marital,'-',balance) fullname,concat_ws('-',job,marital,balance) fullname_ws from tab")
#res=df.withColumn("fullname",F.concat_ws("-",F.col("job"),F.col("marital"),F.col("balance"))).withColumn("fullname1",F.concat(F.col("job"),F.lit("-"),F.col("marital"),F.lit("-"),F.col("balance")))
res.show()