from pyspark.sql import *
from pyspark.sql import functions as F
import re
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("csvdata").master("local").getOrCreate()
df = spark.createDataFrame([
      (7369, "SMITH", "CLERK", 7902, "17-Dec-80", 800, 20, 10),
      (7499, "ALLEN", "SALESMAN", 7698, "20-Feb-81", 1600, 300, 30),
      (7521, "WARD", "SALESMAN", 7698, "22-Feb-81", 1250, 500, 30),
      (7566, "JONES", "MANAGER", 7839, "2-Apr-81", 2975, 0, 20),
      (7654, "MARTIN", "SALESMAN", 7698, "28-Sep-81", 1250, 1400, 30),
      (7698, "BLAKE", "MANAGER", 7839, "1-May-81", 2850, 0, 30),
      (7782, "CLARK", "MANAGER", 7839, "9-Jun-81", 2450, 0, 10),
      (7788, "SCOTT", "ANALYST", 7566, "19-Apr-87", 3000, 0, 20),
      (7839, "KING", "PRESIDENT", 0, "17-Nov-81", 5000, 0, 10),
      (7840,"VENKAT", "SALESMAN", 0, "17-Nov-81", 900, 0, 30),
      (7844, "TURNER", "SALESMAN", 7698, "8-Sep-81", 1500, 0, 30),
      (7845, "VENU", "SALESMAN", 7698, "8-Sep-81", 1000, 0, 30),
      (7876, "ADAMS", "CLERK", 7788, "23-May-87", 1100, 0, 20)]
    ).toDF("empno", "ename", "job", "mgr", "hiredate", "sal", "comm", "deptno")
df.createOrReplaceTempView("tab")
#person getting maximum salary
res=spark.sql("select * from tab where sal=(select max(sal) from tab)")
#res.show()
#############SparkSQL way
res1=spark.sql("select *,rank() over(partition by job order by sal desc) rank from tab")
#res1.show()
res2=spark.sql("select *,dense_rank() over(partition by job order by sal desc) dense_rank,row_number() over (partition by job order by sal desc) rownumber from tab")
#res2.show()

#DSL way##########Ranking function
##1-100 age .. 1 to 20 rank- l..20-40 rank-2 40-60  rank3 60-80;...5 rank
##rank+percent=percent rank (Skip rank when there is a duplicate)
WinSpec=Window.partitionBy(F.col("job")).orderBy(F.col("sal").desc())
res3=df.withColumn("rank",F.rank().over(WinSpec)).withColumn("denserank",F.dense_rank().over(WinSpec)).withColumn("rownumber",F.row_number().over(WinSpec))\
.withColumn("percentrank",100*F.percent_rank().over(WinSpec)).withColumn("ntile4",F.ntile(4).over(WinSpec)).withColumn("ntile2",F.ntile(2).over(WinSpec)).withColumn("ntil3",F.ntile(3).over(WinSpec))
res3.show()


# lead -This function will return the value after the offset rows from DataFrame.
# lag-This function will return the value prior to offset rows from DataFrame.
##########################last########
# For last:This happens because default window frame is range between unbounded preceding and current row, so the last_value() never looks beyond current row unless you change the frame.
####If ORDER BY is not specified entire partition is used for a window frame. This applies only to functions that
# do not require ORDER BY clause. If ROWS/RANGE is not specified but ORDER BY is specified, RANGE UNBOUNDED
# PRECEDING AND CURRENT ROW is used as default for window frame. This applies only to functions that have can
# accept optional ROWS/RANGE specification. For example, ranking functions cannot accept ROWS/RANGE, therefore
# this window frame is not applied even though ORDER BY is present and ROWS/RANGE is not.

######Since last_value() has an optional order by clause, its default window frame ends at the current row. So
# with the default frame, no matter which partition or ordering you choose, last_value() returns the value from
# the current row.

WinSpec1=Window.partitionBy (F.col("job")).orderBy(F.col("sal").desc())
UnboundWinSpec=Window.partitionBy (F.col("job")).orderBy(F.col("sal").desc()).rowsBetween(Window.currentRow,Window.unboundedFollowing)
res4=df.withColumn("lag",F.lag(F.col("sal")).over(WinSpec1)).withColumn("lead",F.lead(F.col("sal")).over(WinSpec1))\
    .withColumn("First",F.first(F.col("sal")).over(WinSpec1)).withColumn("UnboundedLast",F.last(F.col("sal")).over(UnboundWinSpec))\
    .withColumn("DefaultLast",F.last(F.col("sal")).over(WinSpec1)).withColumn("saldiff",F.col("sal")-F.col("lead"))  #used when u want to find the difference between the pervious year /currentyear salary or to calculate hike
##rank+percent=percent rank (Skip rank when there is a duplicate)
res4.show()