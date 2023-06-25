# Databricks notebook source
#Aggregating is the act of collecting something together and is a cornerstone of big data analytics.In an aggregation, you will specify a key or grouping and an aggregation function that specifies how you should transform one or more columns. This function must produce one result for each group, given multiple input values. 

#The simplest grouping is to just summarize a complete DataFrame by performing an aggregation in a select statement.

#A “group by” allows you to specify one or more keys as well as one or more aggregation functions to transform the value columns.

#A “window” gives you the ability to specify one or more keys as well as one or more aggregation functions to transform the value columns. However, the rows input to the function are somehow related to the current row.

#A “grouping set,” which you can use to aggregate at multiple different levels. Grouping sets are available as a primitive in SQL and via rollups and cubes in DataFrames.

# “rollup” makes it possible for you to specify one or more keys as well as one or more aggregation functions to transform the value columns, which will be summarized hierarchically.

#A “cube” allows you to specify one or more keys as well as one or more aggregation functions to transform the value columns, which will be summarized across all combinations of columns.

# COMMAND ----------

df = spark.read.format("csv").option("header","true").option("inferschema","true").load("dbfs:/FileStore/shared_uploads/annemchandrareddy123@gmail.com/*.csv").coalesce(5)
df.cache()
df.createOrReplaceTempView("dfTable")
df.show(10)

# COMMAND ----------

# DBTITLE 1,Count
#As mentioned, basic aggregations apply to an entire DataFrame. The simplest example is the count method:
print(df.count())

#If you’ve been reading this book chapter by chapter, you know that count is actually an action as opposed to a transformation, and so it returns immediately. You can use count to get an idea of the total size of your dataset but another common pattern is to use it to cache an entire DataFrame in memory, just like we did in this example.

#Now, this method is a bit of an outlier because it exists as a method (in this case) as opposed to a function and is eagerly evaluated instead of a lazy transformation. In the next section, we will see count used as a lazy function, as well.

#The first function worth going over is count, except in this example it will perform as a transformation instead of an action.
from pyspark.sql.functions import count
df.select(count("StockCode")).show()

# COMMAND ----------

# DBTITLE 1,#WARNING
#There are a number of gotchas when it comes to null values and counting. For instance, when performing a count(*), Spark will count null values (including rows containing all nulls). However, when counting an individual column, Spark will not count the null values.

# COMMAND ----------

# DBTITLE 1,countDistinct
from pyspark.sql.functions import countDistinct
df.select(countDistinct("StockCode")).show()

# COMMAND ----------

# DBTITLE 1,approx_count_distinct
#approx_count_distinct
#Often, we find ourselves working with large datasets and the exact distinct count is irrelevant. There are times when an approximation to a certain degree of accuracy will work just fine, and for that, you can use the approx_count_distinct function:
from pyspark.sql.functions import approx_count_distinct
df.select(approx_count_distinct("StockCode", 0.1)).show()#0.01

#You will notice that approx_count_distinct took another parameter with which you can specify the maximum estimation error allowed. In this case, we specified a rather large error and thus receive an answer that is quite far off but does complete more quickly than countDistinct. You will see much greater performance gains with larger datasets.

# COMMAND ----------

# DBTITLE 1,first and last || min and max||sum and sumDistinct
from pyspark.sql.functions import first, last, min, max, sum, sumDistinct, avg, mean, expr
df.select(first("StockCode"),last("StockCode")).show()

df.select(min("Quantity"), max("Quantity")).show()

df.select(sum("Quantity")).show()

df.select(sumDistinct("Quantity")).show()

# COMMAND ----------

df.select(
    count("Quantity").alias("Total_transactions"),
    sum("Quantity").alias("Total_purchases"),
    avg("Quantity").alias("avg_of_purchases"),
    expr("mean(Quantity)").alias("mean_of_purchases")
).selectExpr("Total_transactions","Total_purchases","avg_of_purchases","mean_of_purchases").show()


# COMMAND ----------

from pyspark.sql.functions import var_pop, stddev_pop
from pyspark.sql.functions import var_samp, stddev_samp
df.select(var_pop("Quantity"), var_samp("Quantity"),
  stddev_pop("Quantity"), stddev_samp("Quantity")).show()