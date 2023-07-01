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

# COMMAND ----------

# DBTITLE 1,Skewness and kurtosis

from pyspark.sql.functions import skewness, kurtosis
df.select(skewness("Quantity"), kurtosis("Quantity")).show()

# COMMAND ----------

from pyspark.sql.functions import covar_pop,covar_samp,corr

df.select(corr("InvoiceNo","Quantity"),covar_samp("InvoiceNo","Quantity"),covar_pop("InvoiceNo","Quantity")).show()

# COMMAND ----------

# DBTITLE 1,Aggregating to Complex Types
#In Spark, you can perform aggregations not just of numerical values using formulas, you can also perform them on complex types. For example, we can collect a list of values present in a given column or only the unique values by collecting to a set.


from pyspark.sql.functions import collect_set, collect_list
df.agg(collect_set("Country"), collect_list("Country")).show()

#SELECT collect_set(Country), collect_set(Country) FROM dfTable

# COMMAND ----------

# DBTITLE 1,Grouping with Expressions
#As we saw earlier, counting is a bit of a special case because it exists as a method. For this, usually we prefer to use the count function. Rather than passing that function as an expression into a select statement, we specify it as within agg. This makes it possible for you to pass-in arbitrary expressions that just need to have some aggregation specified. You can even do things like alias a column after transforming it for later use in your data flow:

from pyspark.sql.functions import count

df.groupBy("InvoiceNo").agg(
    count("Quantity").alias("quan"),
    expr("count(Quantity)")).show()

# COMMAND ----------

# DBTITLE 1,Grouping with Maps
#Sometimes, it can be easier to specify your transformations as a series of Maps for which the key is the column, and the value is the aggregation function (as a string) that you would like to perform. You can reuse multiple column names if you specify them inline, as well:

df.groupBy("InvoiceNo").agg(expr("avg(Quantity)"),expr("stddev_pop(Quantity)"))\
  .show()

# COMMAND ----------

# DBTITLE 1,Window Functions

#You can also use window functions to carry out some unique aggregations by either computing some aggregation on a specific “window” of data, which you define by using a reference to the current data. This window specification determines which rows will be passed in to this function. Now this is a bit abstract and probably similar to a standard group-by, so let’s differentiate them a bit more.

#A group-by takes data, and every row can go only into one grouping. A window function calculates a return value for every input row of a table based on a group of rows, called a frame. Each row can fall into one or more frames. A common use case is to take a look at a rolling average of some value for which each row represents one day. If you were to do this, each row would end up in seven different frames. We cover defining frames a little later, but for your reference, Spark supports three kinds of window functions: ranking functions, analytic functions, and aggregate functions.

from pyspark.sql.functions import col, to_date
dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))
dfWithDate.createOrReplaceTempView("dfWithDate")

from pyspark.sql.window import Window
from pyspark.sql.functions import desc
windowSpec = Window\
  .partitionBy("CustomerId", "date")\
  .orderBy(desc("Quantity"))\
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)

#e want to use an aggregation function to learn more about each specific customer. An example might be establishing the maximum purchase quantity over all time. To answer this, we use the same aggregation functions that we saw earlier by passing a column name or expression. In addition, we indicate the window specification that defines to which frames of data this function will apply:
from pyspark.sql.functions import max
maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)

#You will notice that this returns a column (or expressions). We can now use this in a DataFrame select statement. Before doing so, though, we will create the purchase quantity rank. To do that we use the dense_rank function to determine which date had the maximum purchase quantity for every customer. We use dense_rank as opposed to rank to avoid gaps in the ranking sequence when there are tied values (or in our case, duplicate rows):
from pyspark.sql.functions import dense_rank, rank
purchaseDenseRank = dense_rank().over(windowSpec)
purchaseRank = rank().over(windowSpec)

from pyspark.sql.functions import col

dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId")\
  .select(
    col("CustomerId"),
    col("date"),
    col("Quantity"),
    purchaseRank.alias("quantityRank"),
    purchaseDenseRank.alias("quantityDenseRank"),
    maxPurchaseQuantity.alias("maxPurchaseQuantity")).show()


# COMMAND ----------

# DBTITLE 1,Grouping Sets
#Thus far in this chapter, we’ve seen simple group-by expressions that we can use to aggregate on a set of columns with the values in those columns. However, sometimes we want something a bit more complete—an aggregation across multiple groups. We achieve this by using grouping sets. Grouping sets are a low-level tool for combining sets of aggregations together. They give you the ability to create arbitrary aggregation in their group-by statements.

dfNoNull = dfWithDate.drop()
dfNoNull.createOrReplaceTempView("dfNoNull")

spark.sql("SELECT CustomerId, stockCode, sum(Quantity) FROM dfNoNull GROUP BY customerId, stockCode ORDER BY CustomerId DESC, stockCode DESC").show()

spark.sql("SELECT CustomerId, stockCode, sum(Quantity) FROM dfNoNull GROUP BY customerId, stockCode GROUPING SETS((customerId, stockCode)) ORDER BY CustomerId DESC, stockCode DESC").show()

# COMMAND ----------



# COMMAND ----------


