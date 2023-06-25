# Databricks notebook source
from pyspark.sql import *

#we are reading data as df
df = spark.read.format("json").option("inferSchema", "true").load("dbfs:/FileStore/shared_uploads/annemchandrareddy123@gmail.com/2015_summary.json")
df.show(5)

df.printSchema()
spark.read.format("json").load("dbfs:/FileStore/shared_uploads/annemchandrareddy123@gmail.com/2015_summary.json").schema

# COMMAND ----------

from pyspark.sql.types import *
#As the DF have columns, lets define schema fir them



#lets define schema manually
manualSchema = StructType([StructField("DEST_COUNTRY_NAME",StringType(),True),
                          StructField("ORIGIN_COUNTRY_NAME",StringType(), True),
                          StructField("count", IntegerType(), False, metadata={"hello":"world"})])

manualschemadf=spark.read.format("json").schema(manualSchema).load("dbfs:/FileStore/shared_uploads/annemchandrareddy123@gmail.com/2015_summary.json")

manualschemadf.show()

manualschemadf.printSchema()
spark.read.format("json").schema(manualSchema).load("dbfs:/FileStore/shared_uploads/annemchandrareddy123@gmail.com/2015_summary.json").schema

# COMMAND ----------

from pyspark.sql.functions import *
df = spark.range(5).toDF("someCol")
df1 = df.withColumn("newCol",expr("someCol - 5"))
df1.show()

# COMMAND ----------

#We can access columns by defining schema, we to access columns progrmatically we can use .colums

spark.read.format("json").load("dbfs:/FileStore/shared_uploads/annemchandrareddy123@gmail.com/2015_summary.json").columns

# COMMAND ----------

df = spark.read.format("json").load("dbfs:/FileStore/shared_uploads/annemchandrareddy123@gmail.com/2015_summary.json")
df.first()

# COMMAND ----------

#creating rows and accessing them
from pyspark.sql import Row
myrow = Row("chandra",None,1,False)
print(myrow[0])
myrow

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Create a DataFrame
data = [("Alice", "Math", 90),
        ("Alice", "English", 85),
        ("Bob", "Math", 80),
        ("Bob", "English", 75)]

df = spark.createDataFrame(data, ["Name", "Subject", "Score"])

# Pivot the DataFrame
pivot_df = df.groupBy("Name").pivot("Subject").sum("Score")

pivot_df.show()


# COMMAND ----------

df = spark.read.format("json").load("dbfs:/FileStore/shared_uploads/annemchandrareddy123@gmail.com/2015_summary.json")
df.createOrReplaceTempView("flight_data")
df_sql = spark.sql("select * from flight_data")
df_sql.show(5)

from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, LongType

myManualSchma = StructType([StructField("name",StringType(),True),
                           StructField("Addresss",StringType(), True),
                            StructField("mobile",LongType(),False)])

data = [("chandra","3655 Southern Ave",6654455565),
       ("ram","None",6654455965)]
df = spark.createDataFrame(data,myManualSchma)
df.show()

# we can also create data using row object
data_row = [Row("chandra","3655 Southern Ave",6654455565),Row("ram","None",6654455965)]
df_row = spark.createDataFrame(data_row,myManualSchma)
df_row.show()

# COMMAND ----------

from pyspark.sql.functions import *
#How to use select and selectExpr methods in dataframes
df = spark.read.format("json").load("dbfs:/FileStore/shared_uploads/annemchandrareddy123@gmail.com/2015_summary.json")
df.show(5)

df.select("DEST_COUNTRY_NAME","ORIGIN_COUNTRY_NAME").show(5)

#As discussed in “Columns and Expressions”, you can refer to columns in a number of different ways
df.select(expr("DEST_COUNTRY_NAME"),col("DEST_COUNTRY_NAME"),column("DEST_COUNTRY_NAME")).show(5)




# COMMAND ----------

#As we’ve seen thus far, expr is the most flexible reference that we can use. It can refer to a plain column or a string manipulation of #a column. To illustrate, let’s change the column name, and then change it back by using the AS keyword and then the alias method on the #column:

from pyspark.sql.functions import *
#How to use select and selectExpr methods in dataframes
df = spark.read.format("json").load("dbfs:/FileStore/shared_uploads/annemchandrareddy123@gmail.com/2015_summary.json")

df.select(expr("DEST_COUNTRY_NAME as destination")).show(5)

#we use alias to rename column again
df.select(expr("DEST_COUNTRY_NAME as destination").alias("DEST_COUNTRY_NAME")).show(5)

#Because select followed by a series of expr is such a common pattern, Spark has a shorthand for doing this efficiently: selectExpr. This is probably the most convenient interface for everyday use

df.selectExpr("DEST_COUNTRY_NAME as destination").show(5)
df.selectExpr("DEST_COUNTRY_NAME as destination","DEST_COUNTRY_NAME").show(5)
#df.selectExpr("DEST_COUNTRY_NAME as destination").alias("DEST_COUNTRY_NAME").show(5)  This won't work



# COMMAND ----------

from pyspark.sql.functions import *

# let us add a new column to our df using selectExpr
df.selectExpr("*","(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as with_in_country").show(5)

#Perform few aggregations
df.selectExpr("avg(count) as avgcount","count(DISTINCT(DEST_COUNTRY_NAME)) as DISTINCT_COUNTRIES").show(5)

# COMMAND ----------

# DBTITLE 1,Converting to Spark Types (Literals)
from pyspark.sql.functions import lit
df.select(expr("*"),lit(1).alias("one")).show(2)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col

spark = SparkSession.builder.getOrCreate()

# Create a DataFrame
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Use literals in Spark operations
df_filtered = df.filter(col("Age") > lit(30))
df_transformed = df.withColumn("Incremented_Age", col("Age") + lit(5))

df_filtered.show()
df_transformed.show()


# COMMAND ----------

# DBTITLE 1,Adding Columns
df.withColumn("newnumber",lit(1)).show(2)

df.withColumn("withinCountry",expr("DEST_COUNTRY_NAME=ORIGIN_COUNTRY_NAME")).show(2)

#we can also rename a column in this way, actually we are renaming and adding that clumn to df
df.withColumn("destination",expr("DEST_COUNTRY_NAME")).show(2)

# COMMAND ----------

#Renaming Columns
#Although we can rename a column in the manner that we just described, another alternative is to use the withColumnRenamed method. This #will rename the column with the name of the string in the first argument to the string in the second argument:

df.withColumnRenamed("DEST_COUNTRY_NAME","destination").show(2)

# COMMAND ----------

# DBTITLE 1,Need to look again: Reserved Characters and Keywords
dfWithLongColName = df.withColumn(
    "This Long Column-Name",
    expr("ORIGIN_COUNTRY_NAME"))
dfWithLongColName.show(2)

dfWithLongColName.selectExpr(
    "`This Long Column-Name`",
    "`This Long Column-Name` as `new col`")\
  .show(2)

dfWithLongColName.select(expr("`This Long Column-Name`")).columns

# COMMAND ----------

# DBTITLE 1,Removing Columns
dfWithLongColName.drop("ORIGIN_COUNTRY_NAME").show(2)
dfWithLongColName.drop("ORIGIN_COUNTRY_NAME","This Long Column-Name").show(2)

# COMMAND ----------

# DBTITLE 1,Changing a Column’s Type (cast)
df_change = df.withColumn("count2", col("count").cast("short"))
df_change.printSchema()
df.select(col("count").cast("short")).schema

# COMMAND ----------

# DBTITLE 1,Filtering Rows
#filter

df.filter(expr("count >100")).show(5)
df.where(expr("count>100")).show(5)

#we can also do multiple
df.where(expr("count>2100")).where(expr('DEST_COUNTRY_NAME = "United States"')).show(5)



# COMMAND ----------

# DBTITLE 1,Getting Unique Rows
print(df.select("DEST_COUNTRY_NAME","ORIGIN_COUNTRY_NAME").distinct().count())
df.select("ORIGIN_COUNTRY_NAME").distinct().count()

# COMMAND ----------

# DBTITLE 1,Sample
# Sometimes, you might just want to sample some random records from your DataFrame. You can do this by using the sample method on a DataFrame, which makes it possible for you to specify a fraction of rows to extract from a DataFrame and whether you’d like to sample with or without replacement:
seed = 5
withReplacement = False
fraction = 0.5
print(df.sample(withReplacement, fraction, seed).count())
df.sample(withReplacement, fraction, seed).show()

# COMMAND ----------

# DBTITLE 1,Random Split of DF

SPLIT_DF =  df.randomSplit([0.25,0.75], seed) 
print(SPLIT_DF[0].count())
print(SPLIT_DF[1].count())
print(SPLIT_DF[0].count()<SPLIT_DF[1].count())
SPLIT_DF[0].show()


# COMMAND ----------

# DBTITLE 1,Concatenating and Appending Rows (Union)
#As you learned in the previous section, DataFrames are immutable. This means users cannot append to DataFrames because that would be changing it. To append to a DataFrame, you must union the original DataFrame along with the new DataFrame. This just concatenates the two DataFramess. To union two DataFrames, you must be sure that they have the same schema and number of columns; otherwise, the union will fail.

from pyspark.sql import Row
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

schema =df.schema
newRows = [Row("India","United States",104),Row("India","Newzealand",3)]

paralleliz_newrows = spark.sparkContext.parallelize(newRows)
newDF = spark.createDataFrame(paralleliz_newrows,schema)

df.union(newDF).show()

df.union(newDF).where(expr("count = 3 or count = 104")).where(expr('DEST_COUNTRY_NAME != "United States"')).show()


# COMMAND ----------

# DBTITLE 1,Sorting Rows
#When we sort the values in a DataFrame, we always want to sort with either the largest or smallest values at the top of a DataFrame. There are two equivalent operations to do this sort and orderBy that work the exact same way. They accept both column expressions and strings as well as multiple columns. The default is to sort in ascending order:

df.sort("count").show(5)
df.orderBy("count","DEST_COUNTRY_NAME").show(5)

# we can also specify the sort direction
df.orderBy(expr("count desc")).show(5)
df.orderBy(desc("DEST_COUNTRY_NAME"),asc("count")).show(5)

from pyspark.sql import SparkSession
from pyspark.sql.functions import asc_nulls_first, desc_nulls_last

spark = SparkSession.builder.getOrCreate()

# Create a DataFrame
data = [(1, "Alice"),
        (2, "Bob"),
        (3, None),
        (4, "Charlie"),
        (5, None)]

df1 = spark.createDataFrame(data, ["ID", "Name"])

# Order the DataFrame with null values first
df_nulls_first = df1.orderBy(asc_nulls_first("Name"))

# Order the DataFrame with null values last
df_nulls_last = df1.orderBy(desc_nulls_last("Name"))

df_nulls_first.show()
df_nulls_last.show()

# For optimization purposes, it’s sometimes advisable to sort within each partition before another set of transformations. You can use the sortWithinPartitions method to do this
df.sortWithinPartitions("count").show()

# COMMAND ----------

# DBTITLE 1,Limit
df.orderBy(expr("count desc")).limit(6).show()

# COMMAND ----------

# DBTITLE 1,Repartition and Coalesce
#Another important optimization opportunity is to partition the data according to some frequently filtered columns, which control the physical layout of data across the cluster including the partitioning scheme and the number of partitions.

#Repartition will incur a full shuffle of the data, regardless of whether one is necessary. This means that you should typically only repartition when the future number of partitions is greater than your current number of partitions or when you are looking to partition by a set of columns

print(df.rdd.getNumPartitions())
df.repartition(5)

df1 = df.repartition(exp("DEST_COUNTRY_NAME"))
print(df1.rdd.getNumPartitions())

df1 = df.repartition(5,exp("DEST_COUNTRY_NAME"))
print(df1.rdd.getNumPartitions())

df2 = df.repartition(5, col("DEST_COUNTRY_NAME")).coalesce(2)
print(df2.rdd.getNumPartitions())

# COMMAND ----------

#Collect data to driver
collectDF = df.limit(10)
print(collectDF.take(5)) # take works with an Integer count
collectDF.show() # this prints it out nicely
collectDF.show(5, False)
print(collectDF.collect())

for i in collectDF.toLocalIterator():
    print(i)
    
#WARNING
#Any collection of data to the driver can be a very expensive operation! If you have a large dataset and call collect, you can crash the driver. If you use toLocalIterator and have very large partitions, you can easily crash the driver node and lose the state of your application. This is also expensive because we can operate on a one-by-one basis, instead of running computation in parallel.