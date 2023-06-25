# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

def to_date_df(df, fmt, fld):
    return df.withColumn(fld, to_date(col(fld), fmt))

# COMMAND ----------

my_schema = StructType([
    StructField("ID", StringType()),
    StructField("EventDate", StringType())
])

my_rows = [Row("123", "04/05/2023"),Row("124", "4/5/2023"), Row("125", "4/5/2023"), Row("126", "04/05/2023")]
#my_rdd = spark.sparkContext.parallelize(my_rows,2)
my_df = spark.createDataFrame(my_rows, my_schema)


# COMMAND ----------

my_df.printSchema()
my_df.show()
new_df = to_date_df(my_df,"M/d/y", "EventDate")

new_df.printSchema()
new_df.show()

# COMMAND ----------

from pyspark.sql.functions import col
df = spark.range(100).toDF("number")
#df.select(df["number"]+10).show()
df.select(col("number")+10).show()
#df.select("number"+10).show() This will not work df.select("number"+10).show()

# COMMAND ----------

d = spark.range(2).collect()
print(d)

df = spark.range(2).toDF("num")

df.show()

# COMMAND ----------

#How to instanciate a typr to a cloumn
from pyspark.sql.types import *
b = ByteType()
print(type(b))

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, IntegerType

spark = SparkSession.builder.getOrCreate()

# Define a schema with an ArrayType column
schema = ArrayType(IntegerType(), containsNull=False)

# Create a DataFrame with a column of type ArrayType
data = [(1, [1, 2, 3]), (2, None), (3, [4, 5])]
df = spark.createDataFrame(data, ["id", "numbers"], schema)
df.show()
for id, numbers in data:
    if id == 1:
        for a in numbers:
            print(a)

df.printSchema()


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import MapType, StringType, IntegerType

spark = SparkSession.builder.getOrCreate()

# Define a schema with a MapType column
schema = MapType(StringType(), IntegerType(), valueContainsNull=False)

# Create a DataFrame with a column of type MapType
data = [(1, {"Alice": 25, "Bob": 30}), (2, None), (3, {"Charlie": 35})]
df = spark.createDataFrame(data, ["id", "age_map"], schema)
df.show()
df.printSchema()


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.getOrCreate()

# Define the schema with multiple StructFields
schema = StructType([
    StructField("name", StringType(), nullable=False),
    StructField("age", IntegerType(), nullable=True),
    StructField("city", StringType(), nullable=True)
])

# Create a DataFrame with the defined schema
data = [("Alice", 25, "New York"), ("Bob", None, "San Francisco")]
df = spark.createDataFrame(data, schema)
df.show()

df.printSchema()
