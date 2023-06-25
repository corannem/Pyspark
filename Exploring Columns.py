# Databricks notebook source
# MAGIC %fs ls /databricks-datasets/

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/airlines

# COMMAND ----------

# MAGIC %fs head /databricks-datasets/airlines/part-00000

# COMMAND ----------

airlineDF = spark.read\
             .format("csv")\
             .option("header", "true")\
             .option("inferschema", "true")\
             .option("samplingratio", "0.0001")\
             .load("/databricks-datasets/airlines/part-00000")


# COMMAND ----------

airlineDF.select("origin","Dest","Distance").show(10)

# COMMAND ----------

from pyspark.sql.functions import *
airlineDF.select(column("origin"), col("Dest"), airlineDF.Distance).show()

# COMMAND ----------

airlineDF.select(column("origin"), col("Dest"), "Distance","year").show()

# COMMAND ----------

#Column Expressions using Strings or SQL
airlineDF.select("Origin","Dest","Distance","Year","Month","DayofMonth").show(10)

# COMMAND ----------

#Creating column expression
airlineDF.select("Origin","Dest","Distance", "to_date(concat(Year, Month, DayofMonth), 'yyyyMMdd') as FlighDate").show(10)

# COMMAND ----------

airlineDF.select("Origin","Dest","Distance", expr("to_date(concat(Year, Month, DayofMonth), 'yyyyMMdd') as FlighDate")).show(10)

# COMMAND ----------

#Column object Expressions
airlineDF.select("Origin","Dest","Distance", to_date(concat("Year", "Month", "DayofMonth"), 'yyyyMMdd').alias ("FlighDate")).show(10)

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/

# COMMAND ----------

airlineDF.filter(airlineDF.DayofMonth > '16').show()