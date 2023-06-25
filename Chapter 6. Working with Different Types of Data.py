# Databricks notebook source
df = spark.read.format("csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load("dbfs:/FileStore/shared_uploads/annemchandrareddy123@gmail.com/2010_12_01.csv")
df.printSchema()
df.createOrReplaceTempView("dfTable")

spark.sql("select * from dfTable ").show(5)

df.select("InvoiceNo","StockCode").show(5)

# COMMAND ----------

# DBTITLE 1,Converting to Spark Types
#One thing you’ll see us do throughout this chapter is convert native types to Spark types. We do this by using the first function that we introduce here, the lit function. This function converts a type in another language to its correspnding Spark representation. 
from pyspark.sql.functions import lit
df.select(lit(5),lit("five"),lit(5.0)).schema

#The lit function in Spark is used to convert a literal value from another programming language (such as Scala or Python) to its corresponding Spark representation. It creates a column with a constant value that can be used in DataFrame operations

# COMMAND ----------

# DBTITLE 1,The lit function in Spark is used to convert a literal value from another programming language (such as Scala or Python) to its corresponding Spark representation. It creates a column with a constant value that can be used in DataFrame operations
from pyspark.sql.functions import *
# we can use == or =
df1 = df.where(expr("InvoiceNo == 536365")).select("InvoiceNo","StockCode","Description")
df1.show(5,False) # If we set True the values in the columns will be truncated if they exceed certain length

df2_notequal = df.where("InvoiceNo <> 536365")
df2_notequal.show(5, False)

#We mentioned that you can specify Boolean expressions with multiple parts when you use and or or. In Spark, you should always chain together and filters as a sequential filter.

#The reason for this is that even if Boolean statements are expressed serially (one after the other), Spark will flatten all of these filters into one statement and perform the filter at the same time, creating the and statement for us. Although you can specify your statements explicitly by using and if you like, they’re often easier to understand and to read if you specify them serially. or statements need to be specified in the same statement:

priceFilter = col("UnitPrice") > 600
descripFilter = instr("Description","POSTAGE")>=1
df.where(df.StockCode.isin("DOT")).where(priceFilter|descripFilter).show()

# COMMAND ----------

#Boolean expressions are not just reserved to filters. To filter a DataFrame, you can also just specify a Boolean column:
from pyspark.sql.functions import *


DOTcodeFilter = col("StockCode") == "DOT"
priceFilter   = col("UnitPrice") > 100
descriptionFilter = instr(col("Description"),"POSTAGE") >=1
df.withColumn("isexpensive",DOTcodeFilter|(priceFilter|descriptionFilter))\
  .where("isexpensive")\
  .select("unitPrice","StockCode","Description","isexpensive").show(10)

# COMMAND ----------

#Notice how we did not need to specify our filter as an expression and how we could use a column name without any extra work.

#If you’re coming from a SQL background, all of these statements should seem quite familiar. Indeed, all of them can be expressed as a where clause. In fact, it’s often easier to just express filters as SQL statements than using the programmatic DataFrame interface and Spark SQL allows us to do this without paying any performance penalty. For example, the following two statements are equivalent:

from pyspark.sql.functions import expr
df.withColumn("isExpensice",expr("NOT unitPrice >250")).where("isExpensice").select(expr("*")).show(100, False)

# COMMAND ----------

#WARNING
#One “gotcha” that can come up is if you’re working with null data when creating Boolean expressions. If there is a null in your data, you’ll need to treat things a bit differently. Here’s how you can ensure that you perform a null-safe equivalence test:

df.where(col("Description").eqNullSafe("hello")).select("Description").show()


# COMMAND ----------

# DBTITLE 1,Working with Numbers
#When working with big data, the second most common task you will do after filtering things is counting things. For the most part, we simply need to express our computation, and that should be valid assuming that we’re working with numerical data types.

#To fabricate a contrived example, let’s imagine that we found out that we mis-recorded the quantity in our retail dataset and the true quantity is equal to (the current quantity * the unit price)2 + 5. This will introduce our first numerical function as well as the pow function that raises a column to the expressed power:

from pyspark.sql.functions import expr, pow
fabricated_quantity = pow((col("Quantity")*col("UnitPrice")), 2)+5
df.select("CustomerID",fabricated_quantity.alias("realquantity")).show()


# Naturally we can add and subtract as necessary, as well. In fact, we can do all of this as a SQL expression, as well:
df.selectExpr("CustomerID","POWER((Quantity*UnitPrice),2)+5 as RealQuantity").show()


#Another common numerical task is rounding. If you’d like to just round to a whole number, oftentimes you can cast the value to an integer and that will work just fine. However, Spark also has more detailed functions for performing this explicitly and to a certain level of precision. In the following example, we round to one decimal place:

df.select(round(lit("2.5")),bround(lit("2.5"))).show()
df.select(round(col("UnitPrice"),1).alias("roundUnitPrice"),col("UnitPrice")).show()

#Another numerical task is to compute the correlation of two columns. For example, we can see the Pearson correlation coefficient for two columns to see if cheaper things are typically bought in greater quantities. We can do this through a function as well as through the DataFrame statistic methods:

from pyspark.sql.functions import corr
print(df.stat.corr("Quantity","UnitPrice")) 
df.select(corr("Quantity","UnitPrice")).show()
#In the given example, the correlation between the "Quantity" and "UnitPrice" columns is calculated, resulting in a correlation coefficient of approximately -0.041. This negative correlation coefficient suggests that there is a weak negative relationship between the "Quantity" and "UnitPrice" variables.
#Interpreting the correlation coefficient, we can say that as the quantity of items purchased increases, the unit price tends to decrease slightly. However, since the correlation coefficient is close to zero and negative, the relationship is weak, indicating that the variables are not strongly associated in a negative direction.

#Another common task is to compute summary statistics for a column or set of columns. We can use the describe method to achieve exactly this. This will take all numeric columns and calculate the count, mean, standard deviation, min, and max. You should use this primarily for viewing in the console because the schema might change in the future:

df.describe().show()

#If you need these exact numbers, you can also perform this as an aggregation yourself by importing the functions and applying them to the columns that you need:

from pyspark.sql.functions import count, mean, stddev_pop, min, max


#df.select("CustomerID", agg(mean("UnitPrice").alias("mean_unitprice")).groupBy("CustomerID").show(5) error

df.groupBy("CustomerID").agg(mean("UnitPrice").alias("mean_unitprice")).show(5)



# COMMAND ----------

# DBTITLE 1,Part2
#There are a number of statistical functions available in the StatFunctions Package (accessible using stat as we see in the code block below). These are DataFrame methods that you can use to calculate a variety of different things. For instance, you can calculate either exact or approximate quantiles of your data using the approxQuantile method:

colName = "UnitPrice"
quantileProbs = [0.5]
relError = 0.05
print(df.stat.approxQuantile(colName,quantileProbs,relError))

#You also can use this to see a cross-tabulation or frequent item pairs (be careful, this output will be large and is omitted for this reason):

#df.stat.crosstab("StockCode","Quantity").show()
df.stat.freqItems(["StockCode"]).show(10,False)

#As a last note, we can also add a unique ID to each row by using the function monotonically_increasing_id. This function generates a unique value for each row, starting with 0:

df.select(monotonically_increasing_id(),expr("*")).show()


# COMMAND ----------

# DBTITLE 1,Working with Strings
#String manipulation shows up in nearly every data flow, and it’s worth explaining what you can do with strings. You might be manipulating log files performing regular expression extraction or substitution, or checking for simple string existence, or making all strings uppercase or lowercase.
#Let’s begin with the last task because it’s the most straightforward. The initcap function will capitalize every word in a given string when that word is separated from another by a space.
from pyspark.sql.functions import initcap, lower, upper

df.select(initcap("Description")).show(10, False)

df.select("Description", lower("Description"),upper("Description"),upper(lower("Description"))).show(5, False)

#Another trivial task is adding or removing spaces around a string. You can do this by using lpad, ltrim, rpad and rtrim, trim
df.select(ltrim(lit("   Hello  ")).alias("lt"),rtrim(lit("   Hello  ")).alias("rtrim"),trim(lit("   Hello  ")).alias("trim"), lpad(lit("Ramsethu"),12,"*").alias("lpad"),rpad(lit("Ramsethu"),12,"*").alias("rpad")).show(4,False)

#Note that if lpad or rpad takes a number less than the length of the string, it will always remove values from the right side of the string.

# COMMAND ----------

# DBTITLE 0,Regular Expressions
#Probably one of the most frequently performed tasks is searching for the existence of one string in another or replacing all mentions of a string with another value. This is often done with a tool called regular expressions that exists in many programming languages. Regular expressions give the user an ability to specify a set of rules to use to either extract values from a string or replace them with some other values.
#Spark takes advantage of the complete power of Java regular expressions. The Java regular expression syntax departs slightly from other programming languages, so it is worth reviewing before putting anything into production.
#There are two key functions in Spark that you’ll need in order to perform regular expression tasks: regexp_extract and regexp_replace. These functions extract values and replace values, respectively.

from pyspark.sql.functions import regexp_replace

reg_exp = "BLACK|WHITE|RED|GREEN|BLUE"

df.select(regexp_replace("Description",reg_exp,"COLOR").alias("clean_color"), "Description").show(5, False)

#Another task might be to replace given characters with other characters. Building this as a regular expression could be tedious, so Spark also provides the translate function to replace these values. This is done at the character level and will replace all instances of a character with the indexed character in the replacement string:

df.select("Description",translate("Description","LEET","1337").alias("character_change")).show(5, False)

#We can also perform something similar, like pulling out the first mentioned color:
from pyspark.sql.functions import regexp_extract

extract_str = "(BLACK|WHITE|RED|GREEN|BLUE)"

df.select("Description",regexp_extract("Description",extract_str, 1).alias("extracted_value")).show(5,False)

#Sometimes, rather than extracting values, we simply want to check for their existence. In Python and SQL, we can use the instr function. This will return a Boolean declaring whether the value you specify is in the column’s string

from pyspark.sql.functions import instr
colorBlack = instr("Description", "BLACK") >=1
colorWhite = instr("Description","WHITE") >=1
df.withColumn("hasaColor",(colorBlack|colorWhite)).where("hasaColor").select("hasaColor","Description").show(5, False)







# COMMAND ----------

#Let’s work through this in a more rigorous way and take advantage of Spark’s ability to accept a dynamic number of arguments. 
#We can also do this quite easily in Python. In this case, we’re going to use a different function, locate, that returns the integer location (1 based location). We then convert that to a Boolean before using it as the same basic feature

from pyspark.sql.functions import expr, locate

simpleColors = ["black", "white", "red", "green", "blue"]

def color_locator(column, color_string, c):
    return locate(color_string.upper(), column)\
        .cast("boolean")\
        .alias("is_" + c)

selectedColumns = [color_locator(df.Description, c, c) for c in simpleColors]
print(selectedColumns)
selectedColumns.append(expr("*"))  # has to be a Column type

df.select(*selectedColumns).where(expr("is_white OR is_red"))\
    .select("Description").show(3, False)


# COMMAND ----------

# DBTITLE 1,Working with Dates and Timestamps
#Although Spark will do read dates or times on a best-effort basis. However, sometimes there will be no getting around working with strangely formatted dates and times. The key to understanding the transformations that you are going to need to apply is to ensure that you know exactly what type and format you have at each given step of the way. Another common “gotcha” is that Spark’s TimestampType class supports only second-level precision, which means that if you’re going to be working with milliseconds or microseconds, you’ll need to work around this problem by potentially operating on them as longs. Any more precision when coercing to a TimestampType will be removed.
#Spark can be a bit particular about what format you have at any given point in time. It’s important to be explicit when parsing or converting to ensure that there are no issues in doing so. At the end of the day, Spark is working with Java dates and timestamps and therefore conforms to those standards.

#Let’s begin with the basics and get the current date and the current timestamps:

from pyspark.sql.functions import current_date, current_timestamp
dateDF = spark.range(10)\
         .withColumn("date",current_date())\
         .withColumn("now",current_timestamp())
dateDF.createOrReplaceTempView("dates")

spark.sql("select * from dates").show()
dateDF.printSchema()

#Now that we have a simple DataFrame to work with, let’s add and subtract five days from today. These functions take a column and then the number of days to either add or subtract as the arguments

from pyspark.sql.functions import date_add, date_sub
dateDF.withColumn("datesub", date_sub("date",5)).withColumn("dateadd", date_add("date",5)).select("*").show(5)
#dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)

# COMMAND ----------

#Another common task is to take a look at the difference between two dates. We can do this with the datediff function that will return the number of days in between two dates. Most often we just care about the days, and because the number of days varies from month to month, there also exists a function, months_between, that gives you the number of months between two dates:

from pyspark.sql.functions import datediff, months_between, to_date
dateDF.withColumn("weekago", date_sub("date",7)).select(datediff("weekago","date")).show()
from pyspark.sql.functions import to_date, lit
dateDF.select(to_date(lit("2016-01-01")).alias("start"),to_date(lit("2019-01-01")).alias("end")).select(months_between("start","end")).show()

#Notice that we introduced a new function: the to_date function. The to_date function allows you to convert a string to a date, optionally with a specified format. We specify our format in the Java SimpleDateFormat which will be important to reference if you use this function:
# in Python
from pyspark.sql.functions import to_date, lit, col
spark.range(5).withColumn("date", lit("2017-01-01"))\
  .select(to_date(col("date"))).show(1)

dateDF.select(to_date(lit("2016-20-12")),to_date(lit("2017-12-11"))).show(1)


#We find this to be an especially tricky situation for bugs because some dates might match the correct format, whereas others do not. In the previous example, notice how the second date appears as Decembers 11th instead of the correct day, November 12th. Spark doesn’t throw an error because it cannot know whether the days are mixed up or that specific row is incorrect.

#Let’s fix this pipeline, step by step, and come up with a robust way to avoid these issues entirely. The first step is to remember that we need to specify our date format according to the Java SimpleDateFormat standard.

date_format = "yyyy-dd-MM"
cleandatDF = spark.range(1).withColumn("date1",to_date(lit("2015-01-05"),date_format)).withColumn("date2",to_date(lit("2015-20-05"),date_format))
cleandatDF.select("date1","date2").show()

from pyspark.sql.functions import to_timestamp
cleandatDF.select(to_timestamp(col("date1"), date_format)).show()

# COMMAND ----------

from pyspark.sql.functions import to_date, lit,to_timestamp
spark.range(15).withColumn("date",to_date(lit("2015-12-11"))).show(5)
#spark can not parse the date becuase for month field we gave 14 which is not valid. spark will not throw error just returns NULL
spark.range(15).withColumn("date",to_date(lit("2015-20-11"))).show(5)

#We find this to be an especially tricky situation for bugs because some dates might match the correct format, whereas others do not. In the previous example, notice how the second date appears as Decembers 11th instead of the correct day, November 12th. Spark doesn’t throw an error because it cannot know whether the days are mixed up or that specific row is incorrect

#Let’s fix this pipeline, step by step, and come up with a robust way to avoid these issues entirely. The first step is to remember that we need to specify our date format according to the Java SimpleDateFormat standard.

#We will use two functions to fix this: to_date and to_timestamp. The former optionally expects a format, whereas the latter requires one:

date_format = "yyyy-dd-MM"
spark.range(15).withColumn("date",to_date(lit("2015-20-11"),date_format)).show(5)
clean_date = spark.range(15).withColumn("timestamp",to_timestamp(lit("2015-20-11"),date_format))
clean_date.show(5)
clean_date.filter(to_date(lit("2015-12-12"))>col("timestamp")).show()

# COMMAND ----------

# DBTITLE 1,Working with Nulls in Data
df.na.drop()
df.na.drop("any")
df.na.drop("all")
#Specifying "any" as an argument drops a row if any of the values are null. Using “all” drops the row only if all values are null or NaN for that row:
#We can also apply this to certain sets of columns by passing in an array of columns:
df.na.drop("all", subset=["StockCode", "InvoiceNo"])

#fill
#Using the fill function, you can fill one or more columns with a set of values. This can be done by specifying a map—that is a particular value and a set of columns.
df.na.fill("All Null values become this string").show(5)
df.na.fill("all", subset=["StockCode", "InvoiceNo"])
#If we want fill Nulls for different columns with diffwerent numbers
fill_cols_vals = {"StockCode": 5, "Description" : "No Value"}
df.na.fill(fill_cols_vals)

#replace
#In addition to replacing null values like we did with drop and fill, there are more flexible options that you can use with more than just null values. Probably the most common use case is to replace all values in a certain column according to their current value. The only requirement is that this value be the same type as the original value:

df.na.replace([""], ["UNKNOWN"], "Description")

#Ordering
#As we discussed in Chapter 5, you can use asc_nulls_first, desc_nulls_first, asc_nulls_last, or desc_nulls_last to specify where you would like your null values to appear in an ordered DataFrame.

# COMMAND ----------

# DBTITLE 1,Structs
#Complex types can help you organize and structure your data in ways that make more sense for the problem that you are hoping to solve. There are three kinds of complex types: structs, arrays, and maps.

#You can think of structs as DataFrames within DataFrames. A worked example will illustrate this more clearly. We can create a struct by wrapping a set of columns in parenthesis in a query:

df.selectExpr("(Description, InvoiceNo) as complex", "*").show(5)
df.selectExpr("struct(Description, InvoiceNo) as complex", "*").show(5)

complex_DF = df.select(struct("Description","InvoiceNo").alias("complex"))
complex_DF.show(5,False)
complex_DF.select("complex.Description").show(5, False)
complex_DF.createOrReplaceTempView("complex_table")
spark.sql('select complex.Description,* from complex_table').show(5,False)

# COMMAND ----------

# DBTITLE 1,Arrays
#To define arrays, let’s work through a use case. With our current data, our objective is to take every single word in our Description column and convert that into a row in our DataFrame

df.select(split("Description"," ")).show(5, False)