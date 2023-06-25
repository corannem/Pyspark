# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, column, desc, col

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master('local[3]') \
        .appName("Structured Streaming") \
        .getOrCreate()

    # lets irst read it as DF and analyze
    spark.conf.set("spark.sql.shuffle.partitions", "5")
    Structured_DF = spark.read \
        .format("csv") \
        .option("inferschema", "true") \
        .option("header", "true") \
        .load("dbfs:/FileStore/shared_uploads/annemchandrareddy123@gmail.com/data/*.csv")

    Structured_DF.createOrReplaceTempView("retail_data")
    schmea_static_DF = Structured_DF.schema
    print(schmea_static_DF)
    Structured_DF.printSchema()

    Structured_DF \
        .selectExpr("CustomerID",
                    " (UnitPrice * Quantity) as total_cost",
                    "InvoiceDate") \
        .groupBy("CustomerID", window("InvoiceDate", "1 day")) \
        .sum("total_cost") \
        .show(100)

    # lets create process to stream data
    dataStream = spark.readStream \
        .schema(schmea_static_DF) \
        .option("maxFilesPerTrigger", 1) \
        .format("csv") \
        .option("header", "true") \
        .load("dbfs:/FileStore/shared_uploads/annemchandrareddy123@gmail.com/data/*.csv")

    print(dataStream.isStreaming)

    # lets write some business logic to aggreate and group the data on customer id
    dataStream_groupby = dataStream \
        .selectExpr("CustomerID", "(Quantity * UnitPrice) as total_cost",
                    "InvoiceDate") \
        .groupBy(col("CustomerID"), window(col("InvoiceDate"), "1 day")) \
        .sum("total_cost")

    dataStream_groupby.writeStream \
        .format("console") \
        .queryName("customer_purchases") \
        .outputMode("complete") \
        .start()



# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.functions import window, column, desc, col, date_format

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master('local[3]') \
        .appName("Structured Streaming") \
        .getOrCreate()

    # lets first read it as DF and analyze
    spark.conf.set("spark.sql.shuffle.partitions", "5")
    Structured_DF = spark.read \
        .format("csv") \
        .option("inferschema", "true") \
        .option("header", "true") \
        .load("dbfs:/FileStore/shared_uploads/annemchandrareddy123@gmail.com/*.csv")
    
    Structured_DF.printSchema()
    
    #Basically MLLib require data in Numerical format
    preppedDataFrame = Structured_DF\
                        .na.fill(0)\
                        .withColumn("day of week", date_format("InvoiceDate", "EEEE"))\
                        .coalesce(5)\
                       # .show(1000)
    
    preppedDataFrame.printSchema()
    
    indexer = StringIndexer()\
               .setInputCol("day of week")\
               .setOutputCol("day of week index")
    
    #indexedDataFrame = indexer.fit(preppedDataFrame).transform(preppedDataFrame)
    #indexedDataFrame.show(5)
    
    encoder = OneHotEncoder()\
                  .setInputCol("day of week index")\
                  .setOutputCol("day of week encoded")

    #encodedDataFrame = encoder.fit(indexedDataFrame).transform(indexedDataFrame)
   # encodedDataFrame.show(5)
    
    vectorAssembler = VectorAssembler()\
                      .setInputCols(["Quantity","UnitPrice","day_of_week_encoded"])\
                      .setOutputCol("features")
        
    transformPipeline = Pipeline()\
                        .setStages([indexer, encoder, vectorAssembler])
    
    trainDataFrame = indexedDataFrame\
                  .where("InvoiceDate<'2011-07-01'")\
          
    testDataFrame = indexedDataFrame\
                    .where("InvoiceDate>='2011-07-01'")\
                    .show(5)
    
    fittedPipeline = transformPipeline.fit(trainDataFrame)
    transformPipelinelast = fittedPipeline.transform(trainDataFrame)
    
    transformPipelinelast.show(5)
    
               
            
    
    
    
    

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.functions import window, column, desc, col, date_format

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master('local[3]') \
        .appName("Structured Streaming") \
        .getOrCreate()

    # lets first read it as DF and analyze
    spark.conf.set("spark.sql.shuffle.partitions", "5")
    Structured_DF = spark.read \
        .format("csv") \
        .option("inferschema", "true") \
        .option("header", "true") \
        .load("dbfs:/FileStore/shared_uploads/annemchandrareddy123@gmail.com/*.csv")
    
    Structured_DF.printSchema()
    
    #Basically MLLib require data in Numerical format
    preppedDataFrame = Structured_DF\
                        .na.fill(0)\
                        .withColumn("day of week", date_format("InvoiceDate", "EEEE"))\
                        .coalesce(5)\
                       # .show(1000)
    
    preppedDataFrame.printSchema()
    
    indexer = StringIndexer()\
               .setInputCol("day of week")\
               .setOutputCol("day of week index")
    
    #indexedDataFrame = indexer.fit(preppedDataFrame).transform(preppedDataFrame)
    #indexedDataFrame.show(5)
    
    encoder = OneHotEncoder()\
                  .setInputCol("day of week index")\
                  .setOutputCol("day of week encoded")

    #encodedDataFrame = encoder.fit(indexedDataFrame).transform(indexedDataFrame)
   # encodedDataFrame.show(5)
    
    vectorAssembler = VectorAssembler()\
                      .setInputCols(["Quantity","UnitPrice","day of week encoded"])\
                      .setOutputCol("features")
        
    transformPipeline = Pipeline()\
                        .setStages([indexer, encoder, vectorAssembler])
    
    trainDataFrame = preppedDataFrame\
                  .where("InvoiceDate<'2011-07-01'")\
          
    testDataFrame = preppedDataFrame\
                    .where("InvoiceDate>='2011-07-01'")\
                    
    
    fittedPipeline = transformPipeline.fit(trainDataFrame)
    transformPipelinelast = fittedPipeline.transform(trainDataFrame)
    
    transformPipelinelast.show(5)
    
    transformPipelinelast.cache()
    
    kmeans = KMeans()\
             .setK(20)\
             .setSeed(1)
    
    kmeansModel = kmeans.fit(transformPipelinelast)
    
    cost = kmeansModel.summary.trainingCost
    print("Cost: ", cost)
    
    
    transformedTest = fittedPipeline.transform(testDataFrame)
    predictions = kmeansModel.transform(transformedTest)
    cost = kmeansModel.summary.predictions
    print("Cost: ", cost)

    
    
    
    