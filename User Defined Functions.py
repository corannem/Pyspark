# Databricks notebook source
import re
import logging

# Create a logger object
logger = logging.getLogger(__name__)

# Set the log level
logger.setLevel(logging.INFO)

# Define a log handler (e.g., writing logs to a file)
handler = logging.FileHandler('logfile.log')

# Set the log format
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(handler)
survey_df = spark.read.format("csv").option("header","true").option("inferschema","true")\
            .load("dbfs:/FileStore/shared_uploads/annemchandrareddy123@gmail.com/survey.csv")

survey_df.select("Gender").distinct().show(100)

def parse_gender(gender):
    female_pattern = r"^f$|f.m|w.m"
    male_pattern = r"^m$|ma|m.l"
    if re.search(female_pattern, gender.lower()):
        return "Female"
    elif re.search(male_pattern, gender.lower()):
        return "Male"
    else:
        return "Unknown"

#register the udf
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
logger.info("Catalog Entry")
parse_gender_udf = udf(parse_gender, StringType())
[logger.info(r) for r in spark.catalog.listFunctions() if "parse_gender" in r.name]
#coloumn object expression
survey_df2 = survey_df.withColumn("Gender",parse_gender_udf("Gender"))
survey_df2.select("Gender").distinct().show(100)

# How to register as sql function and it registers in catalog
logger.info("Catalog Entry")
spark.udf.register("parse_gender_udf",parse_gender,StringType())
[logger.info(r) for r in spark.catalog.listFunctions() if "parse_gender" in r.name]
survey_df3 = survey_df.withColumn("Gender",expr("parse_gender_udf(Gender)"))
survey_df3.select("Gender").distinct().show(100)