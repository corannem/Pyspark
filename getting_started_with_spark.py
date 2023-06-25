# Databricks notebook source
daimonds_df = spark.read.format("csv")\
              .option("header","true")\
              .option("inferSchema","true")\
              .load("/databrics-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv")
daimonds_df.show(10)