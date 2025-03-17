# Databricks notebook source
# DBTITLE 1,Read
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SwiggyData").getOrCreate()

file_path = "dbfs:/FileStore/tables/swiggy.csv"

df = spark.read.csv(file_path, header=True, inferSchema=True)


# COMMAND ----------

# DBTITLE 1,dropping unwanted
df = df.drop("ID", "Area", "Total ratings", "Address")


# COMMAND ----------

# DBTITLE 1,Dropping Duplicates

df = df.dropDuplicates(["Restaurant", "City"])


# COMMAND ----------

# DBTITLE 1,Checking missing values
from pyspark.sql.functions import col, sum

df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]).show()

