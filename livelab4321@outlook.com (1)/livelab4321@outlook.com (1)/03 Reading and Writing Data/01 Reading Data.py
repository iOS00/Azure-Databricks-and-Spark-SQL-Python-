# Databricks notebook source
spark.read.csv("/FileStore/tables/countries.csv")

# COMMAND ----------

countries_df = spark.read.csv("/FileStore/tables/countries.csv")

# COMMAND ----------

type(countries_df)

# COMMAND ----------

countries_df.show()

# COMMAND ----------

#or alternative to display in a tabular format
display(countries_df)

# COMMAND ----------

# reopen csv with enabling header= True

countries_df = spark.read.csv("/FileStore/tables/countries.csv", header = True)

# COMMAND ----------

display(countries_df)

# COMMAND ----------

#alternative way

countries_df = spark.read.options(header=True).csv('/FileStore/tables/countries.csv')

# COMMAND ----------

display(countries_df)

# COMMAND ----------

#check data types of columns

countries_df.dtypes

# COMMAND ----------

# to get the schema

countries_df.schema

# COMMAND ----------

# use .describe() - is a method, requires (), while dtypes, schema are attributes
countries_df.describe()

# COMMAND ----------

# To make sure that data is read properly, in the correct data format, it's recommended to use inferSchema = True
# expand > countries_df dropdown in the output to check the data format

countries_df = spark.read.options(header=True, inferSchema = True).csv('/FileStore/tables/countries.csv')

# COMMAND ----------

# # note that in example above 2 jobs have being involved (for schema and for data). 
# So use inferSchema is not costly efficient. Better solution is to specify schema yourself

# to import all types use: from pyspark.sql.types import *
from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType
countries_schema=StructType(List(
    StructField("COUNTRY_ID",IntegerType(),False),
    StructField(NAME,StringType,true),StructField(NATIONALITY,StringType,true),StructField(COUNTRY_CODE,StringType,true),StructField(ISO_ALPHA2,StringType,true),StructField(CAPITAL,StringType,true),StructField(POPULATION,StringType,true),StructField(AREA_KM2,StringType,true),StructField(REGION_ID,StringType,true),StructField(SUB_REGION_ID,StringType,true),StructField(INTERMEDIATE_REGION_ID,StringType,true),StructField(ORGANIZATION_REGION_ID,StringType,true)))


