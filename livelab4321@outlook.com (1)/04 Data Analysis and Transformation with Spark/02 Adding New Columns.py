# Databricks notebook source
countries_path = '/FileStore/tables/countries.csv'

from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType
countries_schema = StructType([
                    StructField("COUNTRY_ID", IntegerType(), False),
                    StructField("NAME", StringType(), False),
                    StructField("NATIONALITY", StringType(), False),
                    StructField("COUNTRY_CODE", StringType(), False),
                    StructField("ISO_ALPHA2", StringType(), False),
                    StructField("CAPITAL", StringType(), False),
                    StructField("POPULATION", DoubleType(), False),
                    StructField("AREA_KM2", IntegerType(), False),
                    StructField("REGION_ID", IntegerType(), True),
                    StructField("SUB_REGION_ID", IntegerType(), True),
                    StructField("INTERMEDIATE_REGION_ID", IntegerType(), True),
                    StructField("ORGANIZATION_REGION_ID", IntegerType(), True)
                    ]
                    )

# COMMAND ----------

countries = spark.read.csv(path = countries_path, header= True, schema= countries_schema)

# COMMAND ----------

# create new column containing current date (use function to populate data)

from pyspark.sql.functions import current_date

# withColumn(column name, column data) method creates new column 
countries.withColumn('current_date', current_date()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC command below installs the library allowing python formatting of code 
# MAGIC %pip install black==22.3.0 tokenize-rt==4.2.1

# COMMAND ----------

# create a column of literal value
from pyspark.sql.functions import lit

countries.withColumn('updated_by', lit('MV')).display()

# COMMAND ----------

# add a column containing some calculations
countries.withColumn('population_m', countries['population']/1000000).display()
