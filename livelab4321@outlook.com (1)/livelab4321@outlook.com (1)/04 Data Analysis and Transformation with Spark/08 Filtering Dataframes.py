# Databricks notebook source
# Reading in the countries.csv file into a Dataframe called countries
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

countries=spark.read.csv(path=countries_path, header=True, schema=countries_schema)

# COMMAND ----------

countries.display()

# COMMAND ----------

countries.filter(countries['population']>1000000000).display()

# COMMAND ----------

# filter the countries having capital name starting with "B" ("B" on the 1 index)
#function locate() returns the position of the substing in the target string

from pyspark.sql.functions import locate

countries.filter(locate("B", countries['capital'])==1).display()

# COMMAND ----------

# combine conditions
countries.filter(  (locate("B", countries['capital']) ==1) & (countries['population']>1000000000)  ).display()

# COMMAND ----------

# use Or operator (|)
countries.filter(  (locate("B", countries['capital']) ==1) | (countries['population']>1000000000)  ).display()

# COMMAND ----------

# use SQL syntax
countries.filter("region_id==10").display()

# COMMAND ----------

# use SQL syntax
countries.filter("region_id != 10").display()

# COMMAND ----------

# use SQL syntax
countries.filter("region_id != 10 and population == 0").display()

# COMMAND ----------

from pyspark.sql.functions import length

# COMMAND ----------

countries.filter( (length(countries['name'])>15) & (countries['region_id'] != 10) ).display()

# COMMAND ----------

#alternative using SQL syntax
countries.filter("length(name)>15 and region_id != 10").display()

# COMMAND ----------


