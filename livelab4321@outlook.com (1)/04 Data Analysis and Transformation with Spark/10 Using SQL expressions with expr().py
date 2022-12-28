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

# import expr function
from pyspark.sql.functions import expr

countries.select(expr('NAME as country_name')).display()

# COMMAND ----------

# SQL Left()
countries.select(expr( 'left(NAME, 2) as country_name')).display()

# COMMAND ----------

# create new column based on SQL Case
#NB doesn't work with block indentations

countries.withColumn('population_class', expr("case when population > 100000000 then 'large' when population > 50000000 then 'medium' else 'small' end")).display()

# COMMAND ----------

countries.withColumn('area_class', expr("case when area_km2>1000000 then 'large' when area_km2 >300000 then 'medium' else 'small' end")).display()
