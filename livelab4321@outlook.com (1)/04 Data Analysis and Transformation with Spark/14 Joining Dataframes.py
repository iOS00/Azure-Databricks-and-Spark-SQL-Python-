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

# Reading in the regions csv file
regions_path = '/FileStore/tables/country_regions.csv'
 
regions_schema = StructType([
                    StructField("Id", StringType(), False),
                    StructField("NAME", StringType(), False)
                    ]
                    )
 
regions = spark.read.csv(path=regions_path, header=True, schema=regions_schema)

# COMMAND ----------

countries.display()

# COMMAND ----------

regions.display()

# COMMAND ----------

# use inner join

countries.join(regions, countries['region_id'] == regions['id'], 'inner').display()

# COMMAND ----------

# perform right join

countries.join(regions, countries['region_id'] == regions['id'], 'right').display()

# COMMAND ----------

# to select just the needed columns use ...join()...select()

countries.join(regions, countries['region_id'] == regions['id'], 'right').select(countries['name'], regions['name'], countries['population']).display()

# COMMAND ----------

# combine join with alias and sort. Make sure that all the needed functions are imported

from pyspark.sql.functions import *

countries.join(regions, regions['id'] == countries['region_id'], 'inner'). \
select(countries['name'].alias('country_name'), regions['name'].alias('region_name'), countries['population']). \
sort(countries['population'].desc()). \
display()


