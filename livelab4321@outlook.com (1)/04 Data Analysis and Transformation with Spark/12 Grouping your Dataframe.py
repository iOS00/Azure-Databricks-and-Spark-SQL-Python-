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

#this code returns an object. To display it we need an aggregation to perform
countries.groupBy('region_id')

# COMMAND ----------

from pyspark.sql.functions import *

#code returns DataFrame object
countries.groupBy('region_id').sum('population')

# COMMAND ----------

countries.groupBy('region_id').sum('population').display()

# COMMAND ----------

# TIPS: to make code readable as multiline use " \" (whitespace before\ but not after!!!)
countries. \
groupBy('region_id'). \
sum('population'). \
display()

# COMMAND ----------

# with multiple columns

countries. \
groupBy('region_id'). \
sum('population' ,'area_km2'). \
display()

# COMMAND ----------

# to use multiple type aggregations (like sum, avg) - agg() method is required

countries.groupBy('region_id').agg(avg('population'), sum('area_km2')).display()

# COMMAND ----------

# groupBy multiple columns

countries.groupBy('region_id', 'sub_region_id').agg(avg('population'), sum('area_km2')).display()

# COMMAND ----------

# combine groubBy with renaming columns:
# here agg() is optional because just one type of aggregation is performed - sum()
countries.groupBy('region_id','sub_region_id'). \
agg(sum('population'),sum('area_km2')). \
withColumnRenamed('sum(population)','total_pop').withColumnRenamed('sum(area_km2)', 'total_area').display()


# COMMAND ----------

# use Aliases
countries.groupBy('region_id','sub_region_id'). \
agg(sum('population').alias('total_pop'),sum('area_km2').alias('total_area')).display()


# COMMAND ----------

countries.groupBy('region_id', 'sub_region_id'). \
agg(max('population').alias('max_pop'), \
    min('population').alias('min_pop')). \
sort(countries['region_id'].asc()). \
display()
