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

countries.groupBy('region_id', 'sub_region_id').sum('population').display()

# COMMAND ----------

# pivoting allows to make all rows from pivoting column into columns. Applies after groupBy but before aggregate function

countries.groupBy('region_id', 'sub_region_id').sum('population').display()

# COMMAND ----------

countries.groupBy('region_id', 'sub_region_id').pivot('region_id').sum('population').display()

# COMMAND ----------

#we can remove region_id from the groupBy statement
countries.groupBy('sub_region_id').pivot('region_id').sum('population').display()


# COMMAND ----------


