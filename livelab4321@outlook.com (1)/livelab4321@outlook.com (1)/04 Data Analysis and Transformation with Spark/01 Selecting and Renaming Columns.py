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

countries = spark.read.csv(path = countries_path, header = True, schema= countries_schema)

# COMMAND ----------

countries.display()

# COMMAND ----------

#case insencitive. This way doen't allow applying methods on output
countries.select('name', 'capital', 'population').display()

# COMMAND ----------

#case insencitive. This way allows applying methods on output
countries.select(countries['name'], countries['capital'], countries['population']).display()

# COMMAND ----------

#Case Sensitive
countries.select(countries.NAME, countries.CAPITAL, countries.POPULATION).display()

# COMMAND ----------

# another alternative is by importing and calling function

from pyspark.sql.functions import col
countries.select(col('name'), col('capital'), col('population')).display()


# COMMAND ----------

# Rename columns - there are several methods:

#make kolumn aliases
countries.select(countries['name'].alias('country_name'), countries['capital'].alias('capital_city'), countries['population']).display()

# COMMAND ----------

countries.select('name', 'capital', 'population').withColumnRenamed('name', 'country_name').withColumnRenamed('capital', 'capital_city').display()

# COMMAND ----------

# import regions csv file

regions_schema = StructType([
    StructField('Id', StringType(), False),
    StructField('NAME', StringType(), False)
])

# COMMAND ----------

regions_path = '/FileStore/tables/country_regions.csv'

# COMMAND ----------

regions = spark.read.csv(path = regions_path, header = True, schema = regions_schema)

# COMMAND ----------

regions.display()

# COMMAND ----------

regions.select(regions['Id'], regions['name'].alias('continent')).display()

# COMMAND ----------

regions.select(regions['Id'], regions['name']).withColumnRenamed('name', 'continent').display()

# COMMAND ----------


