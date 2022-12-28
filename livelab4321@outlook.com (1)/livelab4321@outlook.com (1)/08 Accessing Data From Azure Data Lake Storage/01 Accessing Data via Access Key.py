# Databricks notebook source
9j/fz7/AtoYtZLvAMyocUztgTVe41G+lhBbaMzIg9XlIGQ6wU5x0BJ7wUGqU8SEJye1R56q/ePZj+AStSzcXOA==

# COMMAND ----------

# connect to Azure Data Lake2 storrage account (this way is not recommended-reveals key for users of this notebook)
spark.conf.set(
    "fs.azure.account.key.datalake6391.dfs.core.windows.net", 
    "9j/fz7/AtoYtZLvAMyocUztgTVe41G+lhBbaMzIg9XlIGQ6wU5x0BJ7wUGqU8SEJye1R56q/ePZj+AStSzcXOA==")

# COMMAND ----------

# now we should be able to read the file from the Azure Data Lake Gen2 Storrage account
# where abfss stands for "azure blob file storrage system"

countries = spark.read.csv("abfss://bronze@datalake6391.dfs.core.windows.net/countries.csv", header=True)
regions = spark.read.csv("abfss://bronze@datalake6391.dfs.core.windows.net/country_regions.csv", header=True)

# COMMAND ----------

regions.display()

# COMMAND ----------

countries.display()

# COMMAND ----------


