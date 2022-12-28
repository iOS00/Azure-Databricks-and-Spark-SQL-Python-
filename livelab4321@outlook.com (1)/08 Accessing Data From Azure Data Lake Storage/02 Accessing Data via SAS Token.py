# Databricks notebook source
# required configurations. Here datalake6391 - storrage account name
spark.conf.set("fs.azure.account.auth.type.datalake6391.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.datalake6391.dfs.core.windows.net", 
               "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.datalake6391.dfs.core.windows.net", "sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2022-12-27T23:47:45Z&st=2022-12-27T15:47:45Z&spr=https&sig=zHMkjJ%2BLqhXtadoELaRsgBO%2FSz3QfrmuMqUquPisNno%3D")#remember to remove "?" at the beginning av token

# COMMAND ----------

spark.read.csv("abfss://bronze@datalake6391.dfs.core.windows.net/countries.csv", header=True).display()

# COMMAND ----------


