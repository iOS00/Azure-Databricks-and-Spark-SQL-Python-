# Databricks notebook source
# look through mount points

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC alternative to the code above is to use the appropriate magic function

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

application_id = "960ecd7d-c684-45fd-a630-bb3510a943d3"

tenant_id = "0d5c8b9f-2b8b-46ef-b63a-e8278105f6af"

secret = "_j.8Q~uXvfCHgRDe1scKCa8ZZR~oPE.MnxJHPczo"

# COMMAND ----------

# Performing the mount
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "960ecd7d-c684-45fd-a630-bb3510a943d3",
          "fs.azure.account.oauth2.client.secret": "_j.8Q~uXvfCHgRDe1scKCa8ZZR~oPE.MnxJHPczo",
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/0d5c8b9f-2b8b-46ef-b63a-e8278105f6af/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://bronze@datalake6391.dfs.core.windows.net/",
  mount_point = "/mnt/bronze",
  extra_configs = configs)

# COMMAND ----------

# now we are able to access data without providing logging credentials each time we open new notebook/session by providing relative path to
# mount-point

spark.read.csv("/mnt/bronze/countries.csv", header = True).display()

# COMMAND ----------

# to unmount the container:
dbutils.fs.unmount('/mnt/bronze')


# COMMAND ----------

#look through available mounted points
display(dbutils.fs.mounts())
