# Databricks notebook source
application_id = "960ecd7d-c684-45fd-a630-bb3510a943d3"
tenant_id = "0d5c8b9f-2b8b-46ef-b63a-e8278105f6af"
secret = "_j.8Q~uXvfCHgRDe1scKCa8ZZR~oPE.MnxJHPczo" # the value of the secret is available just after creation (copied from previous lab)

# COMMAND ----------

container_name = "bronze"
account_name = "datalake6391"
mount_point = "/mnt/bronze"

# COMMAND ----------

#this method is simple for authentication but is not recommended (can expose token)
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": application_id,
          "fs.azure.account.oauth2.client.secret": secret, 
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

#if mount succeeded we shall get "True" in the output
dbutils.fs.mount(source = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/",
                mount_point = mount_point,
                extra_configs = configs)

# COMMAND ----------

# to unmount. #if unmount succeeded we shall get "True" in the output
dbutils.fs.unmount('/mnt/bronze')

# COMMAND ----------

# MAGIC %md
# MAGIC to create a secret scope create a Key Vault in the main resource group that contains databricks (main group for this lab, not the group created automatically by databricks).
# MAGIC The secret scope is the Key Vault name. 
# MAGIC In my lab - databricks-secrets-6391. And 3 secrets have being created inside the key vault: secret, tenant-id, application-id for storing values of variables located in the first cell of current notebook (they actually should be stored in the Key Vault directly, from the beginning, for safety reason)

# COMMAND ----------

# Follow the demo to create a secret scope, so it won't be needed to store variables as we did in the 1 cell of the notebook 
# right click on "Microsoft Azure"//open in new window//in the url address, after "#"" provide secrets/createScope
# in my lab it looks like: https://adb-7437217614949944.4.azuredatabricks.net/?o=7437217614949944#secrets/createScope
# make sure you choose Manage Principals: "All Asers" - to allow all users to modify SecretScope

# to access secret values
dbutils.secrets.get(scope = "databricks-secrets-6391", key = "tenant-id")


# COMMAND ----------

# to get visible output of the secret value (not recommended):
for i in dbutils.secrets.get(scope = "databricks-secrets-6391", key = "tenant-id"):
    print(i)

# COMMAND ----------

# so now we can replace our 1st cell of the notebook with more common and secure way to store our credentials:
#remember - values unique for my lab

application_id = dbutils.secrets.get(scope = "databricks-secrets-6391", key = "application-id")
tenant_id = dbutils.secrets.get(scope = "databricks-secrets-6391", key = "tenant-id")
secret = dbutils.secrets.get(scope = "databricks-secrets-6391", key = "secret")

# COMMAND ----------

# and re-run mounting process with our variables updated

container_name = "bronze"
account_name = "datalake6391"
mount_point = "/mnt/bronze"

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": application_id,
          "fs.azure.account.oauth2.client.secret": secret, 
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

#if mount succeeded we shall get "True" in the output
dbutils.fs.mount(source = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/",
                mount_point = mount_point,
                extra_configs = configs)
