# Databricks notebook source
# our bronze container has being mounted in previous notebook
# so now we'll mount our silver container

application_id = dbutils.secrets.get(scope = "databricks-secrets-6391", key = "application-id")
tenant_id = dbutils.secrets.get(scope = "databricks-secrets-6391", key = "tenant-id")
secret = dbutils.secrets.get(scope = "databricks-secrets-6391", key = "secret")

# COMMAND ----------

container_name = "silver"
account_name = "datalake6391"
mount_point = "/mnt/silver"

# COMMAND ----------

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

#repeat the step to mount the gold container

container_name = "gold"
account_name = "datalake6391"
mount_point = "/mnt/gold"

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

# read the data from the bronze container
countries_path = '/mnt/bronze/countries.csv'
 
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

countries = countries.drop("sub_region_id","intermediate_region_id","organization_region_id")

# COMMAND ----------

countries.write.parquet('/mnt/silver/countries')

# COMMAND ----------

regions_path = '/mnt/bronze/country_regions.csv'
 
regions_schema = StructType([
                    StructField("Id", StringType(), False),
                    StructField("NAME", StringType(), False)
                    ]
                    )
 
regions = spark.read.csv(path=regions_path, header=True, schema=regions_schema)

# COMMAND ----------

regions.display()

# COMMAND ----------

regions = regions.withColumnRenamed('NAME', 'REGION_NAME')

# COMMAND ----------

regions.display()

# COMMAND ----------

regions.write.parquet('/mnt/silver/regions')

# COMMAND ----------

countries = spark.read.parquet('/mnt/silver/countries')
regions = spark.read.parquet('/mnt/silver/regions')

# COMMAND ----------

countries.display()

# COMMAND ----------

regions.display()

# COMMAND ----------

# join and clean the data to store in the gold level:

country_data = countries.join(regions, countries['region_id'] == regions['Id'], 'left').drop('Id', 'region_id', 'country_code', 'iso_alpha_2')

# COMMAND ----------

country_data.display()

# COMMAND ----------

#load the cleaned data to the gold level as a parquet file
country_data.write.parquet('/mnt/gold/country_data')
