# Databricks notebook source
# import the relevant data types

from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType

# COMMAND ----------

orders_path = "/FileStore/tables/bronze/orders.csv"

orders_schema = StructType([
                    StructField("ORDER_ID", IntegerType(), False),
                    StructField("ORDER_DATETIME", StringType(), False),
                    StructField("CUSTOMER_ID", IntegerType(), False),
                    StructField("ORDER_STATUS", StringType(), False),
                    StructField("STORE_ID", IntegerType(), False)
                    ]
                    )

orders=spark.read.csv(path=orders_path, header=True, schema=orders_schema)

# COMMAND ----------

orders.display()

# COMMAND ----------

# to convert the column to_timestamp the column has to be in string format(was set in schema above)
from pyspark.sql.functions import to_timestamp

# COMMAND ----------

orders= orders.select('ORDER_ID', \
                     to_timestamp(orders['order_datetime'], "dd-MMM-yy kk.mm.ss.SS").alias('ORDER_TIMESTAMP'), \
                     'CUSTOMER_ID', \
                     'ORDER_STATUS', \
                     'STORE_ID'
                     )

# COMMAND ----------

orders.dtypes

# COMMAND ----------

orders.display()

# COMMAND ----------

# filter the data with "COMPLETE" order status
orders = orders.filter(orders['order_status'] == "COMPLETE")

# COMMAND ----------

# Reading the stores csv file

stores_path = "/FileStore/tables/bronze/stores.csv"

stores_schema = StructType([
                    StructField("STORE_ID", IntegerType(), False),
                    StructField("STORE_NAME", StringType(), False),
                    StructField("WEB_ADDRESS", StringType(), False),
                    StructField("LATITUDE", DoubleType(), False),
                    StructField("LONGITUDE", DoubleType(), False)
                    ]
                    )

stores=spark.read.csv(path=stores_path, header=True, schema=stores_schema)

# COMMAND ----------

stores.display()

# COMMAND ----------

#join tables
orders = orders.join(stores, orders['store_id']==stores['store_id'], 'left').select('ORDER_ID', 'ORDER_TIMESTAMP', 'CUSTOMER_ID', 'STORE_NAME')

# COMMAND ----------

orders.display()

# COMMAND ----------

# writing the orders dataframe as a parquet file in the silver layer, should use mode = 'overwrite' in this instance (create a folder)
orders.write.parquet("/FileStore/tables/silver/orders", mode = 'overwrite')

# COMMAND ----------

# Reading the order items csv file

order_items_path = "/FileStore/tables/bronze/order_items.csv"

order_items_schema = StructType([
                    StructField("ORDER_ID", IntegerType(), False),
                    StructField("LINE_ITEM_ID", IntegerType(), False),
                    StructField("PRODUCT_ID", IntegerType(), False),
                    StructField("UNIT_PRICE", DoubleType(), False),
                    StructField("QUANTITY", IntegerType(), False)
                    ]
                    )

order_items=spark.read.csv(path=order_items_path, header=True, schema=order_items_schema)

# COMMAND ----------

# reviewing the order_items dataframe, the line_item_id column can be removed
order_items.display()

# COMMAND ----------

# selecting only the required columns and assigning this back to the order_items variable
order_items = order_items.drop('LINE_ITEM_ID')

# COMMAND ----------

# revieiwing the order_items dataframe
order_items.display()

# COMMAND ----------

# writing the order_items parquet table in the silver layer
order_items.write.parquet("/FileStore/tables/silver/order_items", mode='overwrite')

# COMMAND ----------

# Reading the products csv file

products_path = "/FileStore/tables/bronze/products.csv"

products_schema = StructType([
                    StructField("PRODUCT_ID", IntegerType(), False),
                    StructField("PRODUCT_NAME", StringType(), False),
                    StructField("UNIT_PRICE", DoubleType(), False)
                    ]
                    )

products=spark.read.csv(path=products_path, header=True, schema=products_schema)

# COMMAND ----------

products.display()

# COMMAND ----------

# writing the parquet file
products.write.parquet('/FileStore/tables/silver/products', mode='overwrite')

# COMMAND ----------

# Reading the customers csv file
customers_path = "/FileStore/tables/bronze/customers.csv"

customers_schema = StructType([
                    StructField("CUSTOMER_ID", IntegerType(), False),
                    StructField("FULL_NAME", StringType(), False),
                    StructField("EMAIL_ADDRESS", StringType(), False)
                    ]
                    )

customers=spark.read.csv(path=customers_path, header=True, schema=customers_schema)

# COMMAND ----------

customers.display()

# COMMAND ----------

# writing the parquet file
customers.write.parquet('/FileStore/tables/silver/customers', mode='overwrite')
