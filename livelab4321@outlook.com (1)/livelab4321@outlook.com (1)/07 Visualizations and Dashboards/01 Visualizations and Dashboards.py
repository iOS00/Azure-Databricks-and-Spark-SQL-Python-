# Databricks notebook source
order_details = spark.read.parquet("/FileStore/tables/gold/order_details")
monthly_sales = spark.read.parquet("/FileStore/tables/gold/monthly_sales")

# COMMAND ----------

# use display() function (not method) to display the dataframe
# use the "+" in the UI output to explore options: Data Profile and Visualization
# you can add multiple Data Profile and Visualization and rename them if needed

display(order_details)

# COMMAND ----------

# MAGIC %md
# MAGIC Some text for my notebook

# COMMAND ----------

# MAGIC %md
# MAGIC #Title for my dashboard

# COMMAND ----------

# add text to dashboard using HTML
displayHTML("""<font size="6" color="red" face="sans-serif">Sales Dashboard</font>""")

# COMMAND ----------


