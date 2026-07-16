# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %sql
# MAGIC show external locations;

# COMMAND ----------

orders_df = spark.read.format('csv').option('header','true').option('inferschema', 'true').load('abfss://exdestination@datalakemohan07.dfs.core.windows.net/raw/orders.csv')


# COMMAND ----------

orders_df.display()

# COMMAND ----------

products_df = spark.read.format('csv').option('header','true').option('inferschema', 'true').load('/Volumes/databricksmaster_7405604775596291/default/exlocation-adls/raw/products.csv')

# COMMAND ----------

products_df.display()