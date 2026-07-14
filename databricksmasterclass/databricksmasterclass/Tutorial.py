# Databricks notebook source
# MAGIC %md
# MAGIC # DATABRICKS MASTERCLASS

# COMMAND ----------

mydata = [(1,'aa',30),(2,'bb',40),(3,'cc',50)]

myschema = "id INT, name STRING, marks INT"

df = spark.createDataFrame(mydata,schema=myschema)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Access Data

# COMMAND ----------

# spark.conf.set("fs.azure.account.auth.type.datalakemohan07.dfs.core.windows.net", "OAuth")
# spark.conf.set("fs.azure.account.oauth.provider.type.datalakemohan07.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# spark.conf.set("fs.azure.account.oauth2.client.id.datalakemohan07.dfs.core.windows.net", "4dccaf8e-2fb0-4408-9e53-8bd0d3ec9649")
# spark.conf.set("fs.azure.account.oauth2.client.secret.datalakemohan07.dfs.core.windows.net", "P_d8Q~spfVxU1RNHZ7QhvXUnbCi2gRwWsfN1Yar4mohan")
# spark.conf.set("fs.azure.account.oauth2.client.endpoint.datalakemohan07.dfs.core.windows.net", "https://login.microsoftonline.com/42f683cb-ef5f-47d4-8b54-7c66d797fc47mohan/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DB Utilities

# COMMAND ----------

# MAGIC %md
# MAGIC **dbutils.fs()**

# COMMAND ----------

dbutils.fs.ls("abfss://source@datalakemohan07.dfs.core.windows.net/")

# COMMAND ----------

# MAGIC %md
# MAGIC **dbutils.widgets**

# COMMAND ----------

dbutils.widgets.text("p_name","mohan")

# COMMAND ----------

var = dbutils.widgets.get("p_name")

# COMMAND ----------

var

# COMMAND ----------

# MAGIC %md
# MAGIC **dbutils.secrets**

# COMMAND ----------

dbutils.secrets.list(scope='mohanscope')

# COMMAND ----------

dbutils.secrets.get(scope='mohanscope',key='app-secret')

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Reading

# COMMAND ----------

df_sales = spark.read.format('csv')\
              .option('header',True)\
              .option('inferSchema',True)\
              .load('abfss://source@datalakemohan07.dfs.core.windows.net/') 


# COMMAND ----------

df_sales.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### PySpark Transformations

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_sales.withColumn('Item_Type',split(col('Item_Type'),' ')).display()

# COMMAND ----------

df_sales.withColumn('flag',lit(var)).display()

# COMMAND ----------

df_sales.withColumn('Item_Visibility',col('Item_Visibility').cast(StringType())).display()

# COMMAND ----------

