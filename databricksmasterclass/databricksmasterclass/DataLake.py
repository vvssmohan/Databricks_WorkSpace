# Databricks notebook source
# MAGIC %run "/databricksmasterclass/Tutorial"

# COMMAND ----------

# MAGIC %md
# MAGIC # DELTA LAKE

# COMMAND ----------

df_sales.write.format("delta")\
                .mode("append")\
                .option('path','abfss://destination@datalakemohan07.dfs.core.windows.net/delta')\
                .save()

# COMMAND ----------

# MAGIC %md
# MAGIC # MANAGED VS EXTERNAL DELTA TABLES
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Managed Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS SalesDB;

# COMMAND ----------

# DBTITLE 1,Cell 7
# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS SalesDB.StdTable
# MAGIC (
# MAGIC     id INT,
# MAGIC     name STRING,
# MAGIC     marks INT
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO SalesDB.StdTable
# MAGIC VALUES (1, 'John', 80),
# MAGIC        (2, 'Mary', 90),
# MAGIC        (3, 'Mike', 70),
# MAGIC        (4, 'Sara', 85),
# MAGIC        (5, 'Mohan',97),
# MAGIC        (6, 'Priya', 95),
# MAGIC        (7, 'Ravi', 88),
# MAGIC        (8, 'Priya', 92),
# MAGIC        (9, 'Priya', 89),
# MAGIC        (10, 'Priya', 91)
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from SalesDB.StdTable;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE SalesDB.StdTable;

# COMMAND ----------

# MAGIC %md
# MAGIC # EXTERNAL TABLE

# COMMAND ----------

# DBTITLE 1,Cell 12
# MAGIC %sql
# MAGIC -- create table salesdb.externaltable
# MAGIC -- (
# MAGIC --     id INT,
# MAGIC --     name STRING,
# MAGIC --     marks INT
# MAGIC -- )
# MAGIC -- USING DELTA LOCATION 'abfss://destination@datalakemohan07.dfs.core.windows.net/externaltable'
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 13
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM delta.`abfss://destination@datalakemohan07.dfs.core.windows.net/delta`
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW EXTERNAL LOCATIONS;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW STORAGE CREDENTIALS;

# COMMAND ----------

# MAGIC %md
# MAGIC # DELTA TABLES FUNCTIONALITIES

# COMMAND ----------

# MAGIC %sql DESCRIBE HISTORY delta.`abfss://destination@datalakemohan07.dfs.core.windows.net/delta`

# COMMAND ----------

# MAGIC %md
# MAGIC ## TIME TRAVEL

# COMMAND ----------

# MAGIC %sql SELECT *
# MAGIC FROM delta.`abfss://destination@datalakemohan07.dfs.core.windows.net/delta`
# MAGIC VERSION AS OF 1;

# COMMAND ----------

# MAGIC %md
# MAGIC # VACUUM 

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM salesdb.externaltable;

# COMMAND ----------

# MAGIC %md
# MAGIC # VACUUM RETAIN 0 HRS

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VACUUM salesdb.externaltable RETAIN 0 HOURS;

# COMMAND ----------

# MAGIC %md
# MAGIC # DELTA TABLE OPTIMIZATION

# COMMAND ----------

# MAGIC %md
# MAGIC ## # OPTIMIZE

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from SalesDB.StdTable;

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE SalesDB.StdTable;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from SalesDB.StdTable;

# COMMAND ----------

# MAGIC %md
# MAGIC ## ZORDER BY

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE SalesDB.StdTable ZORDER BY (id)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from SalesDB.StdTable;

# COMMAND ----------

# MAGIC %md
# MAGIC # INCREMENTAL LOADING
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader

# COMMAND ----------

# MAGIC %md
# MAGIC **Streaming DataFrame**
# MAGIC

# COMMAND ----------

df =spark.readStream.format('cloudFiles')\
    .option("cloudFiles.format", "csv")\
    .option("cloudFiles.schemaLocation", "abfss://aldestination@datalakemohan07.dfs.core.windows.net/checkpoint")\
    .load("abfss://alsource@datalakemohan07.dfs.core.windows.net")

# COMMAND ----------

# DBTITLE 1,Cell 36
df.writeStream.format('delta')\
              .option('checkpointLocation', 'abfss://aldestination@datalakemohan07.dfs.core.windows.net/checkpoint')\
              .trigger(processingTime='5 seconds')\
              .start('abfss://aldestination@datalakemohan07.dfs.core.windows.net')

# COMMAND ----------

# MAGIC %md
# MAGIC stop this stream when you not use ..its high cost
# MAGIC

# COMMAND ----------

