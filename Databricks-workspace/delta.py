# Databricks notebook source
spark.sql('CREATE DATABASE IF NOT EXISTS retail_db3')

# COMMAND ----------

spark.sql('use retail_db3')

# COMMAND ----------

spark.sql('show databases').show()

# COMMAND ----------

spark.sql('describe database retail_db3').show()

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases;

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

orders_schema = StructType([
    StructField('order_id',    StringType(),  False),
    StructField('customer_id', StringType(),  True),
    StructField('product_id',  StringType(),  True),
    StructField('city',        StringType(),  True),
    StructField('order_date',  StringType(),  True),
    StructField('quantity',    IntegerType(), True),
    StructField('unit_price',  DoubleType(),  True),
    StructField('status',      StringType(),  True),
])

# COMMAND ----------

orders_df = spark.read.format('csv').option('header', 'true').schema(orders_schema).load('/Volumes/mycatalog/pyspark/files/orders.csv')
orders_df = orders_df.withColumn('order_date', to_date(col('order_date'), 'yyyy-MM-dd'))

# COMMAND ----------

#orders_df.display()
orders_df.printSchema()

# COMMAND ----------

products_schema = StructType([
    StructField('product_id',   StringType(), True),
    StructField('product_name', StringType(), True),
    StructField('category',     StringType(), True),
    StructField('brand',        StringType(), True),
    StructField('base_price',   DoubleType(), True),
    StructField('gst_pct',      IntegerType(),True),
])
customers_schema = StructType([
    StructField('customer_id',        StringType(), True),
    StructField('customer_name',      StringType(), True),
    StructField('city',               StringType(), True),
    StructField('tier',               StringType(), True),
    StructField('registration_date',  StringType(), True),
])


# COMMAND ----------

products_df = spark.read.format('csv')\
.option('header', 'true')\
.schema(products_schema)\
.load('/Volumes/mycatalog/pyspark/files/products.csv')

customers_df = spark.read.format('csv').option('header', 'true').schema(customers_schema).load('/Volumes/mycatalog/pyspark/files/customers.csv')

# COMMAND ----------

products_df.printSchema()
customers_df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC use retail_db3;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS orders (
# MAGIC     order_id    STRING,
# MAGIC     customer_id STRING,
# MAGIC     product_id  STRING,
# MAGIC     city        STRING,
# MAGIC     order_date  DATE,
# MAGIC     quantity    INT,
# MAGIC     unit_price  DOUBLE,
# MAGIC     status      STRING
# MAGIC )
# MAGIC USING DELTA;   
# MAGIC

# COMMAND ----------

orders_df.write.format('delta').mode('append').saveAsTable('retail_db3.orders')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders;

# COMMAND ----------

# MAGIC %md
# MAGIC writing to a delta table along with table creation

# COMMAND ----------

orders_df.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable('retail_db3.orders1')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC --insert using sql
# MAGIC
# MAGIC insert into retail_db3.orders values ('ORD018', 'CUST107', 'PROD_E', 'Bangalore', '2024-02-25', 1,7500.00, 'Pending' );

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_db3.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC update retail_db3.orders set city = 'Chennai' where customer_id = 'CUST107';

# COMMAND ----------

#update using pyspark

from delta.tables import DeltaTable

delta_orders = DeltaTable.forName(spark, 'retail_db3.orders')

delta_orders.update(condition = "customer_id = 'CUST107'", set = {'city': "'Secunderabad'"})

# COMMAND ----------

#Delete using Pyspark

delta_orders.delete(condition = "customer_id = 'CUST107'")

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from retail_db3.orders where customer_id = 'CUST106';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_db3.orders;

# COMMAND ----------

df121 = spark.table('retail_db3.orders')
df121.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC --sql joins
# MAGIC select * from retail_db3.orders o
# MAGIC left join retail_db3.products p on o.product_id = p.product_id
# MAGIC left join retail_db3.customers c on o.customer_id = c.customer_id;

# COMMAND ----------

#pyspark tables join
# read the three tables into 3 different dataframes

orders_df = spark.table('retail_db3.orders')
customers_df = spark.table('retail_db3.customers')
products_df = spark.table('retail_db3.products')


#then join the 3 dataframes

# COMMAND ----------

# Time Travel
delta_orders = DeltaTable.forName(spark, 'retail_db3.orders')
delta_orders.history().display()

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history retail_db3.orders;

# COMMAND ----------

df_v1 = spark.read.format('delta').option('versionAsof', 1).table('retail_db3.orders')
df_v1.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_db3.orders version as of 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_db3.orders timestamp as of '2026-07-16T06:28:42.000+00:00';

# COMMAND ----------

df_vts = spark.read.format('delta').option('timestampAsOf', '2026-07-16T06:29:09.000+00:00').table('retail_db3.orders')
df_vts.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC restore table retail_db3.orders to version as of 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Retention
# MAGIC
# MAGIC vacuum retail_db3.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum retail_db3.orders retain 1 hours;

# COMMAND ----------

