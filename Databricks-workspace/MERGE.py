# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

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

products_df = spark.read.format('csv').option('header', 'true').schema(products_schema).load('/Volumes/mycatalog/pyspark/files/products.csv')
customers_df = spark.read.format('csv').option('header', 'true').schema(customers_schema).load('/Volumes/mycatalog/pyspark/files/customers.csv')

# COMMAND ----------

products_df.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable('retail_db3.products')
customers_df.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable('retail_db3.customers')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_db3.products;

# COMMAND ----------

# MAGIC %sql
# MAGIC  -- sql code for merge
# MAGIC
# MAGIC -- Daily supplier feed (inline VALUES — acts as the source)
# MAGIC MERGE INTO retail_db3.products AS target
# MAGIC USING (
# MAGIC     SELECT 'PROD_C' AS product_id, 'Tata Salt 1kg'    AS product_name,
# MAGIC            'Grocery'  AS category, 'Tata Consumer' AS brand,
# MAGIC            920.00 AS base_price, 5 AS gst_pct
# MAGIC     UNION ALL
# MAGIC     SELECT 'PROD_H', 'Amul Butter 500g', 'Grocery', 'Amul', 320.00, 5
# MAGIC     UNION ALL
# MAGIC     SELECT 'PROD_I', 'Britannia Bread',  'FMCG',   'Britannia', 55.00, 5
# MAGIC ) AS source
# MAGIC ON target.product_id = source.product_id
# MAGIC  
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET
# MAGIC         target.base_price   = source.base_price,
# MAGIC         target.product_name = source.product_name
# MAGIC  
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (product_id, product_name, category, brand, base_price, gst_pct)
# MAGIC     VALUES (source.product_id, source.product_name, source.category,
# MAGIC             source.brand, source.base_price, source.gst_pct);

# COMMAND ----------

