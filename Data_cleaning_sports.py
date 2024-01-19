# Databricks notebook source


# COMMAND ----------

from pyspark.sql.functions import explode

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Loading Data from DBFS
df1 = spark.read.options(inferSchema=True).format("json").load("dbfs:/FileStore/shared_uploads/rohitkushwaha630@gmail.com/sport_expenditure-1.json")

# COMMAND ----------

df2=df1.select(explode(df1.data).alias("data"),df1.fields)
df2= df2.select(explode(df2.data).alias("data"),"fields")
df2.show(n=2)

# COMMAND ----------

df3=df2.select("data",explode(df2.fields).alias("fields"))
df3.show(n=2)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


