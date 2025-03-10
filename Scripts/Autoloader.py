# Databricks notebook source
# MAGIC %md
# MAGIC **Incrementaldata loadingwith auto loader**

# COMMAND ----------

checkpoint_path = "abfss://silver@nexflixprojectarun.dfs.core.windows.net"

# COMMAND ----------


df=(spark.readStream
  .format("cloudFiles")\
  .option("cloudFiles.format", "csv")\
  .option("cloudFiles.schemaLocation", checkpoint_path)\
  .load("abfss://raw@nexflixprojectarun.dfs.core.windows.net"))

 

# COMMAND ----------

display(df)

# COMMAND ----------

df.writeStream\
  .option("checkpointLocation", checkpoint_path)\
  .trigger(processingTime='10 seconds')\
  .start("abfss://bronze@nexflixprojectarun.dfs.core.windows.net/netflix_titles")