# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df=spark.read.format("delta")\
    .option("header",True)\
        .option("inferSchema",True)\
            .load("abfss://bronze@nexflixprojectarun.dfs.core.windows.net/netflix_titles")

# COMMAND ----------

df.display()

# COMMAND ----------

avg_duration = df.select(avg("duration_minutes")).collect()[0][0]



# COMMAND ----------

avg_duration=int(avg_duration)
avg_duration=str(avg_duration)
print(avg_duration)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.fillna({"duration_minutes": avg_duration})

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.fillna({"duration_seasons": 1})

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df=df.withColumn("duration_minutes", col('duration_minutes').cast(IntegerType()))

# COMMAND ----------

a='9/8/2018'

year=a.split('/')[2]
print

# COMMAND ----------

from pyspark.sql.functions import col, split

df = df.withColumn("added_year", split(col('date_added'), '/').getItem(2))
display(df)

# COMMAND ----------

df.write.format("delta").mode("overwrite").option("path","abfss://silver@nexflixprojectarun.dfs.core.windows.net/netflix_titles").save()