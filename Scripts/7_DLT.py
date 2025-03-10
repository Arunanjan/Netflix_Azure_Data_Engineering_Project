# Databricks notebook source
import dlt

# COMMAND ----------

looktables_rules={"rule1":"show_id is not null"}

# COMMAND ----------

@dlt.table(
    name="gold_netflixdirectors"
)
def myfunc():
    df=spark.readStream.format("delta").load("abfss://silver@nexflixprojectarun.dfs.core.windows.net/netflix_directors")
    return df

# COMMAND ----------

@dlt.table(
    name="gold_netflixcountries"
)
def myfunc():
    df=spark.readStream.format("delta").load("abfss://silver@nexflixprojectarun.dfs.core.windows.net/netflix_countries")
    return df

# COMMAND ----------

@dlt.table(
    name="gold_netflixcategory"
)
def myfunc():
    df=spark.readStream.format("delta").load("abfss://silver@nexflixprojectarun.dfs.core.windows.net/netflix_category")
    return df

# COMMAND ----------

@dlt.table(
    name="gold_netflixcast"
)
def myfunc():
    df=spark.readStream.format("delta").load("abfss://silver@nexflixprojectarun.dfs.core.windows.net/netflix_cast")
    return df

# COMMAND ----------

@dlt.table(
    name="gold_stg_netflixtitles"
)
def myfunc():
    df=spark.readStream.format("delta").load("abfss://silver@nexflixprojectarun.dfs.core.windows.net/netflix_titles")
    return df

# COMMAND ----------

@dlt.view
@dlt.expect_all_or_drop(looktables_rules)
def gold_trans_netflixtitles():
    df=spark.readStream.table("LIVE.gold_stg_netflixtitles")
    df = df.withColumn("type_flag", when(col("type") == "Movie", 1).otherwise(0))

    return df
