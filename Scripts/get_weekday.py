# Databricks notebook source
var = dbutils.jobs.taskValues.get(taskKey="Weekday_Lookup",key="weekoutput")

# COMMAND ----------

print(var)