# Databricks notebook source
spot_table = 'adinte_datainfrastructure.master.relational_spot_jp'
df_spot_with_place = spark.table(spot_table).select("start_date").distinct()
display(df_spot_with_place)

# COMMAND ----------

