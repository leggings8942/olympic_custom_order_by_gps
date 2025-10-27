# Databricks notebook source
# MAGIC %pip install spark-excel

# COMMAND ----------

from pyspark.sql import DataFrame
import pandas as pd

# COMMAND ----------

blob_path = '/dbfs/mnt/adintedataexplorer_ml-datastore/powerbi/custom_order/olympic_gps/'
file_path = '顧客分析データ抽出/'

# powerbi/custom_order/olympic_gps/顧客分析データ抽出/001_顧客分析データ抽出(小金井).xlsx

# COMMAND ----------

df = (spark.read.format("com.crealytics.spark.excel")
          .option("useHeader", "true")
          .option("inferSchema", "true")
          .load(blob_path+file_path+'001_顧客分析データ抽出(小金井).xlsx'))
display(df)

# COMMAND ----------

# pdf = pd.read_excel(blob_path+file_path+'001_顧客分析データ抽出(小金井).xlsx')
pdf = pd.read_csv(blob_path+'test.csv')
pdf

# COMMAND ----------

# Define the full path
full_path = blob_path + file_path

# List all files in the directory
files = dbutils.fs.ls(full_path)

display(pd.DataFrame([{"name": f.name, "path": f.path, "size": f.size} for f in files]))

# COMMAND ----------

# Read each Excel file into a DataFrame and combine them
df_list = []
for file in files:
    if file.name.endswith('.xlsx'):
        pdf = pd.read_excel(f"{file.path}")
        df = spark.createDataFrame(pdf)
        df_list.append(df)

# Combine all DataFrames into one
combined_df = df_list[0]
for df in df_list[1:]:
    combined_df = combined_df.unionByName(df)

display(combined_df)

# COMMAND ----------

