# Databricks notebook source
import pandas as pd
import os

import datetime
from datetime import datetime, timedelta

import pyspark.sql.functions as F
from pyspark.sql.functions import count
from pyspark.sql import Window
from pyspark.sql.functions import col, expr

# COMMAND ----------



# COMMAND ----------

# common_path = 'dbfs:/mnt/adintedataexplorer_ml-datastore/powerbi/custom_order/olympic_gps/'
common_path = 'abfss://ml-datastore@adintedataexplorer.dfs.core.windows.net/powerbi/custom_order/'

intermediate_path = common_path + 'olympic_gps/intermediate/'
output_path = common_path + 'olympic_gps/output/gps_to_pos/'
# abfss://ml-datastore@adintedataexplorer.dfs.core.windows.net/


# COMMAND ----------

start_date = '2025-05-01'
end_date   = '2025-05-31'

# COMMAND ----------

# ショップ情報の取得
# path = '/dbfs/mnt/adintedataexplorer_ml-datastore/powerbi/custom_order/olympic/'
shop_file = 'info/shoplist/shoplist.csv'
# df_shoplist = pd.read_csv(common_path + shop_file)
df_shoplist = spark.read.csv(common_path +'olympic/'+ shop_file, header=True)
df_shoplist.display()

# COMMAND ----------

# adid_to_posデータの取得
df_adid_to_pos = spark.read.parquet(intermediate_path)
df_adid_to_pos.display()

# COMMAND ----------

# place_idのリスト
place_id_list = df_adid_to_pos.select('place_id').distinct()\
    .toPandas()['place_id'].to_list()
print(place_id_list)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 推定来訪者数

# COMMAND ----------

param = 2

# COMMAND ----------

# 日付・place_id・地域・商品メニューごとにadidを集計
# 集計数10以上を抽出
df_count_visitor = df_adid_to_pos\
    .dropDuplicates()\
    .groupBy(
        'user_id','place_id','date','caption','shop_code','item_name',
    )\
    .agg(count('adid').alias('count'))\
    .filter(col('count')>=param)
display(df_count_visitor)
# print(f"行数: {df_adid_to_pos.count()}")
# print(f"行数: {df_adid_to_pos.dropDuplicates().count()}")
    # .dropDuplicates()\



# COMMAND ----------

# year,month,dtでpartitionを作成
df_count_visitor_output = df_count_visitor\
    .withColumn('place', col('place_id'))\
    .withColumn('year' , expr("substring(date,1,4)"))\
    .withColumn('month', expr("substring(date,1,7)"))\
    .withColumn('dt'   , col('date'))\
    .orderBy([
        'shop_code','date','item_name'])
display(df_count_visitor_output)
print(f"行数: {df_count_visitor_output.count()}")


# COMMAND ----------

# 出力
output_visitor_path = output_path + f'visitor/'
df_count_visitor_output\
    .repartition(1)\
    .write\
    .mode('overwrite')\
    .option('header','true')\
    .option('partitinOverwriteMode','dynamic')\
    .partitionBy('place','year','month','dt')\
    .parquet(output_visitor_path)

# COMMAND ----------

