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

# 下限値設定ファイルの取得
inf_file = 'input/olympic_pos_infimum.csv'
df_infimum = spark.read.csv(common_path +'olympic_gps/'+ inf_file, header=True)
df_infimum.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 実際の処理

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
# MAGIC ### 顧客行動テーマ

# COMMAND ----------

# topicテーブルと結合する
topic_table = 'adinte_datascience.envdev.adid_with_topic'
df_topic = spark.table(topic_table)\
    .filter(col('ver')=='2025-01-01')\
    .filter(col('topic_number')=='10')\
    .drop('date','ver')\
    .join(df_adid_to_pos,on=['adid'],how='inner')
display(df_topic)


# COMMAND ----------

# トピックを数え上げる
df_topic_count = df_topic\
    .groupBy(
        'user_id','place_id','competitor_id','date',
        'caption','shop_code','item_name',
        'topic_id','pattern','topic_number')\
    .agg(count('adid').alias('count'))


# COMMAND ----------

# 集計数5以上を抽出
df_count_topic_inf = df_topic_count\
    .join(
        df_infimum,on=['caption','place_id'],how='inner')\
    .filter(col('count')>=col('infimum'))\
    .drop('infimum')
display(df_count_topic_inf)


# COMMAND ----------

# パーティションを設定する
df_topic_output = df_count_topic_inf\
    .withColumn('place', col('place_id'))\
    .withColumn('year' , expr("substring(date,1,4)"))\
    .withColumn('month', expr("substring(date,1,7)"))\
    .withColumn('dt'   , col('date'))\
    .orderBy(['shop_code','date','topic_id','item_name'])
df_topic_output.display()
print(f"行数: {df_topic_output.count()}")

# COMMAND ----------

# 出力
output_topic_path = output_path + 'topic/'
df_topic_output\
    .repartition(5)\
    .write\
    .mode('overwrite')\
    .option('header','true')\
    .option('partitinOverwriteMode','dynamic')\
    .partitionBy('place','year','month','dt')\
    .parquet(output_topic_path)


# COMMAND ----------

