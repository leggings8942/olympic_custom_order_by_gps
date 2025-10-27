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
# MAGIC ### 自身の数量を集計する
# MAGIC

# COMMAND ----------

# adidユニーク数を集計する
df_own_count = df_adid_to_pos\
    .groupBy('user_id','place_id','competitor_id','date',
             'shop_code','caption','item_name')\
    .agg(count('adid').alias('own_count'))\
    .orderBy('place_id','competitor_id','date','shop_code',)
df_own_count.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 重複調査

# COMMAND ----------

# 重複対象のプレイスの取得
gps_table = 'adinte_datascience.adinte_analyzed_data.gps_contact'
df_adid_comp = spark.table(gps_table)\
    .filter(col('place').isin(place_id_list))\
    .filter(col('competitor_id') != 0)\
    .filter(col('date') >= start_date)\
    .filter(col('date') <= end_date)\
    .withColumn('duplication_place_id',col('place_id'))\
    .withColumn('duplication_competitor_id',col('competitor_id'))\
    .select(
    'user_id','place_id','adid',
    'date','datetime',
    'duplication_place_id','duplication_competitor_id'
    )
display(df_adid_comp)

# COMMAND ----------

# 重複対象のプレイスとの突合
df_pos_to_comp = df_adid_comp\
    .join(
        df_adid_to_pos,
        on  = ['user_id','place_id','adid','date'],
        how = 'inner')
display(df_pos_to_comp)

# COMMAND ----------

# 日付・place_id・重複プレイス・商品メニューごとにadidを集計
df_count_duplication = df_pos_to_comp\
.groupBy(
    'user_id','place_id','competitor_id',
    'date','caption','shop_code','item_name',
    'duplication_place_id','duplication_competitor_id')\
.agg(count('adid').alias('count'))
display(df_count_duplication)

# COMMAND ----------

# 自身カウントデータと突合する
df_count_duplication_to_own = df_count_duplication\
    .join(
        df_own_count,
        on=['user_id','place_id','competitor_id','date','caption','shop_code','item_name'],
        how='inner')
df_count_duplication_to_own.display()

# COMMAND ----------

# 集計数5以上を抽出
df_count_duplication_inf = df_count_duplication_to_own\
    .join(
        df_infimum,on=['caption','place_id'],how='inner')\
    .filter(col('count')>=col('infimum'))\
    .drop('infimum')
display(df_count_duplication_inf)


# COMMAND ----------

# year,month,dtでpartitionを作成
df_count_duplication_output = df_count_duplication_inf\
    .withColumn('place', col('place_id'))\
    .withColumn('year' , expr("substring(date,1,4)"))\
    .withColumn('month', expr("substring(date,1,7)"))\
    .withColumn('dt'   , col('date'))\
    .orderBy(['shop_code','date','duplication_place_id','duplication_competitor_id','item_name'])
display(df_count_duplication_output)
print(f"行数: {df_count_duplication_output.count()}")

# COMMAND ----------

# 出力
output_duplication_path = output_path + 'duplication/'
df_count_duplication_output\
    .repartition(1)\
    .write\
    .mode('overwrite')\
    .option('header','true')\
    .option('partitinOverwriteMode','dynamic')\
    .partitionBy('place','year','month','dt')\
    .parquet(output_duplication_path)


# COMMAND ----------

