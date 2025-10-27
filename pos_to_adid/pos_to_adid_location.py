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
# MAGIC ### 居住地勤務地

# COMMAND ----------

table_location = 'adinte_datainfrastructure.master.relational_address_jp'
df_pos_to_location = spark.table(table_location)\
    .filter(col('start_date')==start_date)\
    .join(
        df_adid_to_pos,on='adid',how='inner'
        )
display(df_pos_to_location)


# COMMAND ----------

# 日付・place_id・地域・商品メニューごとにadidを集計
df_count_location = df_pos_to_location\
    .groupBy(
        'user_id','place_id','date','caption','shop_code','item_name',
        'time_category','prefecture','city','block')\
    .agg(count('adid').alias('count'))
display(df_count_location)


# COMMAND ----------

# 集計数5以上を抽出
df_count_loc_inf = df_count_location\
    .join(
        df_infimum,on=['caption','place_id'],how='inner')\
    .filter(col('count')>=col('infimum'))\
    .drop('infimum')
display(df_count_loc_inf)


# COMMAND ----------

# year,month,dtでpartitionを作成
df_count_location_output = df_count_loc_inf\
    .withColumn('place', col('place_id'))\
    .withColumn('year' , expr("substring(date,1,4)"))\
    .withColumn('month', expr("substring(date,1,7)"))\
    .withColumn('dt'   , col('date'))\
    .orderBy([
        'shop_code','date','time_category','prefecture','city','block','item_name'])
display(df_count_location_output)
print(f"行数: {df_count_location_output.count()}")


# COMMAND ----------

# 出力
output_location_path = output_path + 'location/'
df_count_location_output\
    .repartition(5)\
    .write\
    .mode('overwrite')\
    .option('header','true')\
    .option('partitinOverwriteMode','dynamic')\
    .partitionBy('place','year','month','dt')\
    .parquet(output_location_path)

# COMMAND ----------

