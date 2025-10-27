# Databricks notebook source
# MAGIC %md
# MAGIC AdIDとPOSデータの紐づけについて、POSデータと紐づいたAdIDがユニークで何件くらいあるかわかったりしますでしょうか？

# COMMAND ----------

# MAGIC %pip install python-dotenv==1.0.1
# MAGIC %pip install openpyxl

# COMMAND ----------

from azure.storage.blob import BlobServiceClient
import pandas as pd
from io import BytesIO
import os
from dotenv import load_dotenv
import openpyxl


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

from pyspark.sql.functions import col, expr

# COMMAND ----------

df_adid_to_pos_out = spark.read\
    .parquet(intermediate_path+'place=*/year=*/month=*/dt=*/*.parquet', header=True)
df_adid_to_pos_out.display()

# COMMAND ----------

# POSデータと紐づいたユニークadidを数える
df_count = df_adid_to_pos_out\
    .select('user_id','place_id','competitor_id','date','caption','adid')\
    .dropDuplicates()\
    .groupBy('user_id','place_id','competitor_id','date','caption')\
    .count()\
    .orderBy('user_id','place_id','competitor_id','date','caption')
df_count.display()
# adid 

# COMMAND ----------

# # 中間生成物として出力する
# df_adid_to_pos_out\
#     .write\
#     .mode('overwrite')\
#     .option('header','true')\
#     .option('partitinOverwriteMode','dynamic')\
#     .partitionBy('place','year','month','dt')\
#     .parquet(intermediate_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### リピーター

# COMMAND ----------

import datetime
from datetime import datetime, timedelta

import pyspark.sql.functions as F
from pyspark.sql.functions import count
from pyspark.sql import Window

# COMMAND ----------

# 日別adidをユニークで取得する
table = 'adinte_analyzed_data.gps_contact'
df_input = spark.table(table)\
    .filter(col('place').isin(place_id_list))\
    .filter(col('date') >= start_date)\
    .filter(col('date') <= end_date)\
    .select('user_id','place_id','competitor_id','adid','date')\
    .dropDuplicates()

# COMMAND ----------

df_input.display()

# COMMAND ----------

#型変換。datatime -> string
df_input = df_input\
          .withColumn('start_date_of_agg', F.col('date').cast('string'))

#Lag 用にWindow を設定
Windowspec = Window\
            .partitionBy(['user_id', 'place_id', 'competitor_id','adid'])\
            .orderBy("start_date_of_agg")
#前回訪問日時を取得。初訪問なら0を入れる
df_input_2 = df_input\
    .withColumn('before', F.lag('start_date_of_agg', default='0').over(Windowspec))
df_input_2.display()

# COMMAND ----------

#interval 算出
#何日ぶりに来たか=interval
def calc_interval_udf():
  def calc_interval(date,before):
 
    if str(before) == '0':
      interval = '0'
    else:
      date_dt   = datetime.strptime(date, '%Y-%m-%d')
      before_dt = datetime.strptime(before, '%Y-%m-%d') 
      #
      interval_dt = date_dt - before_dt
      interval = interval_dt.days

    return interval
  return F.udf(calc_interval)

# COMMAND ----------

#3 interval=前回訪問と今回訪問との期間を算出
df_input_3 = df_input_2\
              .withColumn('interval', calc_interval_udf()(F.col('start_date_of_agg'),F.col('before')) )

# COMMAND ----------

#移動累計
window_ranges=[-10*86400,0]
#str to datetime
df_input_3 = df_input_3.withColumn("start_date_of_agg_date", F.to_date("start_date_of_agg"))

window_spec = Window\
              .partitionBy(['user_id', 'place_id', 'competitor_id','adid'])\
              .orderBy(F.expr("unix_date(start_date_of_agg_date)"))\
              .rangeBetween(-30, 0)
              #.rangeBetween(int(window_ranges[0]),int(window_ranges[1]))
              #.rowsBetween(Window.unboundedPreceding, 0)#全期間で累積
              #.orderBy(F.expr("unix_date(col_name)")).rangeBetween(-7, 0)
# 移動累積を計算
df_input_accumulation = df_input_3.withColumn("visit_count_in_30days", F.count('adid').over(window_spec))


# COMMAND ----------

df_input_accumulation.display()

# COMMAND ----------

# visit_count_in_30days =  1 → 新規顧客
# visit_count_in_30days => 2 → 再訪問回数n-1回
df_input_visited_30days = df_input_accumulation\
        .withColumn('visited_30days',F.col('visit_count_in_30days')-1)\
        .filter(F.col('visited_30days')<=15)
df_input_visited_30days.display()

# COMMAND ----------



# COMMAND ----------

# 突合のため、列名を変更する
df_adid_to_pos_tmp = df_adid_to_pos\
        .withColumnRenamed('date','start_date_of_agg')
# posと　adidを突合させたデータを取得する
df_repeater_pos = df_input_visited_30days\
    .join(df_adid_to_pos_tmp,['user_id','place_id','adid','start_date_of_agg'],'inner')
df_repeater_pos.display()

# COMMAND ----------

# 必要な情報だけ取り出す
df_repeater_pos = df_repeater_pos\
    .select(['date','user_id','place_id','visited_30days','caption','item_name','place'])\
    .groupBy(['date','user_id','place_id','visited_30days','caption','item_name','place'])\
    .count()
df_repeater_pos.display()

# COMMAND ----------

df_output = df_repeater_pos\
    .withColumn('year', F.lit(start_date[0:4]))\
    .withColumn('month', F.lit(start_date[0:7]))
df_output.display()

# COMMAND ----------

df_output.repartition(1)\
    .write\
    .mode('overwrite')\
    .option('header', 'True')\
    .option('partitionOverwriteMode', 'dynamic')\
    .partitionBy('place','year','month')\
    .parquet(output_path+'repeater/')

# COMMAND ----------

