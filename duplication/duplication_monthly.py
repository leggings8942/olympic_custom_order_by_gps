# Databricks notebook source
input_filename = getArgument("input_filename")
project_name   = getArgument("project_name")
print(project_name,input_filename)

# COMMAND ----------

import datetime
from dateutil.relativedelta import relativedelta
import time
import os
import shutil
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from pyspark.sql import DataFrame
from pyspark.sql.functions import col,lit,when,expr,count,concat
from pyspark.sql import types as T
from pyspark.sql.types import StringType, IntegerType
import pyspark.sql.functions as F


# COMMAND ----------

COMMON_PATH = f'abfss://ml-datastore@adintedataexplorer.dfs.core.windows.net/powerbi/custom_order/{project_name}/'
print(COMMON_PATH)

# COMMAND ----------

# 日付を設定する
dt = datetime.datetime.now(tz=ZoneInfo('Asia/Tokyo'))- relativedelta(days=8)
end = dt.strftime('%Y-%m-%d')

# COMMAND ----------

# 設定ファイル取得する
inputfile = COMMON_PATH+input_filename
df_config = (spark.read\
        .option('inferSchema', 'False')
        .option('header', 'True')
        .csv(inputfile))\
        .select(['place_id','user_id','competitor_id','start_date'])
df_config.display()
place_list = df_config.select('place_id').distinct().toPandas()['place_id'].to_list()
# place_list

# COMMAND ----------

# gpsデータを取得
# NOTE:同じレコードを別日に取り込んでいる可能性があるため、重複削除を行う
gps_table = 'adinte_datainfrastructure.external.gps_contact'
df_raw_data = spark.table(gps_table)\
    .filter(F.col('place').isin(place_list))\
    .filter(col('date')<=end)\
    .withColumn('year_month',F.substring('datetime',1,7))\
    .join(df_config,on=['place_id','competitor_id'],how='inner')\
    .filter(col('date')>=col('start_date'))\
    .dropDuplicates(['place_id','competitor_id','adid','year_month'])\
    .select(['place_id','competitor_id','adid','year_month'])\
    .orderBy(['place_id','competitor_id','year_month'])
                                        
df_raw_data.display()

# COMMAND ----------

# adidユニーク数を集計する
df_adid_count = df_raw_data\
    .drop_duplicates()\
    .groupBy('place_id','competitor_id','year_month')\
    .agg(count('adid').alias('own_count'))\
    .orderBy('place_id','competitor_id','year_month')
df_adid_count.display()

# COMMAND ----------

# 重複比較用のデータを作成する
df_comp_data = df_raw_data\
    .withColumn('duplication_place_id',col('place_id'))\
    .withColumn('duplication_competitor_id',col('competitor_id'))\
    .drop('place_id','competitor_id')
df_comp_data.display()

# COMMAND ----------

# adidとyear_monthで突合して、重複データを削除する
df_duplication = df_raw_data\
    .join(df_comp_data,on=['adid','year_month'],how='inner')\
    .filter(
            (col('place_id')!= col('duplication_place_id')) | (col('competitor_id')!= col('duplication_competitor_id'))
        )\
    .drop_duplicates(['place_id','competitor_id','adid','year_month','duplication_place_id','duplication_competitor_id'])\
    .orderBy('place_id','competitor_id','year_month')
df_duplication.display()

# COMMAND ----------

# 重複数を集計する
df_duplication_count = df_duplication\
    .groupBy('year_month','place_id','competitor_id','duplication_place_id','duplication_competitor_id')\
    .agg(count('adid').alias('duplication_count'))\
    .orderBy('place_id','competitor_id','year_month')
df_duplication_count.display()

# COMMAND ----------

# 自身カウント数と重複数カウント数とを結合する
df_duplication_output = df_duplication_count\
    .join(df_adid_count,on=['place_id','competitor_id','year_month'],how='inner')\
    .orderBy(
        'place_id','competitor_id',
        'duplication_place_id','duplication_competitor_id','year_month')
df_duplication_output.display()

# COMMAND ----------

# パーティション(place)を設定する
df_duplication_output = df_duplication_output\
    .withColumn('place',col('place_id'))
df_duplication_output.display()

# COMMAND ----------

# 出力(集計重複数：日次)
if False:
    df_duplication_output\
        .repartition(1)\
        .write\
        .mode('overwrite')\
        .option('header', 'True')\
        .option('partitionOverwriteMode','dynamic')\
        .partitionBy('place')\
        .csv(COMMON_PATH+f'output/duplication/monthly/')

else:
    from typing import cast
    output_path = COMMON_PATH+f'output/duplication/monthly/'

    # DeltaLake形式での解析結果の出力
    df_output = cast(DataFrame, df_duplication_output)
    df_output.drop('place')\
                .write\
                .format('delta')\
                .mode('overwrite')\
                .option('encoding',               'UTF-8')\
                .option('compression',            'snappy')\
                .option('partitionOverwriteMode', 'dynamic')\
                .partitionBy(['place_id'])\
                .save(output_path)

