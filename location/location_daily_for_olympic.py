# Databricks notebook source
# いったん日別で出す

# COMMAND ----------

from datetime import datetime, timedelta,date
from dateutil.relativedelta import relativedelta
import time
import os
import shutil

import numpy as np
import pandas as pd

from pyspark.sql import DataFrame
from pyspark.sql.functions import col,lit,when,expr,count,concat
from pyspark.sql import types as T
from pyspark.sql.types import StringType, IntegerType
import pyspark.sql.functions as F

# COMMAND ----------

input_filename = getArgument("input_filename")
project_name   = getArgument("project_name")
date = getArgument("date")

# COMMAND ----------

COMMON_PATH = f'abfss://ml-datastore@adintedataexplorer.dfs.core.windows.net/powerbi/custom_order/{project_name}/'
print(COMMON_PATH)

# COMMAND ----------

# 設定ファイル取得する
config = (spark.read\
        .option('inferSchema', 'False')
        .option('header', 'True')
        .csv(COMMON_PATH + input_filename))\
        .select(['user_id', 'place_id','competitor_id'])\
        .withColumn('competitor_id', col('competitor_id').cast(IntegerType()))\
        .dropDuplicates()\
        .orderBy(['user_id', 'place_id','competitor_id'])
urid_list = sorted(config.select('user_id' ).distinct().toPandas()['user_id'].to_list())
peid_list = sorted(config.select('place_id').distinct().toPandas()['place_id'].to_list())
# comp_list = sorted(config.select('competitor_id').distinct().toPandas()['competitor_id'].to_list())
config.display()


# COMMAND ----------

# gpsデータを取得
gps_table = 'adinte_datascience.adinte_analyzed_data.gps_contact'
df_raw_data = spark.table(gps_table)\
                    .filter(col('place').isin(peid_list))\
                    .filter(col('competitor_id')==0)\
                    .filter(col('date')==date)\
                    .select(['place_id','competitor_id','adid','date'])\
                    .dropDuplicates()\
                    .orderBy(['place_id','competitor_id','date'])
                                        
df_raw_data.display()

# COMMAND ----------

# 1都4県に絞る
prefecture_list = ['東京都','神奈川県','埼玉県','群馬県','千葉県']

# COMMAND ----------

# 居住地勤務地マスタテーブルを取得しフィルタリング
location_table = 'adinte_datainfrastructure.master.relational_address_jp'
df_location = spark.table(location_table)\
    .filter(col('prefecture').isin(prefecture_list))\
    .join(df_raw_data,on=['adid'],how='inner')\
    .filter(col('date')<=col('end_date'))\
    .filter(col('date')>=col('start_date'))\
    .drop('latitude','longitude','area','population','household')
    
df_location.display()

# COMMAND ----------

# 日別でカウントを行う
df_location_count_daily = df_location\
    .groupBy(['date','place_id','competitor_id','time_category','prefecture','city','block'])\
    .agg(count('adid').alias('count'))
df_location_count_daily.display()

# COMMAND ----------

df_location_count_daily.count()

# COMMAND ----------

# count<=1を間引く
df_location_count_daily_filtered = df_location_count_daily.filter(col('count')>1)
df_location_count_daily_filtered.count()

# COMMAND ----------

# year,month,dtでパーティションする
df_output = df_location_count_daily_filtered\
    .withColumn('year',col('date').substr(1,4))\
    .withColumn('month',col('date').substr(1,7))\
    .withColumn('dt'   ,col('date'))\
    .orderBy('place_id','competitor_id','date','time_category','prefecture','city','block')
df_output.display()

# COMMAND ----------

# 出力
if False:
    output_path = COMMON_PATH+'output/location/'
    df_output.repartition(1)\
        .write\
        .mode('overwrite')\
        .option('header','true')\
        .option('partitionOverwriteMode','dynamic')\
        .partitionBy('year','month','dt')\
        .parquet(output_path)

else:
    from typing import cast
    output_path = COMMON_PATH+'output/location/'

    # DeltaLake形式での解析結果の出力
    df_output = cast(DataFrame, df_output)
    df_output.drop('year','month','dt')\
                .write\
                .format('delta')\
                .mode('overwrite')\
                .option('encoding',               'UTF-8')\
                .option('compression',            'snappy')\
                .option('partitionOverwriteMode', 'dynamic')\
                .partitionBy(['date'])\
                .save(output_path)

# COMMAND ----------

