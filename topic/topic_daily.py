# Databricks notebook source
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
# NOTE:同じレコードを別日に取り込んでいる可能性があるため、重複削除を行う
gps_table = 'adinte_datascience.adinte_analyzed_data.gps_contact'
df_gps_raw_data = spark.table(gps_table)\
        .filter(col('place').isin(peid_list))\
        .filter(col('competitor_id')==0)\
        .filter(col('date') == date)\
        .join(config,on=['place_id','competitor_id'],how='inner')\
        .select(['place_id','competitor_id','adid','date'])\
        .dropDuplicates()\
        .orderBy(['place_id','competitor_id','date'])
                                        
df_gps_raw_data.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT ver FROM adinte_datascience.envdev.adid_with_topic

# COMMAND ----------

# topicテーブルと結合する
topic_table = 'adinte_datascience.envdev.adid_with_topic'
df_topic = spark.table(topic_table)\
    .filter(col('ver')=='2025-01-01')\
    .filter(col('topic_number')=='10')\
    .filter(col('pattern')=='preference')\
    .drop(
        'date','ver','run_idf_id','run_lda_id',
        'partition_column_of_pattern',
        'partition_column_of_topic_number',
        'partition_column_of_topic_id')\
    .join(df_gps_raw_data,on=['adid'],how='inner')
df_topic.display()

# COMMAND ----------

# トピックを数え上げる
df_topic_count = df_topic\
    .groupBy(
        'place_id','competitor_id',
        'date','topic_id','pattern','topic_number',)\
    .agg(count('adid').alias('count'))
df_topic_count.display()

# COMMAND ----------

# パーティションを設定する
df_output = df_topic_count\
    .withColumn('year' ,col('date').substr(1,4))\
    .withColumn('month',col('date').substr(1,7))\
    .withColumn('dt'   ,col('date'))\
    .orderBy(['place_id','competitor_id','date','topic_id','pattern','topic_number'])

df_output.display()

# COMMAND ----------

# 出力
if False:
    output_path = COMMON_PATH+'output/topic/'
    df_output\
        .repartition(1)\
        .write\
        .mode('overwrite')\
        .option('header', 'True')\
        .option('partitionOverwriteMode','dynamic')\
        .partitionBy('year','month','dt',)\
        .parquet(output_path)

else:
    from typing import cast
    output_path = COMMON_PATH+'output/topic/'

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

