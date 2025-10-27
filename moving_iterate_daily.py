# Databricks notebook source
# 'adinte_datainfrastructure.master.relational_spot_jp'が月単位なので月別の行動量調査となる

# COMMAND ----------

import pandas as pd

import pyspark.sql.functions as F
from pyspark.sql.functions import col,lit,when,count
from pyspark.sql.types import StringType, IntegerType,FloatType


# COMMAND ----------

from datetime import datetime, timedelta,date
from dateutil.relativedelta import relativedelta

place_id    = dbutils.widgets.get('PLACE_ID')
input_path  = dbutils.widgets.get('INPUT_PATH')
folder_name = dbutils.widgets.get('FOLDER_NAME')
date        = dbutils.widgets.get('START_DATE') or (datetime.now() + timedelta(hours=9) - relativedelta(days=8)).strftime('%Y-%m-%d')
# end_date    = dbutils.widgets.get('END_DATE')
# dt = datetime.now() + timedelta(hours=9) - relativedelta(days=8)
# end_date = dt.strftime('%Y-%m-%d')

COMMON_PATH = f'abfss://ml-datastore@adintedataexplorer.dfs.core.windows.net/powerbi/custom_order/{folder_name}/'

# COMMAND ----------

# day_list = [d.strftime('%Y-%m-%d') for d in pd.date_range(start=start_date, end=end_date, freq='D')]
# print(day_list)

# COMMAND ----------



# COMMAND ----------

# 設定ファイル取得する
# input_path = COMMON_PATH + f'input/input_{folder_name}.csv'
config = (spark.read\
        .option('inferSchema', 'False')
        .option('header', 'True')
        .csv(COMMON_PATH + input_path))\
        .filter(col('place_id') == place_id)\
        .select(['user_id', 'place_id','competitor_id'])\
        .withColumn('competitor_id', col('competitor_id').cast(IntegerType()))\
        .dropDuplicates()\
        .orderBy(['user_id', 'place_id','competitor_id'])
urid_list = sorted(config.select('user_id' ).distinct().toPandas()['user_id'].to_list())
peid_list = sorted(config.select('place_id').distinct().toPandas()['place_id'].to_list())
comp_list = sorted(config.select('competitor_id').distinct().toPandas()['competitor_id'].to_list())
config.display()


# COMMAND ----------

gps_table = 'adinte_datascience.adinte_analyzed_data.gps_contact'
df_gps_raw_data = spark.table(gps_table)\
                    .filter(col('usr').isin(urid_list))\
                    .filter(col('place').isin(peid_list))\
                    .filter(col('competitor_id').isin(comp_list))\
                    .filter(col('date')==date)\
                    .select(['user_id', 'place_id', 'competitor_id','adid','date'])\
                    .dropDuplicates()
df_gps_raw_data.display()

# COMMAND ----------

# 1都3県に絞る
prefecture_list = ['東京都','神奈川県','埼玉県','群馬県','千葉県']

# COMMAND ----------

spot_table = 'adinte_datainfrastructure.master.relational_spot_jp'
start_date = date[0:7]+'-01'
df_spot_with_place = spark.table(spot_table)\
    .filter(col('start_date') == start_date)\
    .join(df_gps_raw_data, on=['adid'], how='inner')\
    .filter(col('start_date')<= col('date'))\
    .filter(col('end_date')  >= col('date'))\
    .select(['place_id','competitor_id','adid', 'spot', 'prefecture', 'city', 'address','date'])\
    .orderBy(['place_id','competitor_id','adid', 'date'])
df_spot_with_place.display()

# # SQLクエリを使用してデータを取得
# query = """
# FROM adinte_datainfrastructure.master.relational_spot_jp
# WHERE prefecture IN ('東京都','神奈川県','埼玉県','群馬県')
# """
# df_spot_with_place = spark.sql(query)
# df_spot_with_place = df_spot_with_place.join(df_gps_raw_data, on=['adid','start_date'], how='inner')\
#     .select(['place_id','competitor_id','adid', 'spot', 'prefecture', 'city', 'address','start_date', 'end_date'])\
#     .orderBy(['place_id','competitor_id','adid', 'start_date'])
# df_spot_with_place.display()

# COMMAND ----------

df_spot_with_place = df_spot_with_place\
    .filter(col('prefecture').isin(prefecture_list))
df_spot_with_place.display()

# COMMAND ----------

# 月別でspotや住所をカウントする
df_count = df_spot_with_place\
    .groupBy(['place_id','competitor_id','spot','prefecture','city','address', 'date'])\
    .agg(count('adid').alias('count'))\
    .orderBy(['place_id','competitor_id','spot','date'])
df_count.display()

# COMMAND ----------

# cohortリストを取得する
cohort_path = COMMON_PATH + 'input/filtering_cohort_list.csv'
# powerbi/custom_order/olympic/moving/input/filtering_cohort_list.csv
cohort_list = spark.read\
    .option('header', 'True')\
    .option('inferSchema', 'True')\
    .csv(cohort_path)\
    .toPandas()['カテゴリー'].to_list()
# cohort_list.display()
cohort_list

# COMMAND ----------

# spotとカテゴリを突合してみる
spot_category_table = 'adinte_datainfrastructure.master.navit_spotdata'
df_count_spot_with_place_category = spark.table(spot_category_table)\
    .filter(col('cohort_caption').isin(cohort_list))\
    .join(df_count, on=['spot','prefecture','city','address'], how='inner')\
    .drop('code','category_id','subcategory_id','cohort_id','keywords','tel','zip','latitude','longitude','bbox','WKT','source','type')\
    .orderBy(['place_id','competitor_id','cohort_caption','spot','date'])
df_count_spot_with_place_category.display()

# COMMAND ----------

# パーティションを作成する
df_output = df_count_spot_with_place_category\
    .withColumn('place', col('place_id'))\
    .withColumn('year' , F.date_format(col('date'), 'yyyy'))\
    .withColumn('month', F.date_format(col('date'), 'yyyy-MM'))\
    .withColumn('dt'   , col('date'))
df_output.display()

# COMMAND ----------

# 出力してみる
output_path = COMMON_PATH + 'output/moving/'
df_output.repartition(1)\
    .write\
    .mode('overwrite')\
    .option('header', 'true')\
    .option('partitionOverwriteMode','dynamic')\
    .partitionBy('place','year','month','dt')\
    .parquet(output_path)

# COMMAND ----------

