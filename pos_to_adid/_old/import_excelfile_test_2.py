# Databricks notebook source
# %pip install python-dotenv==1.0.1
%pip install openpyxl

# COMMAND ----------

from azure.storage.blob import BlobServiceClient
import pandas as pd
from io import BytesIO
import os
from dotenv import load_dotenv
import openpyxl


# COMMAND ----------

# 1日単位で回遊計算を出力する
# date = dbutils.widgets.get("DATE")
# date = '2025-05-01'
# if date == 'Now':
#     dt = datetime.now() + timedelta(hours=9) - relativedelta(days=8)
#     date = dt.strftime('%Y-%m-%d')
# else:
#     None

# COMMAND ----------

# 最終出力パス
common_path = 'dbfs:/mnt/adintedataexplorer_ml-datastore/powerbi/custom_order/olympic_gps/'


# COMMAND ----------

start_date = '2025-05-01'
end_date   = '2025-05-31'

# COMMAND ----------

# ショップ情報の取得
path = '/dbfs/mnt/adintedataexplorer_ml-datastore/powerbi/custom_order/olympic/'
shop_file = 'info/shoplist/shoplist.csv'
df_shoplist = pd.read_csv(path + shop_file)
df_shoplist.display()

# COMMAND ----------

# Azure Blob Storageの接続情報
storage_account_name = "adintedataexplorer"
container_name = "ml-datastore"
prefix = "powerbi/custom_order/olympic_gps/顧客分析データ抽出/"
# blob_name = "powerbi/custom_order/olympic_gps/顧客分析データ抽出/test.xlsx"

# .envファイルからSECRET_ACCESS_KEYを読み込む
load_dotenv()
access_key = os.getenv('SECRET_ACCESS_KEY')

# COMMAND ----------

# BlobServiceClientの作成
blob_service_client = BlobServiceClient(
    account_url=f"https://{storage_account_name}.blob.core.windows.net",
    credential=access_key,
)
# ContainerClientの取得
container_client = blob_service_client.get_container_client(container_name)


# COMMAND ----------

# ベースデータフレームの作成
df_base = pd.DataFrame()

# COMMAND ----------

# Blob内のファイルのリスト
blob_filelist = container_client.list_blobs(name_starts_with=prefix)
for file in blob_filelist:
    print(file.name)
    # BlobClientの取得
    blob_client = container_client.get_blob_client(file.name)
    # Blobのダウンロード
    blob_data = blob_client.download_blob().readall()
    # Pandasでエクセルファイルを読み込む
    excel_data = pd.read_excel(BytesIO(blob_data), header=0)
    # 購入日がdateと一致するものだけをフィルタリングする
    # excel_data = excel_data[excel_data['購入日'] == date.replace('-','')]
    # データフレームの表示
    # print(excel_data.head(2))
    # display(excel_data)
    # データフレームを結合
    df_base = pd.concat([df_base, excel_data], ignore_index=True,axis=0)
print(df_base.head(3))
print(df_base.tail(3))


# COMMAND ----------

df_base_renamed = df_base.rename(columns={'店舗': 'shop_code','購入日時':'datetime'})
display(df_base_renamed.head(3))
display(df_base_renamed.tail(3))

# COMMAND ----------

# 店舗番号とplace_idとを突合させる
df_data = df_shoplist.merge(df_base_renamed, on='shop_code', how='inner')
df_pos_data = spark.createDataFrame(df_data)
display(df_pos_data.head(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ### gpsデータとの突合

# COMMAND ----------

from pyspark.sql.functions import col, expr

# COMMAND ----------

# place_idのリスト
place_id_list = df_shoplist['place_id'].to_list()
print(place_id_list)

# COMMAND ----------

diff_time = "INTERVAL 3 MINUTES"


# COMMAND ----------

gps_table = 'adinte_datascience.adinte_analyzed_data.gps_contact'
df_adid = spark.table(gps_table)\
  .filter(col('place').isin(place_id_list))\
  .filter(col('competitor_id') == 0)\
  .filter(col('date') >= start_date)\
  .filter(col('date') <= end_date)\
  .withColumn("begin_time", expr(f"datetime - {diff_time}"))\
  .withColumn("end_time", expr(f"datetime + {diff_time}"))\
  .select(
    'user_id','place_id','competitor_id','adid',
    'date','datetime','begin_time','end_time'
    )
display(df_adid.head(3))

# COMMAND ----------

df_adid_to_pos = df_adid\
  .join(
    df_pos_data, 
    [df_pos_data.datetime >= df_adid.begin_time, 
     df_pos_data.datetime <= df_adid.end_time,
     df_pos_data.place_id == df_adid.place_id])
display(df_adid_to_pos.head(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 属性分布

# COMMAND ----------

from pyspark.sql.functions import count

# COMMAND ----------

# 属性分布の突合
table_agegender = 'adinte_datascience.adinte_analyzed_data.master_relationalagegender'
df_pos_to_agegender = spark.table(table_agegender)\
  .filter(col('start_date')==start_date)\
  .join(
    df_adid_to_pos,on='adid',how='inner'
    )
display(df_pos_to_agegender.head(3))

# COMMAND ----------

# 日付・place_id・年齢・性別・商品メニューごとにadidを集計
df_count_agegender = df_pos_to_agegender\
  .groupBy('user_id','place_id','date','caption','shop_code','age','gender')\
  .agg(count('adid').alias('count'))
display(df_count_agegender.head(3))

# COMMAND ----------

# year,month,dtでpartitionを作成
df_count_agegender_output = df_count_agegender\
    .withColumn('year' , expr("substring(date,1,4)"))\
    .withColumn('month', expr("substring(date,1,7)"))\
    .withColumn('dt'   , col('date'))\
    .orderBy(['shop_code','date','age','gender','caption'])
display(df_count_agegender_output.head(3))

# COMMAND ----------

# 出力
output_agegender_path = common_path + 'output/gps_to_pos/agegender/'
df_count_agegender_output\
    .repartition(1)\
    .write\
    .mode('overwrite')\
    .option('header','true')\
    .option('partitinOverwriteMode','dynamic')\
    .partitionBy('year','month')\
    .parquet(output_agegender_path)


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
display(df_topic.head(3))

# COMMAND ----------

# トピックを数え上げる
df_topic_count = df_topic\
    .groupBy(
        'user_id','place_id','competitor_id','date',
        'caption','shop_code',
        'topic_id','pattern','topic_number')\
    .agg(count('adid').alias('count'))

# COMMAND ----------

# パーティションを設定する
df_topic_output = df_topic_count\
    .withColumn('year' , expr("substring(date,1,4)"))\
    .withColumn('month', expr("substring(date,1,7)"))\
    .withColumn('dt'   , col('date'))
df_topic_output.display()

# COMMAND ----------

# 出力
output_topic_path = common_path + 'output/gps_to_pos/topic/'
df_topic_output\
    .repartition(1)\
    .write\
    .mode('overwrite')\
    .option('header','true')\
    .option('partitinOverwriteMode','dynamic')\
    .partitionBy('year','month')\
    .parquet(df_topic_output)


# COMMAND ----------

# MAGIC %md
# MAGIC ### リピーター

# COMMAND ----------

# 日別adidをユニークで取得する
table = 'adinte_analyzed_data.gps_contact'
df_input = spark.table(table)\
    .filter(F.col('place').isin(place_id_list))\
    .filter(F.col('date') >= start_date)\
    .filter(F.col('date') <= end_date)\
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

# 突合のため、列名を変更する
df_adid_to_pos_tmp = df_adid_to_pos\
        .withColumnRenamed('date','start_date_of_agg')
# posと　adidを突合させたデータを取得する
df_repeater_pos = df_input_visited_30days\
    .join(df_adid_to_pos_tmp,['adid','start_date_of_agg'],'inner')
df_repeater_pos.display()