# Databricks notebook source
import json
from pyspark.sql.functions import col,lit,when,count
from pyspark.sql.types import StringType, IntegerType,FloatType


# COMMAND ----------

start_date = dbutils.widgets.get('START_DATE')
input_path = dbutils.widgets.get('INPUT_PATH')
folder_name = dbutils.widgets.get('FOLDER_NAME')

# start_idx = int(dbutils.widgets.get('S_IDX'))
# end_idx   = int(dbutils.widgets.get('E_IDX'))


# COMMAND ----------

# プレイスをリストで取得する
COMMON_PATH = f'abfss://ml-datastore@adintedataexplorer.dfs.core.windows.net/powerbi/custom_order/{folder_name}/moving/'

config = (spark.read\
        .option('inferSchema', 'False')
        .option('header', 'True')
        .csv(COMMON_PATH + input_path))\
        .select(['user_id', 'place_id','competitor_id'])\
        .withColumn('competitor_id', col('competitor_id').cast(IntegerType()))\
        .dropDuplicates()\
        .orderBy(['user_id', 'place_id','competitor_id'])
peid_list = sorted(config.select('place_id').distinct().toPandas()['place_id'].to_list())
print(len(peid_list))

# COMMAND ----------

json_list = [
    {
    "PLACE_ID":   place,
    "START_DATE": start_date,
    "INPUT_PATH": input_path,
    "FOLDER_NAME":folder_name,
    }
    for place in peid_list
    ]

# COMMAND ----------

target_json = json.dumps(json_list, indent=4)

# COMMAND ----------

dbutils.jobs.taskValues.set('target_json', target_json)