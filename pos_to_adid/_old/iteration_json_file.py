# Databricks notebook source
# MAGIC %md
# MAGIC ### 過去データ出力用iteration

# COMMAND ----------

import json
import pandas as pd
from pyspark.sql.functions import col,lit,when,count
from pyspark.sql.types import StringType, IntegerType,FloatType


# COMMAND ----------

start_date = dbutils.widgets.get('START_DATE')
end_date   = dbutils.widgets.get('END_DATE')
# input_filename = dbutils.widgets.get("INPUT_FILE_NAME")
# project_name   = dbutils.widgets.get("PROJECT_NAME")

# is_duplication   = dbutils.widgets.get("IS_DUPLICATION")
# input_duplication_filename = dbutils.widgets.get("INPUT_DUPLICATION_FILE_NAME")


# COMMAND ----------

day_list = [d.strftime('%Y-%m-%d') for d in pd.date_range(start_date, end_date)]

# COMMAND ----------

json_list = [
    {
    "DATE": date,
    # "INPUT_FILE_NAME": input_filename,
    # "PROJECT_NAME":project_name,
    # "IS_DUPLICATION":is_duplication,
    # "INPUT_DUPLICATION_FILE_NAME":input_duplication_filename
    }
    for date in day_list
]    

# COMMAND ----------

target_json = json.dumps(json_list, indent=4)

# COMMAND ----------

dbutils.jobs.taskValues.set('target_json', target_json)