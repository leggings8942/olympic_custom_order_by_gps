# Databricks notebook source
# MAGIC %md
# MAGIC ### 出力内容
# MAGIC - 居住地勤務地
# MAGIC - 顧客行動テーマ
# MAGIC - 顧客行動テーマ別コホート
# MAGIC - 行動調査(別タスクで実行する)

# COMMAND ----------

from datetime import datetime, timedelta,date
from dateutil.relativedelta import relativedelta

# COMMAND ----------

# input_filename = dbutils.widgets.get("INPUT_FILE_NAME")
# project_name   = dbutils.widgets.get("PROJECT_NAME")

# 1日単位で回遊計算を出力する
date = dbutils.widgets.get("DATE")
if date == 'Now':
    dt = datetime.now() + timedelta(hours=9) - relativedelta(days=8)
    date = dt.strftime('%Y-%m-%d')
else:
    None

# COMMAND ----------

timeout = 60*60
param_dict = {
  'input_filename' :dbutils.widgets.get("INPUT_FILE_NAME"),
  'project_name'   :dbutils.widgets.get("PROJECT_NAME"),
  'date'           :date
  }
print(param_dict)

# COMMAND ----------

# MAGIC %md
# MAGIC #### ノートブック実行

# COMMAND ----------

# 居住地勤務地
dbutils.notebook.run('location/location_daily_for_olympic', timeout, param_dict)
# 顧客行動テーマ
dbutils.notebook.run('topic/topic_daily', timeout, param_dict)
# コホート（とトピック）
dbutils.notebook.run('cohort/cohort_by_topic_daily', timeout, param_dict)
# 行動調査（出力停止）
# dbutils.notebook.run('moving/dev_moving_daily', timeout, param_dict)