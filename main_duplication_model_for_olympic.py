# Databricks notebook source
# MAGIC %md
# MAGIC ### 出力内容
# MAGIC - 重複調査（日次・週次・月次）

# COMMAND ----------

# input_filename = dbutils.widgets.get("INPUT_FILE_NAME")
input_filename = 'input/polygonList_Olympic_master.csv'
# project_name   = dbutils.widgets.get("PROJECT_NAME")
project_name = 'test_job/nagino/olympic_by_hirako'


# COMMAND ----------

timeout = 60*60
param_dict = {
  'input_filename' :input_filename,
  'project_name'   :project_name,
  }
print(param_dict)

# COMMAND ----------

# MAGIC %md
# MAGIC #### ノートブック実行

# COMMAND ----------

# 重複調査（日次）
dbutils.notebook.run('duplication/duplication_daily', timeout, param_dict)


# COMMAND ----------

# 重複調査（週次）
dbutils.notebook.run('duplication/duplication_weekly', timeout, param_dict)


# COMMAND ----------

# 重複調査（月次）
dbutils.notebook.run('duplication/duplication_monthly', timeout, param_dict)

# COMMAND ----------

