# Databricks notebook source
# MAGIC %md
# MAGIC トピック番号毎のコホート追加（日別）

# COMMAND ----------

from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
import time
import os
import shutil

import numpy as np
import pandas as pd

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, expr, count, concat
from pyspark.sql import types as T
from pyspark.sql.types import StringType, IntegerType
import pyspark.sql.functions as F

# COMMAND ----------

input_filename = getArgument("input_filename")
project_name = getArgument("project_name")
date = getArgument("date")

# COMMAND ----------

COMMON_PATH = f"abfss://ml-datastore@adintedataexplorer.dfs.core.windows.net/powerbi/custom_order/{project_name}/"

# COMMAND ----------

# 設定ファイル取得する
config = (
    (
        spark.read.option("inferSchema", "False")
        .option("header", "True")
        .csv(COMMON_PATH + input_filename)
    )
    .select(["user_id", "place_id", "competitor_id"])
    .withColumn("competitor_id", col("competitor_id").cast(IntegerType()))
    .dropDuplicates()
    .orderBy(["user_id", "place_id", "competitor_id"])
)
urid_list = sorted(config.select("user_id").distinct().toPandas()["user_id"].to_list())
peid_list = sorted(
    config.select("place_id").distinct().toPandas()["place_id"].to_list()
)
# comp_list = sorted(
#     config.select("competitor_id").distinct().toPandas()["competitor_id"].to_list()
# )
config.display()

# COMMAND ----------

# gpsデータを取得
gps_table = "adinte_datascience.adinte_analyzed_data.gps_contact"
df_raw_data = (
    spark.table(gps_table)
    .filter(col("place").isin(peid_list))
    .filter(col('competitor_id')==0)
    .filter(col("date") == date)
    .select(["place_id", "competitor_id", "adid", "date"])
    .dropDuplicates()
    .orderBy(["place_id", "competitor_id", "date"])
)

df_raw_data.display()

# COMMAND ----------

# cohortテーブルと結合する
cohort_table = "adinte_datainfrastructure.master.relational_cohortlist"
df_cohort = (
    spark.table(cohort_table)
    .filter(col("visit_category") == "興味関心")
    .join(df_raw_data, on=["adid"], how="inner")
    .filter(col("start_date") <= col("date"))
    .filter(col("end_date") >= col("date"))
)
df_cohort.display()

# COMMAND ----------

# リストを解除する
df_cohort_exploded = df_cohort.withColumn("cohort", F.explode("cohortlist"))
df_cohort_exploded.display()

# COMMAND ----------

# MAGIC %md
# MAGIC キーワードをくっつける

# COMMAND ----------

# keyword_table = 'adinte_datainfrastructure.master.navit_business_keywords'
# df_keyword = spark.table(keyword_table)
# df_cohort_to_keyword = df_cohort_exploded\
#     .join(df_keyword, df_cohort_exploded.cohort == df_keyword.BUSINESS_NAME_S, 'inner')\
#     .withColumn("keyword", F.explode("KEYWORDS"))\
#     .withColumnRenamed('BUSINESS_NAME_L','cohort_L')\
#     .withColumnRenamed('BUSINESS_NAME_M','cohort_M')\
#     .select(['adid','visit_category',
#              'place_id','competitor_id','date',
#              'cohort_L','cohort_M','cohort','keyword'])
# df_cohort_to_keyword.display()

# COMMAND ----------

# MAGIC %md
# MAGIC トピックテーブルを抽出する

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT
# MAGIC   ver
# MAGIC FROM
# MAGIC   adinte_datascience.envdev.adid_with_topic

# COMMAND ----------

# cohort-adidのデータをtopicテーブルと結合させる
topic_table = "adinte_datascience.envdev.adid_with_topic"
df_topic_to_cohort = (
    spark.table(topic_table)
    .filter(col("ver") == "2025-01-01")
    .filter(col("topic_number") == "10")
    .filter(col("pattern") == "preference")
    .drop(
        "date",
        "ver",
        "exp_id",
        "run_idf_id",
        "run_lda_id",
        "partition_column_of_pattern",
        "partition_column_of_topic_number",
    )
    .join(df_cohort_exploded, on=["adid"], how="inner")
)
df_topic_to_cohort.display()

# COMMAND ----------

# トピックを数え上げる
df_count = df_topic_to_cohort.groupBy(
    "place_id",
    "competitor_id",
    "date",
    "visit_category",
    "cohort",
    "topic_id",
    "pattern",
    "topic_number",
).agg(count("adid").alias("count"))
df_count.display()

# COMMAND ----------

# countが9以下を削除する
df_count_tmp = df_count.filter(col("count") > 9)

# COMMAND ----------

# パーティションを設定する
df_output = (
    df_count_tmp.withColumn("year", col("date").substr(1, 4))
    .withColumn("month", col("date").substr(1, 7))
    .withColumn("dt", col("date"))
    .orderBy(["place_id", "competitor_id", "date", "visit_category", "cohort"])
)

df_output.display()

# COMMAND ----------

# 出力
if False:
    output_path = COMMON_PATH + "output/topic_to_cohort/"
    df_output.repartition(1).write.mode("overwrite").option("header", "True").option(
        "partitionOverwriteMode", "dynamic"
    ).partitionBy("year", "month", "dt",).parquet(output_path)

else:
    from typing import cast
    output_path = COMMON_PATH + "output/topic_to_cohort/"

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

