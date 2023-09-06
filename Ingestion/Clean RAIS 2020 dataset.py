# Databricks notebook source
# MAGIC %md
# MAGIC Step 1 - Set schema and load data

# COMMAND ----------

df_despesa=spark.read.csv("/mnt/rais/rais-2020/RAIS_VINC_PUB_CENTRO_OESTE.txt.gz", encoding='ISO-8859-1', header=True, sep=";")

# COMMAND ----------

display(df_despesa)

# COMMAND ----------

display(df_despesa.count())

# COMMAND ----------


