# Databricks notebook source
import pyspark.pandas as pd

# COMMAND ----------

from pyspark.sql.functions import split
from pyspark.sql.types import *

# COMMAND ----------

atividade=pd.read_excel('/mnt/rais2020/bronze/RAIS_vinculos_layout2020.xls', sheet_name="subclasse 2.0")

# COMMAND ----------

display(atividade)

# COMMAND ----------

atividade=atividade[atividade['CNAE 2.0 Subclas'].str.contains(":")]

# COMMAND ----------

atividade[["id_atividade","nome_atividade"]]=atividade["CNAE 2.0 Subclas"].str.split(":", n=2,expand=True)

# COMMAND ----------

display(atividade)

# COMMAND ----------

df_atividade=atividade[["id_atividade","nome_atividade"]]

# COMMAND ----------

display(df_atividade)

# COMMAND ----------

df_atividade.to_parquet("/mnt/rais2020/silver/atividades_cnae.parquet")

# COMMAND ----------


