# Databricks notebook source
import pyspark.pandas as pd

# COMMAND ----------

from pyspark.sql.functions import split
from pyspark.sql.types import *

# COMMAND ----------

ocupacao=pd.read_excel('/mnt/rais2020/bronze/RAIS_vinculos_layout2020.xls', sheet_name="ocupação")

# COMMAND ----------

display(ocupacao)

# COMMAND ----------

ocupacao=ocupacao[ocupacao['CBO 2002 Ocupação'].str.contains(":")]

# COMMAND ----------

ocupacao[["id_ocupacao","nome_ocupacao"]]=ocupacao["CBO 2002 Ocupação"].str.split(":", n=2,expand=True)

# COMMAND ----------

display(ocupacao)

# COMMAND ----------

df_ocupacao=ocupacao[["id_ocupacao","nome_ocupacao"]]

# COMMAND ----------

df_ocupacao.to_parquet("/mnt/rais2020/silver/ocupacao.parquet")

# COMMAND ----------


