# Databricks notebook source
import pyspark.pandas as pd

# COMMAND ----------

from pyspark.sql.functions import split
from pyspark.sql.types import *

# COMMAND ----------

municipios=pd.read_excel('/mnt/rais2020/bronze/RAIS_vinculos_layout2020.xls', sheet_name="municipio")

# COMMAND ----------

display(municipios)

# COMMAND ----------

municipios=municipios[municipios['Município'].str.contains(":")]

# COMMAND ----------

municipios[["id_municipio","local"]]=municipios["Município"].str.split(":", n=2,expand=True)

# COMMAND ----------

municipios[["uf","cidade"]]=municipios["local"].str.split("-", n=2,expand=True)

# COMMAND ----------

municipios['uf']=municipios['uf'].str.upper()

# COMMAND ----------

df_municipios=municipios[["id_municipio","cidade","uf"]]


# COMMAND ----------

df_municipios.to_parquet("/mnt/rais2020/silver/municipios.parquet")

# COMMAND ----------


