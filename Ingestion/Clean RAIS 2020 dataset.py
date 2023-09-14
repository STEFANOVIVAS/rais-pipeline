# Databricks notebook source
# MAGIC %md
# MAGIC Step 1 - Set schema and load data

# COMMAND ----------

from pyspark.sql.functions import col,format_number,regexp_replace

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,FloatType,DoubleType,DecimalType,LongType

# COMMAND ----------

# MAGIC %md
# MAGIC #### Designing dataframe schema
# MAGIC

# COMMAND ----------

rais_schema=StructType(fields=[StructField("Bairros SP",StringType(),False),
                                  StructField("Bairros Fortaleza",StringType(),False),
                                  StructField("Bairros RJ",StringType(),False),
                                  StructField("Causa Afastamento 1",StringType(),False),
                                  StructField("Causa Afastamento 2",StringType(),True),
                                  StructField("Causa Afastamento 3",StringType(),False),
                                  StructField("Motivo Desligamento",StringType(),True),
                                  StructField("CBO Ocupação 2002",StringType(),False),
                                  StructField("CNAE 2.0 Classe",StringType(),False),
                                  StructField("CNAE 95 Classe",StringType(),False),
                                  StructField("Distritos SP",StringType(),True),
                                  StructField("Vínculo Ativo 31/12",StringType(),True),
                                  StructField("Faixa Etária",StringType(),True),
                                  StructField("Faixa Hora Contrat",StringType(),True),
                                  StructField("Faixa Remun Dezem (SM)",StringType(),True),
                                  StructField("Faixa Remun Média (SM)",StringType(),True),
                                  StructField("Faixa Tempo Emprego",StringType(),True),
                                  StructField("Escolaridade após 2005",StringType(),True),
                                  StructField("Qtd Hora Contr",IntegerType(),True),
                                  StructField("Idade",IntegerType(),True),
                                  StructField("Ind CEI Vinculado",StringType(),True),
                                  StructField("Ind Simples",StringType(),True),
                                  StructField("Mês Admissão",IntegerType(),True),
                                  StructField("Mês Desligamento",IntegerType(),True),
                                  StructField("Mun Trab",StringType(),True),
                                  StructField("Município",StringType(),True),
                                  StructField("Nacionalidade",StringType(),False),
                                  StructField("Natureza Jurídica",StringType(),True),
                                  StructField("Ind Portador Defic",StringType(),True),
                                  StructField("Qtd Dias Afastamento",IntegerType(),True),
                                  StructField("Raça Cor",StringType(),True),
                                  StructField("Regiões Adm DF",StringType(),True),
                                  StructField("Vl Remun Dezembro Nom",DoubleType(),True),
                                  StructField("Vl Remun Dezembro (SM)",DoubleType(),True),
                                  StructField("Vl Remun Média Nom",DoubleType(),True),
                                  StructField("Vl Remun Média (SM)",DoubleType(),True),
                                  StructField("CNAE 2.0 Subclasse",StringType(),True),
                                  StructField("Sexo Trabalhador",StringType(),True),
                                  StructField("Tamanho Estabelecimento",StringType(),True),
                                  StructField("Tempo Emprego",DoubleType(),True),
                                  StructField("Tipo Admissão",StringType(),True),
                                  StructField("Tipo Estab41",StringType(),True),
                                  StructField("Tipo Estab42",StringType(),True),
                                  StructField("Tipo Defic",StringType(),True),
                                  StructField("Tipo Vínculo",StringType(),True),
                                  StructField("IBGE Subsetor",StringType(),True),
                                  StructField("Vl Rem Janeiro SC",DoubleType(),True),
                                  StructField("Vl Rem Fevereiro SC",DoubleType(),True),
                                  StructField("Vl Rem Março SC",DoubleType(),True),
                                  StructField("Vl Rem Abril SC",DoubleType(),True),
                                  StructField("Vl Rem Maio SC",DoubleType(),True),
                                  StructField("Vl Rem Junho SC",DoubleType(),True),
                                  StructField("Vl Rem Julho SC",DoubleType(),True),
                                  StructField("Vl Rem Agosto SC",DoubleType(),True),
                                  StructField("Vl Rem Setembro SC",DoubleType(),True),
                                  StructField("Vl Rem Outubro SC",DoubleType(),True),
                                  StructField("Vl Rem Novembro SC",DoubleType(),True),
                                  StructField("Ano Chegada Brasil",IntegerType(),True),
                                  StructField("Ind Trab Intermitente",StringType(),True),
                                  StructField("Ind Trab Parcial",StringType(),True)])

# COMMAND ----------

# MAGIC %md
# MAGIC #### The dataframe is separated in 6 differents Brazilian regions. Joining all dataframes in just one with all rows.
# MAGIC

# COMMAND ----------

df_rais_centro_oeste=spark.read.csv("/mnt/rais/raw/RAIS_VINC_PUB_CENTRO_OESTE.txt.gz", encoding='ISO-8859-1', header=True, sep=";")

# COMMAND ----------

df_rais_nordeste=spark.read.csv("/mnt/rais/raw/RAIS_VINC_PUB_NORDESTE.txt.gz", encoding='ISO-8859-1', header=True, sep=";")

# COMMAND ----------

df_rais_norte=spark.read.csv("/mnt/rais/raw/RAIS_VINC_PUB_NORTE.txt.gz", encoding='ISO-8859-1', header=True, sep=";")

# COMMAND ----------

df_rais_sul=spark.read.csv("/mnt/rais/raw/RAIS_VINC_PUB_SUL.txt.gz", encoding='ISO-8859-1', header=True, sep=";")

# COMMAND ----------

df_rais_mg=spark.read.csv("/mnt/rais/raw/RAIS_VINC_PUB_MG_ES_RJ.txt.gz", encoding='ISO-8859-1', header=True, sep=";")

# COMMAND ----------

df_rais_sp=spark.read.csv("/mnt/rais/raw/RAIS_VINC_PUB_SP.txt.gz", encoding='ISO-8859-1', header=True, sep=";")

# COMMAND ----------

df_rais_brasil=df_rais_centro_oeste.union(df_rais_mg).union(df_rais_nordeste).union(df_rais_norte).union(df_rais_sp).union(df_rais_sul)

# COMMAND ----------

display(df_rais_brasil)

# COMMAND ----------

df_rais_brasil.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Casting columns to Integer data type
# MAGIC

# COMMAND ----------

def cast_string_to_integer(columns,df):
    for column in columns:
        df=df.withColumn(column,df[column].cast('integer'))
    return df
    

# COMMAND ----------

integer_format_columns=["Qtd Hora Contr", "Idade", "Mês Admissão", "Mês Desligamento", "Qtd Dias Afastamento","Ano Chegada Brasil"]
df_rais_brasil=cast_string_to_integer(integer_format_columns, df_rais_brasil)

# COMMAND ----------

display(df_rais_brasil.printSchema())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Casting columns to Double data type
# MAGIC

# COMMAND ----------

def cast_string_to_double(columns_list, df):
    for column in columns_list:
        df = df.withColumn(column, regexp_replace(column, ',', '.'))
        df=df.withColumn(column,df[column].cast(DoubleType()))
    return df

# COMMAND ----------

double_format_columns=["Tempo Emprego","Vl Remun Dezembro Nom","Vl Remun Dezembro (SM)","Vl Remun Média Nom","Vl Remun Média (SM)","Vl Rem Janeiro SC","Vl Rem Fevereiro SC","Vl Rem Março SC","Vl Rem Abril SC","Vl Rem Maio SC","Vl Rem Junho SC","Vl Rem Julho SC","Vl Rem Agosto SC","Vl Rem Setembro SC","Vl Rem Outubro SC","Vl Rem Novembro SC"]

# COMMAND ----------

df_rais_brasil=cast_string_to_double(double_format_columns,df_rais_brasil)

# COMMAND ----------

display(df_rais_brasil)

# COMMAND ----------

display(df_rais_brasil.printSchema())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop undesirable columns
# MAGIC

# COMMAND ----------

drop_columns=("Bairros SP","Bairros Fortaleza","Bairros RJ", "Distritos SP","Regiões Adm DF")
df_rais_brasil.drop(*drop_columns).printSchema()

# COMMAND ----------


