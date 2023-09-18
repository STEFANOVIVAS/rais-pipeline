# Databricks notebook source
# MAGIC %md
# MAGIC Step 1 - Set schema and load data

# COMMAND ----------

from pyspark.sql.functions import col,format_number,regexp_replace,when

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,FloatType,DoubleType,DecimalType,LongType

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Designing dataframe schema
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
# MAGIC ### 2. Ingesting dataframe from 6 different brazilian regions and union into just one.
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

# MAGIC %md
# MAGIC ### 3. Casting columns to Integer data type
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
# MAGIC ### 4. Casting columns to Double data type
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
# MAGIC ### 5. Drop undesirable columns
# MAGIC

# COMMAND ----------

drop_columns=("Bairros SP","Bairros Fortaleza","Bairros RJ", "Distritos SP","Regiões Adm DF")
df_rais_brasil_clean=df_rais_brasil.drop(*drop_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Data Wrangling
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6.1 Education
# MAGIC

# COMMAND ----------

df_rais_brasil_clean_escolaridade = df_rais_brasil_clean.withColumn("Escolaridade", when(df_rais_brasil_clean["Escolaridade após 2005"] == 1,"ANALFABETO")\
                                                .when(df_rais_brasil_clean["Escolaridade após 2005"] == 2,"ATE 5.A INC")\
                                                .when(df_rais_brasil_clean["Escolaridade após 2005"] == 3,"5.A CO FUND")\
                                                .when(df_rais_brasil_clean["Escolaridade após 2005"] == 4,"6. A 9. FUND")\
                                                .when(df_rais_brasil_clean["Escolaridade após 2005"] == 5,"FUND COMPL")\
                                                .when(df_rais_brasil_clean["Escolaridade após 2005"] == 6,"MEDIO INCOMP")\
                                                .when(df_rais_brasil_clean["Escolaridade após 2005"] == 7,"MEDIO COMPL")\
                                                .when(df_rais_brasil_clean["Escolaridade após 2005"] == 8,"SUP. INCOMP")\
                                                .when(df_rais_brasil_clean["Escolaridade após 2005"] == 9,"SUP. COMP")\
                                                .when(df_rais_brasil_clean["Escolaridade após 2005"] == 10,"MESTRADO")\
                                                .when(df_rais_brasil_clean["Escolaridade após 2005"] == 11,"DOUTORADO")\
                                                .otherwise("IGNORADO")).drop("Escolaridade após 2005")

# COMMAND ----------

#### 6.2 Breed/Color

# COMMAND ----------

df_rais_brasil_clean_color = df_rais_brasil_clean_escolaridade.withColumn("raca_cor", when(df_rais_brasil_clean_escolaridade["Raça Cor"] == 1,"INDIGENA")\
                                                .when(df_rais_brasil_clean_escolaridade["Raça Cor"] == 2,"BRANCA")\
                                                .when(df_rais_brasil_clean_escolaridade["Raça Cor"] == 4,"PRETA")\
                                                .when(df_rais_brasil_clean_escolaridade["Raça Cor"] == 6,"AMARELA")\
                                                .when(df_rais_brasil_clean_escolaridade["Raça Cor"] == 8,"PARDA")\
                                                .when(df_rais_brasil_clean_escolaridade["Raça Cor"] == 9,"NAO IDENT")\
                                                .otherwise("IGNORADO")).drop("Raça Cor")

# COMMAND ----------

#### 6.3 Type of physical disability

# COMMAND ----------

df_rais_brasil_clean_disability = df_rais_brasil_clean_color.withColumn("tipo_deficiencia", when(df_rais_brasil_clean_color["Tipo Defic"] == 1,"FISICA")\
                                                .when(df_rais_brasil_clean_color["Tipo Defic"] == 2,"AUDITIVA")\
                                                .when(df_rais_brasil_clean_color["Tipo Defic"] == 3,"VISUAL")\
                                                .when(df_rais_brasil_clean_color["Tipo Defic"] == 4,"MENTAL")\
                                                .when(df_rais_brasil_clean_color["Tipo Defic"] == 5,"MULTIPLA")\
                                                .when(df_rais_brasil_clean_color["Tipo Defic"] == 6,"REABILITADO")\
                                                .when(df_rais_brasil_clean_color["Tipo Defic"] == 0,"NAO DEFICIENTE")\
                                                .otherwise("IGNORADO")).drop("Tipo Defic")

# COMMAND ----------

#### 6.4 Type of admission

# COMMAND ----------

df_rais_brasil_clean_admission = df_rais_brasil_clean_disability.withColumn("tipo_admissão", when(df_rais_brasil_clean_disability["Tipo Admissão"] == 00,"Não Admitido Ano")\
                                                .when(df_rais_brasil_clean_disability["Tipo Admissão"] == 01,"Primeiro Emprego")\
                                                .when(df_rais_brasil_clean_disability["Tipo Admissão"] == 02,"Reemprego")\
                                                .when(df_rais_brasil_clean_disability["Tipo Admissão"] == 03,"Transferência com Ônus")\
                                                .when(df_rais_brasil_clean_disability["Tipo Admissão"] == 04,"Transferência sem Ônus")\
                                                .when(df_rais_brasil_clean_disability["Tipo Admissão"] == 06,"Reintegração")\
                                                .when(df_rais_brasil_clean_disability["Tipo Admissão"] == 07,"Recondução")\
                                                .when(df_rais_brasil_clean_disability["Tipo Admissão"] == 08,"Reversão")\
                                                .when(df_rais_brasil_clean_disability["Tipo Admissão"] == 09,"Requisição")\
                                                .when(df_rais_brasil_clean_disability["Tipo Admissão"] == 10,"Exercício provisório")\
                                                .when(df_rais_brasil_clean_disability["Tipo Admissão"] == 11,"Readaptação")\
                                                .when(df_rais_brasil_clean_disability["Tipo Admissão"] == 12,"Redistribuição")\
                                                .when(df_rais_brasil_clean_disability["Tipo Admissão"] == 13,"Exercício descentralizado de servidor")\
                                                .when(df_rais_brasil_clean_disability["Tipo Admissão"] == 14,"Remoção")\
                                                .otherwise("Ignorado")).drop("Tipo Admissão")

# COMMAND ----------

#### 6.5 Type of employment relationship

# COMMAND ----------

df_rais_brasil_clean_relationship = df_rais_brasil_clean_admission.withColumn("tipo_vinculo", when(df_rais_brasil_clean_admission["Tipo Vínculo"] == 10,"CLT U/PJ IND")\
                                                .when(df_rais_brasil_clean_admission["Tipo Vínculo"] == 15,"CLT U/PF IND")\
                                                .when(df_rais_brasil_clean_admission["Tipo Vínculo"] == 20,"CLT R/PJ IND")\
                                                .when(df_rais_brasil_clean_admission["Tipo Vínculo"] == 25,"CLT R/PF IND")\
                                                .when(df_rais_brasil_clean_admission["Tipo Vínculo"] == 30,"ESTATUTARIO")\
                                                .when(df_rais_brasil_clean_admission["Tipo Vínculo"] == 31,"ESTAT RGPS")\
                                                .when(df_rais_brasil_clean_admission["Tipo Vínculo"] == 35,"ESTAT N/EFET")\
                                                .when(df_rais_brasil_clean_admission["Tipo Vínculo"] == 40,"AVULSO")\
                                                .when(df_rais_brasil_clean_admission["Tipo Vínculo"] == 50,"TEMPORARIO")\
                                                .when(df_rais_brasil_clean_admission["Tipo Vínculo"] == 55,"APREND CONTR")\
                                                .when(df_rais_brasil_clean_admission["Tipo Vínculo"] == 60,"CLT U/PJ DET")\
                                                .when(df_rais_brasil_clean_admission["Tipo Vínculo"] == 65,"CLT U/PF DET")\
                                                .when(df_rais_brasil_clean_admission["Tipo Vínculo"] == 70,"CLT R/PJ DET")\
                                                .when(df_rais_brasil_clean_admission["Tipo Vínculo"] == 75,"CLT R/PF DET")\
                                                .when(df_rais_brasil_clean_admission["Tipo Vínculo"] == 80,"DIRETOR")\
                                                .when(df_rais_brasil_clean_admission["Tipo Vínculo"] == 90,"CONT PRZ DET")\
                                                .when(df_rais_brasil_clean_admission["Tipo Vínculo"] == 95,"CONT TMP DET")\
                                                .when(df_rais_brasil_clean_admission["Tipo Vínculo"] == 96,"CONT LEI EST")\
                                                .when(df_rais_brasil_clean_admission["Tipo Vínculo"] == 97,"CONT LEI MUN")\
                                                .otherwise("IGNORADO")).drop("Tipo Vínculo")

# COMMAND ----------

#### 6.6 Type of Ecomomic subsector

# COMMAND ----------

df_rais_brasil_clean_subsetor = df_rais_brasil_clean_relationship.withColumn("IBGE_subsetor", when(df_rais_brasil_clean_relationship["IBGE Subsetor"] == 01,"Extrativa mineral")\
                                                .when(df_rais_brasil_clean_relationship["IBGE Subsetor"] == 02,"Indústria de produtos minerais nao metálicos")\
                                                .when(df_rais_brasil_clean_relationship["IBGE Subsetor"] == 03,"Indústria metalúrgica")\
                                                .when(df_rais_brasil_clean_relationship["IBGE Subsetor"] == 04,"Indústria mecânica")\
                                                .when(df_rais_brasil_clean_relationship["IBGE Subsetor"] == 05,"Indústria do material elétrico e de comunicaçoes")\
                                                .when(df_rais_brasil_clean_relationship["IBGE Subsetor"] == 06,"Indústria do material de transporte")\
                                                .when(df_rais_brasil_clean_relationship["IBGE Subsetor"] == 07,"Indústria da madeira e do mobiliário")\
                                                .when(df_rais_brasil_clean_relationship["IBGE Subsetor"] == 08,"Indústria do papel, papelao, editorial e gráfica")\
                                                .when(df_rais_brasil_clean_relationship["IBGE Subsetor"] == 09,"Ind. da borracha, fumo, couros, peles, similares")\
                                                .when(df_rais_brasil_clean_relationship["IBGE Subsetor"] == 10,"Ind. química de produtos farmacêuticos, veterinários, perfumaria")\
                                                .when(df_rais_brasil_clean_relationship["IBGE Subsetor"] == 11,"Indústria têxtil do vestuário e artefatos de tecidos")\
                                                .when(df_rais_brasil_clean_relationship["IBGE Subsetor"] == 12,"Indústria de calçados")\
                                                .when(df_rais_brasil_clean_relationship["IBGE Subsetor"] == 13,"Indústria de produtos alimentícios, bebidas e álcool etílico")\
                                                .when(df_rais_brasil_clean_relationship["IBGE Subsetor"] == 14,"Serviços industriais de utilidade pública")\
                                                .when(df_rais_brasil_clean_relationship["IBGE Subsetor"] == 15,"Construçao civil")\
                                                .when(df_rais_brasil_clean_relationship["IBGE Subsetor"] == 16,"Comércio varejista")\
                                                .when(df_rais_brasil_clean_relationship["IBGE Subsetor"] == 17,"Comércio atacadista")\
                                                .when(df_rais_brasil_clean_relationship["IBGE Subsetor"] == 18,"Instituiçoes de crédito, seguros e capitalizaçao")\
                                                .when(df_rais_brasil_clean_relationship["IBGE Subsetor"] == 19,"Com. e administraçao de imóveis, valores mobiliários, serv. Técnico")\
                                                .when(df_rais_brasil_clean_relationship["IBGE Subsetor"] == 20,"Transportes e comunicaçoes")\
                                                .when(df_rais_brasil_clean_relationship["IBGE Subsetor"] == 21,"Serv. de alojamento, alimentaçao, reparaçao, manutençao, redaçao")\
                                                .when(df_rais_brasil_clean_relationship["IBGE Subsetor"] == 22,"Serviços médicos, odontológicos e veterinários")\
                                                .when(df_rais_brasil_clean_relationship["IBGE Subsetor"] == 23,"Ensino")\
                                                .when(df_rais_brasil_clean_relationship["IBGE Subsetor"] == 24,"Administraçao pública direta e autárquica")\
                                                .when(df_rais_brasil_clean_relationship["IBGE Subsetor"] == 25,"Agricultura, silvicultura, criaçao de animais, extrativismo vegetal")\
                                                .otherwise("Ignorado")).drop("IBGE Subsetor")

# COMMAND ----------

#### 6.6 Insert year

# COMMAND ----------

df_rais_brasil_clean_subsetor.wirtColumn("Ano", lit("2020"))
