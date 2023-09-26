# Databricks notebook source
from pyspark.sql.functions import col,initcap

# COMMAND ----------

df_vinculos=spark.read.parquet("/mnt/rais2020/silver/Rais_vinculos_2020.parquet")

# COMMAND ----------

display(df_vinculos)

# COMMAND ----------

df_vinculos_municipios=df_vinculos.withColumnRenamed("Mun Trab","municipio_trabalho").withColumnRenamed("Município","municipio_estab").withColumnRenamed("CNAE 2.0 Subclasse","cnae_subclasse").withColumnRenamed("CBO Ocupação 2002", "cbo_ocupacao")

# COMMAND ----------

display(df_vinculos_municipios.printSchema())

# COMMAND ----------

df_atividades=spark.read.parquet("/mnt/rais2020/silver/atividades_cnae.parquet")

# COMMAND ----------

display(df_atividades)

# COMMAND ----------

df_municipios=spark.read.parquet("/mnt/rais2020/silver/municipios.parquet")

# COMMAND ----------

display(df_municipios)

# COMMAND ----------

df_ocupacao=spark.read.parquet("/mnt/rais2020/silver/ocupacao.parquet")

# COMMAND ----------

display(df_ocupacao)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join as dataframes into just one

# COMMAND ----------

df_rais_complete=df_vinculos_municipios.join(df_atividades,df_vinculos_municipios["cnae_subclasse"]==df_atividades.id_atividade,"left").join(df_municipios,df_vinculos_municipios.municipio_trabalho==df_municipios.id_municipio,"left").join(df_ocupacao,df_vinculos_municipios.cbo_ocupacao==df_ocupacao.id_ocupacao,"left")

# COMMAND ----------

display(df_rais_complete)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ordering columns logically and discarting final columns
# MAGIC

# COMMAND ----------

columns=['ano','Vínculo Ativo 31/12','Qtd Hora Contr','Idade','escolaridade','raca_cor','sexo_trabalhador','Nacionalidade',
 'Ind Portador Defic','tipo_deficiencia','Ind Simples','Mês Admissão','Mês Desligamento','tipo_admissão','tipo_vinculo','IBGE_subsetor',
 'nome_atividade','nome_ocupacao','cidade','uf','Natureza Jurídica', 'Qtd Dias Afastamento','Tamanho Estabelecimento','Tempo Emprego',
 'Vl Remun Dezembro Nom','Vl Remun Dezembro (SM)','Vl Remun Média Nom','Vl Remun Média (SM)','Tipo Estab42','Vl Rem Janeiro SC',
 'Vl Rem Fevereiro SC', 'Vl Rem Março SC', 'Vl Rem Abril SC', 'Vl Rem Maio SC', 'Vl Rem Junho SC', 'Vl Rem Julho SC', 'Vl Rem Agosto SC',
 'Vl Rem Setembro SC', 'Vl Rem Outubro SC', 'Vl Rem Novembro SC', 'Ano Chegada Brasil', 'Ind Trab Intermitente', 'Ind Trab Parcial']

# COMMAND ----------

df_rais_vinculos=df_rais_complete.select(columns)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Adjusting columns names

# COMMAND ----------

rais_vinculos=df_rais_vinculos.withColumnRenamed("Vínculo Ativo 31/12","vinculo_ativo_31/12").\
                                withColumnRenamed("Qtd Hora Contr","qtd_hora_contr").\
                                withColumnRenamed("Ind Portador Defic","ind_portador_defic").\
                                withColumnRenamed("Ind Simples","ind_simples").\
                                withColumnRenamed("Mês Admissão","mes_admissão").\
                                withColumnRenamed("Mês Desligamento","mes_desligamento").\
                                withColumnRenamed("Natureza Jurídica","natureza_juridica").\
                                withColumnRenamed("Qtd Dias Afastamento","qtd_dias_afastamento").\
                                withColumnRenamed("Tamanho Estabelecimento","tamanho_estabelecimento").\
                                withColumnRenamed("Tempo Emprego","tempo_emprego").\
                                withColumnRenamed("cidade","cidade_laboral").\
                                withColumnRenamed("uf","uf_laboral").\
                                withColumnRenamed("Vl Remun Dezembro Nom","vl_remun_dezembro_nom").\
                                withColumnRenamed("Vl Remun Dezembro (SM)","vl_remun_dezembro_sm)").\
                                withColumnRenamed("Vl Remun Média Nom","vl_remun_media_nom").\
                                withColumnRenamed("Vl Remun Média (SM)","vl_remun_media_sm").\
                                withColumnRenamed("Tipo Estab42","tipo_estabelecimento").\
                                withColumnRenamed("Vl Rem Janeiro SC","vl_rem_janeiro_sc").\
                                withColumnRenamed("Vl Rem Fevereiro SC","vl_rem_fevereiro_sc").\
                                withColumnRenamed("Vl Rem Março SC","vl_rem_marco_sc").\
                                withColumnRenamed("Vl Rem Abril SC","vl_rem_abril_sc").\
                                withColumnRenamed("Vl Rem Maio SC","vl_rem_maio_sc").\
                                withColumnRenamed("Vl Rem Junho SC","vl_rem_junho_sc").\
                                withColumnRenamed("Vl Rem Julho SC","vl_rem_julho_sc").\
                                withColumnRenamed("Vl Rem Agosto SC","vl_rem_agosto_sc").\
                                withColumnRenamed("Vl Rem Setembro SC","vl_rem_setembro_sc").\
                                withColumnRenamed("Vl Rem Outubro SC","vl_rem_outubro_sc").\
                                withColumnRenamed("Vl Rem Novembro SC","vl_rem_novembro_sc").\
                                withColumnRenamed("Vl Rem Dezembro SC","vl_rem_dezembro_sc").\
                                withColumnRenamed("Ano Chegada Brasil","ano_chegada_brasil").\
                                withColumnRenamed("Ind Trab Intermitente","ind_trab_intermitente").\
                                withColumnRenamed("Ind Trab Parcial","ind_trab_parcial")

# COMMAND ----------

display(rais_vinculos)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Capitalize columns 
# MAGIC

# COMMAND ----------

df_rais_vinculos=rais_vinculos.withColumn("raca_cor",initcap(col('raca_cor'))).withColumn("escolaridade",initcap(col("escolaridade"))).withColumn("tipo_deficiencia",initcap(col("tipo_deficiencia")))

# COMMAND ----------

df_rais_vinculos.mode('overwrite').write.parquet("/mnt/rais2020/gold/rais_vinculos.parquet")
