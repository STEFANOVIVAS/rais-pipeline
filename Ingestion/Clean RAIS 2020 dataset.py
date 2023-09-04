# Databricks notebook source
def mount_adls(storage_account,container):
    # get secrets from key vault
    # client_id=dbutils.secrets.get(scope='formula1-scope',key='formula1-client-id')
    # tenant_id=dbutils.secrets.get(scope='formula1-scope',key='formula1-tenant-id')
    client_id='3f45b0e2-4ec4-4e0d-92ae-0e80a7655121'
    tenant_id='4415efc0-fc02-4494-a228-5f6d13544717'
    client_secret='nPZ8Q~F31HVsKV24zZHbwvAyCGxI3FUF~I_GEdi7'

    # client_secret=dbutils.secrets.get(scope='formula1-scope',key='formula1-client-secret')
    # Set spark configs
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

    if any(mount.mountPoint == f"/mnt/{storage_account}/{container}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account}/{container}") 
    # Mount the Storage
    dbutils.fs.mount(
    source = f"abfss://{container}@{storage_account}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account}/{container}",
    extra_configs = configs)
    dbutils.fs.mounts()

# COMMAND ----------

mount_adls('rais','rais-2020')

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1 - Set schema and load data

# COMMAND ----------

dbutils.fs.ls("/mnt/rais/rais-2020/")

# COMMAND ----------

df_despesa=spark.read.csv("/mnt/rais/rais-2020/RAIS_VINC_PUB_CENTRO_OESTE.txt.gz", encoding='ISO-8859-1', header=True, sep=";")

# COMMAND ----------

display(df_despesa)

# COMMAND ----------

display(df_despesa.count())

# COMMAND ----------


