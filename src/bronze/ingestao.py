# Databricks notebook source
# MAGIC %md
# MAGIC #### Etapa de ingestão do dado em Batch e realizando a conexão com o Blob Storage GEN 2 para criação do MOUNT.

# COMMAND ----------

import sys
import os
# -------- # 
sys.path.insert(0, "../lib/") # Inserindo o diretório para a primeira posição da lista

# Importação dos módulos
import utils
import ingestors


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Recebendo os Widgets passados como parâmetro no Job

# COMMAND ----------

catalog = "hive_metastore"  #dbutils.widgets.get("catalog") #
database = "bronze" #dbutils.widgets.get("database") #"
table_name = "access_logs"  #dbutils.widgets.get("tablename") #


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Informações sobre o Blob Storage Account

# COMMAND ----------

account_name = "webserverassignment" # Nome do datalake.
account_key = os.getenv("BLOB_STORAGE_ACCOUNT_KEY") # secret (está como variável de ambiente do cluster).
container_name = "raw" # Nome do contâiner dentro do datalake.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### URL para conexão com o blob e path para ponto do mount

# COMMAND ----------

mount_name = f"/mnt/project/raw/{database}/full_load/{table_name}"  # o mount será gerado nesse path

source_url = f"wasbs://{container_name}@{account_name}.blob.core.windows.net"
conf_key = f"fs.azure.account.key.{account_name}.blob.core.windows.net"

# COMMAND ----------

utils.create_mount(spark, mount_name, source_url, conf_key, account_key)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Verificando se o MOUNT foi criado e exibindo as informações 

# COMMAND ----------

dbutils.fs.ls('/mnt/project/raw/bronze/full_load/access_logs')

# COMMAND ----------

print(dbutils.fs.head('/mnt/project/raw/bronze/full_load/access_logs/access_log.txt', 1000))


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Realizando ingestão do dado e criação da Delta Table

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")  # Criando o Database

if not utils.table_exists(spark, catalog, database, table_name):
    print("Tabela não existe, criando...")

    ingest_full_load = ingestors.Ingestor(
        spark=spark,
        catalog=catalog,
        database=database,
        tablename=table_name, 
        data_format='text',
    )
    
    # Executa a ingestão
    ingest_full_load.execute(f"/mnt/project/raw/{database}/full_load/{table_name}")

    print("Tabela criada com sucesso!")
else:
    print("Tabela já existe, ignorando full-load")

