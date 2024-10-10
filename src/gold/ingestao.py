# Databricks notebook source
# MAGIC %md
# MAGIC ### Etapa de criação de tabelas no modelo Dimensional (tabelas fato) para análise dos dados.

# COMMAND ----------

import sys
sys.path.insert(0, "../lib") # Inserindo o diretório para a primeira posição da lista

# Importação dos módulos 
import utils
import ingestors

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Recebendo os Widgets passados como parâmetro no Job

# COMMAND ----------

catalog = dbutils.widgets.get("catalog") #hive_metastore
database = dbutils.widgets.get("database")  #gold
table_name = dbutils.widgets.get("tablename") #top_10_client_ips

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Realizando a instância da classe para criação da Delta Table no modelo dimensional

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS gold")  # Criando o Database

ingest = ingestors.IngestorCubo(spark=spark,
                                    catalog=catalog,
                                    database=database,
                                    tablename=table_name)

ingest.execute()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Visualização do dado na Delta Table

# COMMAND ----------

spark.sql(f"SELECT * FROM hive_metastore.gold.{table_name}").show()
