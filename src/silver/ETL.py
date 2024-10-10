# Databricks notebook source
# MAGIC %md
# MAGIC ### Etapa de Extração do dado, Transformação e Carregar a base "limpa"

# COMMAND ----------

import sys
sys.path.insert(0, "../lib/") # Inserindo o diretório para a primeira posição da lista

# Importação dos módulos
import utils
import pipeline


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Recebendo os Widgets passados como parâmetro no Job

# COMMAND ----------

table_name = dbutils.widgets.get("tablename") #access_logs
catalog = dbutils.widgets.get("catalog") #hive_metastore 
database = dbutils.widgets.get("database") #silver

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Realizando a instância do Pipeline para execução do processo de ETL

# COMMAND ----------

# Leitura da tabela na camada bronze

df_bronze = spark.sql("SELECT * FROM hive_metastore.bronze.access_logs")

# COMMAND ----------

pipe = pipeline.Pipe(df_bronze) # Instância do Pipeline.

df_silver = pipe.execute() # Atribuindo o dataframe transformado ao dataframe original.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Criação da Delta Table com os dados já processados para futuras análises

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS silver")  # Criando o Database

if not utils.table_exists(spark, catalog, database, table):
    print("Tabela não existe, criando...")

    table_fullname = f"{catalog}.{database}.{table_name}"

    (df_silver.coalesce(1) #Partição única.
            .write
            .format('delta')
            .mode("overwrite")
            .saveAsTable(table_fullname))

    print("Tabela criada com sucesso!")
else:
    print("Tabela já existe, ignorando full-load")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Visualização dos dados na Delta Table

# COMMAND ----------

spark.sql("SELECT * FROM hive_metastore.silver.access_logs").show()
