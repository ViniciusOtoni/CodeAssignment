def import_query(path):

    """
        Esta função realiza a leitura do arquivo como objetivo a extração da query gerada no arquivo *.sql

        Parâmetros:
        
        - path(String): Caminho para o arquivo com a extensão .sql
    """
    
    with open(path, 'r') as open_file:
        return open_file.read()
    


def create_mount(spark, mount_name, source_url, conf_key, account_key):

    """
        Esta função faz a criação do "mount" caso ele ainda não exista.

        Parâmetros:

        - mount_name(String): Nome do mount a ser montado.
        - source_url(String): Path para acesso ao blob storage.
        - conf_key(String): Path para chave de acesso ao blob storage.
        - account_key(String): Chave de acesso do storage account.

    """
    
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
    
    # Verifica se o ponto de montagem já existe
    if any(mount.mountPoint == mount_name for mount in dbutils.fs.mounts()):
        print(f"O ponto de montagem '{mount_name}' já existe.")
    else:
        # Montar o Blob Storage
        dbutils.fs.mount(
            source=source_url,
            mount_point=mount_name,
            extra_configs={conf_key: account_key}
        )

def table_exists(spark, catalog, database, table):

    """
        Esta função realiza a verificação caso a tabela já exista no database.

        Parâmetros:

        - catalog(String): Nome do catálogo. 
        - database(String): Nome do database.
        - table(String): Nome da tabela.

    """
    count = (spark.sql(f"SHOW TABLES FROM {catalog}.{database}")
                .filter(f"database = '{database}' AND tableName = '{table}'")
                .count()) # Retorna a quantidade de tabelas encontradas.
    return count == 1 # Retorna valor boolean de acordo com o valor da variável count.

