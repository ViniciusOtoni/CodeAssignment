from pyspark.sql import Row
import re
import utils

class Ingestor:

    """
        Classe para ingestão em Batch dos dados utilizado na camada bronze do projeto

        Métodos:

        - Construtor: Recebe como parâmetro o spark, catálogo de dados, database, nome da tabela e o formato do arquivo a ser processado.
        - load: Recebe como parâmetro o ponto do MOUNT aonde o arquivo está localizado, retorna um RDD.
        - parseDataframe: Recebe como parâmetro o ponto do MOUNT aonde o arquivo está e realiza a aplicação do schema.
        - save: Recebe como parâmetro o dataframe e salva em um Delta Table.
        - execute: Função responsável pela execução da classe.

    """

    def __init__(self, spark, catalog, database, tablename, data_format):
        self.spark = spark
        self.catalog = catalog
        self.database = database
        self.tablename = tablename
        self.format = data_format
      

    def load(self, path):
        # Carrega o arquivo como RDD
        rdd = self.spark.sparkContext.textFile(path)

        # RDD (conjunto de dados que será distribuído em diferentes VM's do Cluster aumentando a eficiência do processamento).
        return rdd

    def parseDataframe(self, path):
        # Regex pattern para separar os componentes do log
        regex_pattern = r'(\S+) - - \[(.*?)\] \"(\S+) (\S+) (\S+)\" (\S+) (\S+)'

        # Carregar o RDD
        rdd = self.load(path)

        # Processar o RDD usando map para aplicar o regex
        parsed_rdd = rdd.map(lambda line: re.match(regex_pattern, line)) \
                        .filter(lambda x: x is not None)  \
                        .map(lambda match: Row(
                            client_ip=match.group(1),
                            timestamp=match.group(2),
                            request_method=match.group(3),
                            request_url=match.group(4),
                            http_version=match.group(5),
                            response_code=match.group(6),
                            response_size=match.group(7)
                        )) 


        # Converter o RDD para DataFrame
        df = self.spark.createDataFrame(parsed_rdd)
        
        return df

    def save(self, df):
        (df.coalesce(1)
            .write
            .format('delta')
            .mode("overwrite")
            .saveAsTable(f"{self.catalog}.{self.database}.{self.tablename}"))

    def execute(self, path):
        df = self.parseDataframe(path)
        self.save(df)

# --- Classe para ingestão dos dados na camada gold utilizando o modelo dimensional (OLAP) --- 

class IngestorCubo:

    """

        Classe para ingestão dos dados utilizando o modelo dimensional.

        Métodos:

        - Construtor: Recebe como parâmetro o spark, catálogo de dados, database e nome da tabela.
        - set_query: Recebe como parâmetro o nome da tabela para executar a query SQL para análise do dado e criação da tabela fato.
        - load: Recebe qualquer tipo de parâmetro para utilizar como filtro nas query`s SQL (está utilizando o **kwargs).
        - save: Recebe como parâmetro o df e salva em um Delta Table.
        - execute: Função responsável pela execução da classe.

    """

    def __init__(self, spark, catalog, database, tablename):
        self.spark = spark
        self.catalog = catalog
        self.database = database
        self.tablename = tablename

        self.table_fullname = f"{catalog}.{database}.{tablename}"

        self.set_query()

    def set_query(self):
        self.query = utils.import_query(f"{self.tablename}.sql")

    def load(self, **kwargs):
        formatted_query = self.query.format(**kwargs)
        df = self.spark.sql(formatted_query)
        return df
    
    def save(self, df): 

        self.spark.sql(f'DROP TABLE IF EXISTS {self.tablename}')

        if not utils.table_exists(self.spark, self.catalog, self.databasename, self.tablename):
            (df.coalesce(1)
            .write
            .format('delta')
            .mode("overwrite")
            .saveAsTable(self.table))
        else:
            print("Tabela já existe!")
        
    def execute(self, **kwargs):
        df = self.load(**kwargs)
        self.save(df)




        