from pyspark.sql import Row
import re

class Ingestor:

    """
        Classe para ingestão em Batch dos dados utilizado na camada bronze do projeto

        Métodos:

        Construtor: Recebe como parâmetro o spark, catálogo de dados, database, nome da tabela e o formato do arquivo a ser processado.
        load: Recebe como parâmetro o ponto do MOUNT aonde o arquivo está localizado, retorna um RDD.
        parseDataframe: Recebe como parâmetro o ponto do MOUNT aonde o arquivo está e realiza a aplicação do schema.
        save: Recebe como parâmetro o dataframe e salva em um Delta Table.
        execute: Função responsável pela execução da classe.

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

        