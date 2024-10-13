from pyspark.ml import Transformer
from pyspark.sql import DataFrame
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import when
from pyspark.sql.functions import count
from pyspark.sql import Window
from pyspark.sql.functions import count, col


class RemoveNullsAndDuplicates(Transformer):

    """

        Classe que remove registros nulos e duplicados do Dataframe.

        Métodos:

        - Construtor: Recebe como parâmetro a coluna que contém os valores a serem removidos.
        - Transform: Remove registros nulos e duplicados.

    """

    def __init__(self, subset_cols):
        self.subset_cols = subset_cols  # Coluna target para remover nulos e duplicatas

    def _transform(self, df: DataFrame) -> DataFrame:
        # Remove registros nulos e duplicados
        df_clean = df.dropna(subset=self.subset_cols).dropDuplicates(subset=self.subset_cols)
        return df_clean



class NormalizeTimestamp(Transformer):
    
    """
    
        Classe que normaliza o timestamp de uma coluna.

        Métodos:

        - Construtor: Recebe como parâmetro a coluna que contém o timestamp a ser normalizado e o nome da coluna para output com o valor final.
        - Transform: Normaliza o timestamp.
    
    """

    def __init__(self, input_col, output_col):
        self.input_col = input_col
        self.output_col = output_col

    def _transform(self, df: DataFrame) -> DataFrame:
        # Converter o timestamp para um formato padronizado
        df_normalized = df.withColumn(self.output_col, to_timestamp(df[self.input_col], "dd/MMM/yyyy:HH:mm:ss Z"))
        return df_normalized




class CategorizeResponseCode(Transformer):

    """

        Classe que categoriza o status code da requisição HTTP.

        Métodos:

        - Construtor: Recebe como parâmetro a coluna que contém o status code a ser categorizado e o nome da coluna para output com o valor final.
        - Tramsform: Cria a nova feature de categoria do status code.

    """

    def __init__(self, input_col, output_col):
        self.input_col = input_col
        self.output_col = output_col

    def _transform(self, df: DataFrame) -> DataFrame:
        # Categorizar o código de resposta HTTP
        df_categorized = df.withColumn(
            self.output_col, 
            when(df[self.input_col].between(200, 299), "Success")
            .when(df[self.input_col].between(400, 499), "Client Error")
            .when(df[self.input_col].between(500, 599), "Server Error")
            .otherwise("Other")
        )
        return df_categorized

    




