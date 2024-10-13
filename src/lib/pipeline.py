from pyspark.ml import Pipeline
from ETL import RemoveNullsAndDuplicates, NormalizeTimestamp, CategorizeResponseCode

class Pipe:

    def __init__(self, df):
        self.df = df  # DF original

    def create_pipeline(self):
        
        """
            Método responsável pela instância das classes de transformação e criação do Pipeline.
        """

        # Instâncias das classes de transformação
        remove_nulls_duplicates = RemoveNullsAndDuplicates(subset_cols=["client_ip", "timestamp"])
        normalize_timestamp = NormalizeTimestamp(input_col="timestamp", output_col="timestamp_normalized")
        categorize_response = CategorizeResponseCode(input_col="response_code", output_col="response_category")

        # Adicionando as transformações no pipeline 
        return Pipeline(stages=[remove_nulls_duplicates, 
                                normalize_timestamp, 
                                categorize_response])  # Executado de forma sequencial

    def fit_pipeline(self, pipeline, df):
        return pipeline.fit(df)

    def transform_pipeline(self, model, df):
        return model.transform(df)

    def execute(self):

        # Criar o pipeline
        pipeline = self.create_pipeline()
        model = self.fit_pipeline(pipeline, self.df)
        
        # Aplicando as transformações e retornando o DataFrame transformado
        df_transformed = self.transform_pipeline(model, self.df)
        return df_transformed
