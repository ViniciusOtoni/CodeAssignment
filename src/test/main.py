import unittest
from pyspark.sql import SparkSession, Row
from ETL import RemoveNullsAndDuplicates, NormalizeTimestamp, CategorizeResponseCode, CountAccessByIP
import utils
from unittest.mock import patch

class TestUtils(unittest.TestCase): #herdando as funcionalidades da classe TestCase

    #@patch > Decorator que está substituindo a função open
    # Decorator é um design patterns responsável por adicionar ou modificar um comportamento da função de forma dinâmica.

    @patch("builtins.open", unittest.mock.mock_open(read_data="SELECT * FROM table")) #mock_open é responsável por simular a leitura do arquivo
    def test_import_query(self):
        query = utils.import_query("test.sql")
        self.assertEqual(query, "SELECT * FROM table") # Verificando se os resultados batem


class TestETL(unittest.TestCase):

    @classmethod #transforma em um método da classe podendo ser chamado diretamente pela classe 
    def setUpClass(cls): #mesmo conceito do self. (permitir acesso aos atributos e métodos)
       

        cls.spark = SparkSession.builder.master("local[1]").appName("ETLTest").getOrCreate()
        
        # Criando um DataFrame de exemplo para testes
        cls.df = cls.spark.createDataFrame([
            Row(client_ip="192.168.0.1", timestamp="15/Jul/2009:14:58:59 -0700", response_code=200),
            Row(client_ip="192.168.0.2", timestamp="15/Jul/2009:14:58:59 -0700", response_code=404),
            Row(client_ip=None, timestamp=None, response_code=500), #contém nulos
            Row(client_ip="192.168.0.1", timestamp="15/Jul/2009:14:58:59 -0700", response_code=200)  #linha duplicada
        ]) 
        # o método setUpClass é o "construtor" da classe TestCase

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_remove_nulls_and_duplicates(self):
        # Testando remoção de nulos e duplicados
        transformer = RemoveNullsAndDuplicates(subset_cols=["client_ip", "timestamp"])
        df_transformed = transformer._transform(self.df) # passa o df para o método _transform

        # deve remover um nulo e um valor duplicado
        self.assertEqual(df_transformed.count(), 2)

    def test_normalize_timestamp(self):
        # Normalalização da coluna timestamp
        transformer = NormalizeTimestamp(input_col="timestamp", output_col="timestamp_normalized")
        df_transformed = transformer._transform(self.df)

        # Verifica conversão
        normalized_row = df_transformed.select("timestamp_normalized").collect()[0] #pegando a primeira linha 
        self.assertIsNotNone(normalized_row.timestamp_normalized) # verificando se o valor existe

    def test_categorize_response_code(self):
        # Testando a criação das categorias
        transformer = CategorizeResponseCode(input_col="response_code", output_col="response_category")
        df_transformed = transformer._transform(self.df)

        # Verificando se as categorias foram criadas de forma correta pelos seus respectivos status code.
        response_categories = df_transformed.select("response_category").distinct().collect()
        categories = {row.response_category for row in response_categories} #pegando os valores do array gerado

        expected_categories = {"Success", "Client Error", "Server Error"}
        self.assertEqual(categories, expected_categories) #verificando se os valores são iguais

    def test_count_access_by_ip(self):
        # Testando contagem de IP
        transformer = CountAccessByIP(group_col="client_ip", output_col="access_count_per_ip")
        df_transformed = transformer._transform(self.df)

        # verificando count para "192.168.0.1"
        ip_count_1 = df_transformed.filter(df_transformed.client_ip == "192.168.0.1").select("access_count_per_ip").collect()[0][0] #pegando o valor 
        self.assertEqual(ip_count_1, 2)

       
        ip_count_2 = df_transformed.filter(df_transformed.client_ip == "192.168.0.2").select("access_count_per_ip").collect()[0][0]
        self.assertEqual(ip_count_2, 1)


if __name__ == "__main__":
    unittest.main()
