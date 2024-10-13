# CodeAssignment Documentation

### Introdução

Este projeto tem como objetivo realizar análises de um arquivo com o padrão **Web Server Access Log** utilizando Pyspark, Delta Lake e Databricks. Ele segue uma abordagem ETL (Extract, Transform, Load), transformando dados brutos (camada bronze) em dados prontos para análise (camadas silver e gold), otimizando o armazenamento e a consulta por meio de modelos dimensionais e tabelas fato.

O projeto também integra com GitHub Actions para automação de tarefas de CI/CD, garantindo que os processos de sincronização e deploy de jobs no Databricks sejam executados de maneira contínua. O foco principal é garantir que os dados estejam preparados para análise, oferecendo suporte para futuras integrações com ferramentas de Business Intelligence (BI) e outras análises de dados.

### Índice

- Estrutura do Projeto
- Instalação e Configuração do Ambiente
- Escolha das Tecnologias
- Configuração e Execução no Databricks
- Operações de Escrita e Leitura com o Delta Lake

### Estrutura do Projeto

O projeto segue a seguinte estrutura de diretórios:

```css

CodeAssignment/
│
├── github/
|   |──workflows/
|        ├──main.yaml  /* Arquivo de orquestração da esteira CI/CD */
|   
├── data/
|   |──raw/ /* Arquivos de extensão 7z */
|   |──processed/ /* Arquivo txt descompactado */
|   
|   
├── src/
│   ├── bronze/
│   │   ├── ingestao.py /* Responsável pela ingestão dos dados na camada bronze */
│   │   
│   ├── silver/
│   │   └── ETL.py /* Responsável pela execução do Pipeline de ETL e ingestão dos dados na camada silver */
|   |   
│   └── gold/
│   |    └── ingestao.py /* Responsável pela ingestão dos dados na camada gold */
|   |    └── query´s.sql /* Query SQL utilizada na geração das tabelas no modelo dimensional */
|   |
│   └──workflows/
|   |  └── main.py /* Orquestração do Workflow e Jobs  */
|   |  └── access_logs.json /* JSON clone do Workflow utilizado para realizar alterações localmente  */
|   |  └── requirements.txt /* Arquivo listando os módulos necessários para execução  */
|   |  └──.env /* Responsável por armazenar as variáveis de ambiente  */
|   |
|   |
|   └──test/
|   |   └── main.py /* Arquivo que contém os testes unitários  */
|   |
|   ├── lib/
│        └── ETL.py  /* Arquivo que contém as classes de Transformação  */
│        └── ingestors.py /* Arquivo que contém as classes de Ingestão full-load e dimensional  */
|        └── pipeline.py  /* Arquivo que contém a classe de criação do Pipeline  */
|        └── utils.py /* Arquivo que contém métodos que são usados frequentemente no projeto  */
|
│
├── .gitignore 
├── README.md
└── LICENSE

```

### Instalação e Configuração do Ambiente

##### Pré-requisitos    
Certifique-se de ter os seguintes softwares instalados:
 -  Python 3.12.4
 - Pip (gerenciador de pacotes do Python)
 - Databricks (acesso a uma conta)
 - Git (para versionamento de código)

### Passos de Instalação

1. Clone este repositório em sua máquina local:

```bash
    git clone https://github.com/ViniciusOtoni/CodeAssignment.git
```

2. Navegue até o diretório do projeto:

```bash
    cd CodeAssignment
```

3. Instale as dependências do projeto:

```bash
    pip install -r requirements.txt   
```

4. Configure os secrets no GitHub Actions para rodar o pipeline CI/CD:

 - Adicione as variáveis DATABRICKS_HOST e DATABRICKS_TOKEN nas configurações do repositório no GitHub:
   - Vá até Settings -> Secrets and variables -> Actions -> New repository secret.

### Escolha das Tecnologias

As tecnologias utilizadas neste projeto foram selecionadas com base nos seguintes critérios:

 - Python: Linguagem flexível e amplamente utilizada para tarefas de ETL e processamento de dados.

 - Delta Lake: Utilizado para armazenar dados de maneira eficiente, garantindo versionamento, controle transacional (ACID), suporte para operações otimizadas e time travel.

 - GitHub Actions: Ferramenta de CI/CD para automação de testes e deploys, escolhida pela integração fácil com o GitHub e suporte robusto para automação de pipelines.

 - Databricks: Plataforma de análise unificada que oferece uma maneira eficiente de processar grandes volumes de dados, realizar ETL e análise de dados em escala.

 ### Configuração e Execução no Databricks

 ##### Configuração do Databricks

 1. Gerar um Token de Acesso no Databricks:
    - Vá para User Settings no Databricks.
    - Na aba Access Tokens, clique em Generate New Token e copie o token gerado.

2. Configurar Secrets no GitHub Actions:
    - Vá até as configurações do seu repositório no GitHub.
    - Em Settings > Secrets > Actions, adicione os seguintes secrets:
        - DATABRICKS_HOST: O host da sua instância do Databricks.
        - DATABRICKS_TOKEN: O token gerado no Databricks.


### Executando o Projeto no Databricks

1. Configurar o Cluster no Databricks:
    - Crie um cluster no Databricks com o runtime compatível com a versão do Python especificada (3.12.4).
    - Adicione as dependências do requirements.txt ao cluster.
    - Adicione a variável de ambiente BLOB_STORAGE_ACCOUNT_KEY para ter acesso ao Blob Storage Account.
        - Pesquise por Conta de Armazenamento > Chaves de Acesso > Copiar > Colar o valor na variável de ambiente do Cluster.

2. Executar o Notebook:
    - Execute as células do notebook, que conterá ingestão dos dados, o pipeline de ETL para processar os dados, utilizar o Delta Lake e salvar os resultados.


### Operações de Escrita e Leitura com o Delta Lake

##### Escrita de Dados no Delta Lake

Para gravar dados em um Delta Table, basta utilizar o método .write com o formato delta:

```spark
    (df.coalesce(1)
            .write
            .format('delta')
            .mode("overwrite")
            .saveAsTable(f"{catalogo}.{database}.{tablename}))"
```

 - format("delta"): Define o formato Delta.
 - mode("overwrite"): Substitui os dados anteriores (Sobrescreve).
 - coalesce(1): Define a quantidade de partições.

##### Leitura de Dados no Delta Lake

Para ler dados de um Delta Table, pode se utilizar uma Query SQL:

```spark
    spark.sql("SELECT * FROM catalogo.database.tabela").show()
```

Com esta documentação, você tem todas as instruções necessárias para configurar, executar e entender as decisões tecnológicas do projeto. Para mais informações, consulte os arquivos do repositório e as documentações das tecnologias utilizadas.
