# project-insight-lab-databricks

# Plataforma de Dados CNPJ e Balança Comercial

Plataforma analítica de dados públicos desenvolvida em Azure Databricks utilizando arquitetura Medallion (Bronze, Silver, Gold) para processamento e análise de dados de CNPJ e Balança Comercial do Brasil.

---

## Índice

- Visão Geral
- Arquitetura
- Estrutura do Projeto
- Pré-requisitos
- Instalação e Configuração

---

## Visão Geral

Esta plataforma centraliza e processa dados públicos de CNPJ (Receita Federal) e Balança Comercial (Comex Stat) em uma arquitetura moderna de lakehouse, permitindo análises rápidas, confiáveis e escaláveis.

### Principais Funcionalidades

- Ingestão automatizada e incremental de dados
- Processamento distribuído com Apache Spark
- Validação e controle de qualidade
- Modelo dimensional para análise
- Consultas otimizadas via SQL Warehouse
- Rastreabilidade completa de dados

### Tecnologias Utilizadas

- **Azure Databricks** - Processamento distribuído
- **Delta Lake** - Camada transacional ACID
- **Apache Spark** - Engine de processamento
- **Hive Metastore** - Catálogo de metadados
- **SQL Warehouse** - Engine de consulta
- **Azure Blob Storage** - Armazenamento de dados

---

## Arquitetura

A solução segue o padrão **Medallion Architecture** com três camadas: Bronze, Silver e Gold 

### Descrição das Pastas

#### Config/

Contém arquivos de configuração centralizados:

- **spark_config.py**: Configurações do Spark (memória, partições, otimizações)
- **storage_config.py**: Definição de paths para Blob Storage e camadas

#### Utils/

Funções auxiliares reutilizáveis:

- **incremental_bronze.py**: Função `read_csv_with_quotes` para leitura robusta de CSV com tratamento de delimitadores dentro de aspas

#### Bronze/

Notebooks de ingestão de dados brutos:

- **ingestion_cnpj.py**: Processa arquivos ZIP de CNPJ (Empresas, Estabelecimentos, Sócios)
- **ingestion_balancacomercial.py**: Processa arquivos CSV de Importação/Exportação

Características:
- Controle incremental (evita reprocessamento)
- Tratamento de registros corrompidos (quarentena)
- Metadados de rastreabilidade

#### Silver/

Notebooks de limpeza e padronização:

- **silver_cnpj.py**: Limpeza e validação de dados CNPJ
- **silver_balancacomercial.py**: Padronização de dados de comércio exterior

Processos:
- Conversão de tipos
- Eliminação de duplicidades
- Validações de negócio
- Padronização de valores

#### Gold/

Notebooks de modelagem dimensional:

- **gold_cnpj.py**: Criação de dimensões e fatos CNPJ
- **gold_balancacomercial.py**: Modelo estrela para análise de comércio exterior

Entregas:
- Dimensões com chave substituta
- Tabelas fato otimizadas
- Registro no Hive Metastore

---

## Pré-requisitos

### Infraestrutura

- Azure Subscription ativa
- Azure Databricks Workspace
- Azure Blob Storage configurado
- Databricks SQL Warehouse (Serverless recomendado)

### Permissões

- Acesso de leitura ao Blob Storage
- Permissão para criar tabelas no Hive Metastore
- Permissão para executar clusters Databricks


## Instalação e Configuração

### 1. Clone o Repositório no Databricks

```bash
# No Databricks Repos
git clone <url-do-repositorio>
```

### 2. Configure os Paths de Storage

Edite o arquivo `Config/storage_config.py`:

```python
# Paths do Azure Blob Storage
cnpj_path = "abfss://container@storageaccount.dfs.core.windows.net/cnpj/"
balanca_comercial_path = "abfss://container@storageaccount.dfs.core.windows.net/balanca/"

# Paths das camadas
bronze_path = "abfss://container@storageaccount.dfs.core.windows.net/bronze/"
silver_path = "abfss://container@storageaccount.dfs.core.windows.net/silver/"
gold_path = "abfss://container@storageaccount.dfs.core.windows.net/gold/"
```

### 3. Configure o Cluster

Crie um cluster com as seguintes especificações:

```yaml
Cluster Name: data-processing-cluster
Databricks Runtime: 13.3 LTS (Scala 2.12, Spark 3.4.1)
Node Type: Standard_D4as_v5
Workers: 6
Auto Termination: 30 minutos
```

### 4. Configure Credenciais de Acesso

No arquivo `Config/spark_config.py`, configure o acesso ao storage:

```python
def apply_storage_config(spark):
    spark.conf.set(
        "fs.azure.account.key.<storage-account>.dfs.core.windows.net",
        "<access-key>"
    )
```
