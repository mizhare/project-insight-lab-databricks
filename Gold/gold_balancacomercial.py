# Databricks notebook source
# MAGIC %md
# MAGIC ### Importação de configurações e funções

# COMMAND ----------

import sys
sys.path.append("/Workspace/Users/kgenuins@emeal.nttdata.com/project-insight-lab-databricks")

from Config.spark_config import apply_storage_config
from Config.storage_config import *

apply_storage_config(spark)

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    when,
    lit,
    trim,
    upper,
    lower,
    coalesce,
    concat,
    substring,
    length,
    cast,
    round,
    sum as spark_sum,
    count,
    avg,
    max as spark_max,
    min as spark_min,
    countDistinct,
    current_timestamp,
    year,
    month,
    datediff,
    to_date,
    date_format,
    rank,
    dense_rank,
    lag,
    lead,
    sha2,
    concat_ws,
    ceil
)

from pyspark.sql.types import (
    StringType,
    IntegerType,
    DoubleType,
    DateType,
    TimestampType,
    DecimalType
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definição dos paths

# COMMAND ----------

path_storage_silver = f"{silver_path}"
path_storage_gold = f"{gold_path}"

print(f"Silver path: {path_storage_silver}")
print(f"Gold path: {path_storage_gold}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criação da tabela de controle de transformações Gold

# COMMAND ----------

def criar_tabela_controle_gold():
    """
    Cria tabela para rastrear transformações realizadas na Gold
    """
    spark.sql("""
        CREATE TABLE IF NOT EXISTS gold.transformacoes_processadas (
            tabela_gold STRING,
            data_ultima_transformacao TIMESTAMP,
            total_registros BIGINT,
            data_processamento TIMESTAMP
        )
        USING DELTA
    """)
    print("Tabela de controle de transformações Gold criada!")

criar_tabela_controle_gold()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Funções auxiliares para transformações

# COMMAND ----------

def obter_data_ultima_transformacao_gold(tabela_gold):
    """
    Obtém a data da última transformação realizada para uma tabela na Gold
    """
    try:
        resultado = spark.sql(f"""
            SELECT MAX(data_ultima_transformacao) as ultima_data
            FROM gold.transformacoes_processadas
            WHERE tabela_gold = '{tabela_gold}'
        """).collect()
        
        if resultado and resultado[0].ultima_data:
            return resultado[0].ultima_data
        else:
            return None
    except:
        return None

# COMMAND ----------

def registrar_transformacao_gold(tabela_gold, total_registros):
    """
    Registra transformação realizada na Gold
    """
    spark.sql(f"""
        INSERT INTO gold.transformacoes_processadas
        VALUES (
            '{tabela_gold}',
            current_timestamp(),
            {total_registros},
            current_timestamp()
        )
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dimensão TEMPO

# COMMAND ----------

# DBTITLE 1,dim_tempo
print("=" * 60)
print("CRIANDO DIMENSÃO TEMPO")
print("=" * 60)

# Ler dados de EXP e IMP da Silver
df_exp_tratado = spark.read.format("delta").load(f"{path_storage_silver}/balancacomercial/exp_tratado")
df_imp_tratado = spark.read.format("delta").load(f"{path_storage_silver}/balancacomercial/imp_tratado")

# Extrair datas únicas
datas_exp = df_exp_tratado.select("CO_ANO", "CO_MES").distinct()
datas_imp = df_imp_tratado.select("CO_ANO", "CO_MES").distinct()

df_datas_base = datas_exp.union(datas_imp).distinct()

print(f"Total de períodos únicos: {df_datas_base.count()}")

# COMMAND ----------

# Criar dimensão tempo com cálculos de trimestre e semestre
dim_tempo = (
    df_datas_base
    .withColumn("CO_TRIMESTRE", ceil(col("CO_MES") / 3))
    .withColumn("CO_SEMESTRE", ceil(col("CO_MES") / 6))
    .withColumn("NO_TRIMESTRE", concat(col("CO_TRIMESTRE"), lit("º Trimestre")))
    .withColumn("NO_SEMESTRE", concat(col("CO_SEMESTRE"), lit("º Semestre")))
    .withColumn("sk_tempo", sha2(concat_ws("||", col("CO_ANO"), col("CO_MES")), 256))
    .withColumn("data_criacao", current_timestamp())
    .orderBy("CO_ANO", "CO_MES")
)

print(f"Dimensão TEMPO criada: {dim_tempo.count()} registros")

# COMMAND ----------

# Escrita incremental 
(
    dim_tempo
    .write
    .format("delta")
    .mode("overwrite")
    .save(f"{path_storage_gold}dim_tempo/")
)

# Criar tabela se não existir
dim_tempo.write.format("delta").mode("overwrite").saveAsTable("gold.dim_tempo")


registrar_transformacao_gold("dim_tempo", dim_tempo.count())

print(" Dimensão TEMPO salva em Gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dimensão PRODUTO

# COMMAND ----------

# DBTITLE 1,dim_produto
print("=" * 60)
print("CRIANDO DIMENSÃO PRODUTO")
print("=" * 60)

# Carregar tabelas de referência da Silver
ncm_tratado = spark.read.format("delta").load(f"{path_storage_silver}/balancacomercial/ncm_tratado")
ncm_sh_tratado = spark.read.format("delta").load(f"{path_storage_silver}/balancacomercial/ncm_sh_tratado")
ncm_isic_tratado = spark.read.format("delta").load(f"{path_storage_silver}/balancacomercial/ncm_isic_tratado")
ncm_cgce_tratado = spark.read.format("delta").load(f"{path_storage_silver}/balancacomercial/ncm_cgce_tratado")
ncm_fat_agreg_tratado = spark.read.format("delta").load(f"{path_storage_silver}/balancacomercial/ncm_fat_agreg_tratado")

print(f" NCM: {ncm_tratado.count()} registros")
print(f" NCM_SH: {ncm_sh_tratado.count()} registros")
print(f" NCM_ISIC: {ncm_isic_tratado.count()} registros")
print(f" NCM_CGCE: {ncm_cgce_tratado.count()} registros")
print(f" NCM_FAT_AGREG: {ncm_fat_agreg_tratado.count()} registros")

# COMMAND ----------

# Realizar joins para criar dimensão produto
dim_produto = (
    ncm_tratado
    .join(ncm_sh_tratado, on="CO_SH6", how="left")
    .join(ncm_isic_tratado, on="CO_ISIC_CLASSE", how="left")
    .join(ncm_cgce_tratado, on="CO_CGCE_N3", how="left")
    .join(ncm_fat_agreg_tratado, on="CO_FAT_AGREG", how="left")
    .withColumn("sk_produto", sha2(concat_ws("||", col("CO_NCM")), 256))
    .withColumn("data_criacao", current_timestamp())
)

print(f" Dimensão PRODUTO criada: {dim_produto.count()} registros")

# COMMAND ----------

# Escrita incremental (append) na Gold
(
    dim_produto
    .write
    .format("delta")
    .mode("append")
    .save(f"{path_storage_gold}dim_produto/")
)

# Criar tabela se não existir
dim_produto.write.format("delta").mode("overwrite").saveAsTable("gold.dim_produtos")


registrar_transformacao_gold("dim_produto", dim_produto.count())

print("Dimensão PRODUTO salva em Gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dimensão LOCALIDADE_BR

# COMMAND ----------

# DBTITLE 1,dim_localidade
print("=" * 60)
print("CRIANDO DIMENSÃO LOCALIDADE_BR")
print("=" * 60)

# Carregar tabelas de referência da Silver
uf_mun_tratado = spark.read.format("delta").load(f"{path_storage_silver}/balancacomercial/uf_mun_tratado")
uf_tratado = spark.read.format("delta").load(f"{path_storage_silver}/balancacomercial/uf_tratado")

print(f"UF_MUN: {uf_mun_tratado.count()} registros")
print(f"UF: {uf_tratado.count()} registros")

# COMMAND ----------

# Realizar join para criar dimensão localidade
dim_localidade_br = (
    uf_mun_tratado
    .join(uf_tratado, on="SG_UF", how="left")
    .select(
        uf_mun_tratado.CO_MUN_GEO,
        uf_mun_tratado.NO_MUN,
        uf_mun_tratado.SG_UF,
        uf_tratado.CO_UF,
        uf_tratado.NO_UF,
        uf_tratado.NO_REGIAO
    )
    .withColumn("sk_localidade", sha2(concat_ws("||", col("NO_UF"), col("CO_MUN_GEO")), 256))
    .withColumn("data_criacao", current_timestamp())
)

print(f" Dimensão LOCALIDADE_BR criada: {dim_localidade_br.count()} registros")

# COMMAND ----------

# Escrita incremental (append) na Gold
(
    dim_localidade_br
    .write
    .format("delta")
    .mode("overwrite")
    .save(f"{path_storage_gold}dim_localidade_br/")
)

# Criar tabela se não existir

dim_localidade_br.write.format("delta").mode("overwrite").saveAsTable("gold.dim_localidades_br")

registrar_transformacao_gold("dim_localidade_br", dim_localidade_br.count())

print(" Dimensão LOCALIDADE_BR salva em Gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dimensão GEOGRAFIA

# COMMAND ----------

# DBTITLE 1,dim_geografia
print("=" * 60)
print("CRIANDO DIMENSÃO GEOGRAFIA")
print("=" * 60)

# Carregar tabelas de referência da Silver
pais_tratado = spark.read.format("delta").load(f"{path_storage_silver}/balancacomercial/pais_tratado")

print(f" PAIS: {pais_tratado.count()} registros")

# COMMAND ----------

# Realizar join para criar dimensão geografia
dim_geografia = pais_tratado.withColumn("sk_geografia", sha2(concat_ws("||", col("CO_PAIS")), 256)).withColumn("data_criacao", current_timestamp())

print(f" Dimensão GEOGRAFIA criada: {dim_geografia.count()} registros")

# COMMAND ----------

# Escrita incremental (append) na Gold
(
    dim_geografia
    .write
    .format("delta")
    .mode("overwrite")
    .save(f"{path_storage_gold}dim_geografia/")
)

# Criar tabela se não existir
dim_geografia.write.format("delta").mode("overwrite").saveAsTable("gold.dim_geografia")


registrar_transformacao_gold("dim_geografia", dim_geografia.count())

print(" Dimensão GEOGRAFIA salva em Gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fato BALANCA_COMERCIAL_EXP 

# COMMAND ----------

# DBTITLE 1,fato_exp
print("=" * 60)
print("CRIANDO FATO BALANCA_COMERCIAL_EXP")
print("=" * 60)

# ler dados de dimensões
dim_produto = spark.read.format("delta").load(f"{path_storage_gold}/dim_produto")
dim_geografia = spark.read.format("delta").load(f"{path_storage_gold}/dim_geografia")
dim_tempo = spark.read.format("delta").load(f"{path_storage_gold}/dim_tempo")

ultima_transformacao_exp = obter_data_ultima_transformacao_gold("fato_balanca_exp")

# Ler dados de EXP da Silver criação da fato(dim_tempo,dim_produtodim_geografia )
df_exp = spark.read.format("delta").load(f"{path_storage_silver}/balancacomercial/exp_tratado")


if ultima_transformacao_exp:
    df_exp = df_exp.filter(col("DATA_TRANSFORMACAO") > ultima_transformacao_exp)
    print(f" Filtrando EXP desde: {ultima_transformacao_exp}")
else:
    print(" Primeira execução: processando todos os EXP")

print(f"Total de registros EXP para processar: {df_exp.count()}")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from gold.transformacoes_processadas

# COMMAND ----------

# Criar fato com joins às dimensões
fato_exp = (
    df_exp
    .join(dim_tempo, on=["CO_ANO", "CO_MES"], how="left")
    .join(dim_produto, on="CO_NCM", how="left")
    .join(dim_geografia, on="CO_PAIS", how="left")
    .select(
        col("CO_ANO"),
        col("CO_MES"),
        col("CO_NCM"),
        col("CO_PAIS"),
        col("SG_UF_NCM"),
        col("QT_ESTAT").alias("quantidade_estatistica"),
        col("KG_LIQUIDO").alias("peso_liquido_kg"),
        col("VL_FOB").alias("valor_fob"),
        col("TIPO_OPERACAO"),
        current_timestamp().alias("data_processamento_fato"),
        col("sk_tempo").alias("fk_tempo"),
        col("sk_produto").alias("fk_produto"),
        col("sk_geografia").alias("fk_geografia")
    )
)

print(f" Fato EXP criado: {fato_exp.count()} registros")

# COMMAND ----------

display(fato_exp.limit(10))

# COMMAND ----------

# Escrita incremental (append) na Gold
(
    fato_exp
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("CO_ANO", "CO_MES")
    .save(f"{path_storage_gold}fato_balanca_exp/")
)

# Criar tabela se não existir

fato_exp.write.format("delta").mode("overwrite").saveAsTable("gold.fato_exp")

registrar_transformacao_gold("fato_exp", fato_exp.count())

print(" Fato BALANCA_COMERCIAL_EXP salvo em Gold")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.fato_exp limit 10;

# COMMAND ----------

from pyspark.sql.functions import sum

display(fato_exp.agg(sum(col("valor_fob")).alias("soma_valor_fob")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fato BALANCA_COMERCIAL_IMP

# COMMAND ----------

print("=" * 60)
print("CRIANDO FATO BALANCA_COMERCIAL_IMP")
print("=" * 60)

# ler dados de dimensões
dim_produto = spark.read.format("delta").load(f"{path_storage_gold}/dim_produto")
dim_geografia = spark.read.format("delta").load(f"{path_storage_gold}/dim_geografia")
dim_tempo = spark.read.format("delta").load(f"{path_storage_gold}/dim_tempo")

ultima_transformacao_imp = obter_data_ultima_transformacao_gold("fato_balanca_imp")

# Ler dados de IMP da Silver
df_imp = spark.read.format("delta").load(f"{path_storage_silver}/balancacomercial/imp_tratado")

print(f"Total de registros IMP para processar: {df_imp.count()}")

# COMMAND ----------

dim_produto = spark.read.format("delta").load(f"{path_storage_gold}/dim_produto")
dim_geografia = spark.read.format("delta").load(f"{path_storage_gold}/dim_geografia")
dim_tempo = spark.read.format("delta").load(f"{path_storage_gold}/dim_tempo")


# COMMAND ----------

# Criar fato com joins às dimensões
fato_imp = (
    df_imp
    .join(dim_tempo, on=["CO_ANO", "CO_MES"], how="left")
    .join(dim_produto, on="CO_NCM", how="left")
    .join(dim_geografia, on="CO_PAIS", how="left")
    .select(
        col("CO_ANO"),
        col("CO_MES"),
        col("CO_NCM"),
        col("CO_PAIS"),
        col("SG_UF_NCM"),
        col("QT_ESTAT").alias("quantidade_estatistica"),
        col("KG_LIQUIDO").alias("peso_liquido_kg"),
        col("VL_FOB").alias("valor_fob"),
        col("VL_FRETE").alias("valor_frete"),
        col("VL_SEGURO").alias("valor_seguro"),
        col("TIPO_OPERACAO"),
        current_timestamp().alias("data_processamento_fato"),
        col("sk_tempo").alias("fk_tempo"),
        col("sk_produto").alias("fk_produto"),
        col("sk_geografia").alias("fk_geografia")
    )
)

print(f" Fato IMP criado: {fato_imp.count()} registros")

# COMMAND ----------

dim_tempo.columns

# COMMAND ----------

# Criar fato com joins às dimensões
dim_tempo_alias = dim_tempo.alias("dt")
dim_produto_alias = dim_produto.alias("dp")
dim_geografia_alias = dim_geografia.alias("dg")

fato_imp = (
    df_imp
    .join(dim_tempo_alias, on=["CO_ANO", "CO_MES"], how="left")
    .join(dim_produto_alias, on="CO_NCM", how="left")
    .join(dim_geografia_alias, on="CO_PAIS", how="left")
    .select(
        col("CO_ANO"),
        col("CO_MES"),
        col("CO_NCM"),
        col("CO_PAIS"),
        col("SG_UF_NCM"),
        col("QT_ESTAT").alias("quantidade_estatistica"),
        col("KG_LIQUIDO").alias("peso_liquido_kg"),
        col("VL_FOB").alias("valor_fob"),
        col("VL_FRETE").alias("valor_frete"),
        col("VL_SEGURO").alias("valor_seguro"),
        col("TIPO_OPERACAO"),
        current_timestamp().alias("data_processamento_fato"),
        col("dt.sk_tempo").alias("fk_tempo"),
        col("dp.sk_produto").alias("fk_produto"),
        col("dg.sk_geografia").alias("fk_geografia")
    )
)

print(f" Fato IMP criado: {fato_imp.count()} registros")

# COMMAND ----------

dim_produto.groupBy("CO_NCM").count().filter("count > 1").count()

# COMMAND ----------

dim_geografia.groupBy("CO_PAIS").count().filter("count > 1").count()

# COMMAND ----------

dim_tempo.groupBy("CO_ANO","CO_MES").count().filter("count > 1").count()

# COMMAND ----------

print("\n AMOSTRA FATO_BALANCA_IMP:")
spark.table("gold.fato_imp").limit(20).display()

# COMMAND ----------

# Escrita incremental (append) na Gold
(
    fato_imp
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("CO_ANO", "CO_MES")
    .save(f"{path_storage_gold}fato_balanca_imp/")
)

# Criar tabela se não existir
fato_imp.write.format("delta").mode("overwrite").saveAsTable("gold.fato_imp")

registrar_transformacao_gold("fato_balanca_imp", fato_imp.count())

print(" Fato BALANCA_COMERCIAL_IMP salvo em Gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validações e Estatísticas

# COMMAND ----------

print("=" * 60)
print("VALIDAÇÕES GOLD")
print("=" * 60)

# Validação dimensões
print("\n DIMENSÕES:")
print(f" DIM_TEMPO: {spark.table('gold.dim_tempo').count()} registros")
print(f" DIM_PRODUTO: {spark.table('gold.dim_produtos').count()} registros")
print(f" DIM_LOCALIDADE_BR: {spark.table('gold.dim_localidades_br').count()} registros")
print(f" DIM_GEOGRAFIA: {spark.table('gold.dim_geografia').count()} registros")

# Validação fatos
print("\n FATOS:")
print(f" FATO_BALANCA_EXP: {spark.table('gold.fato_exp').count()} registros")
print(f" FATO_BALANCA_IMP: {spark.table('gold.fato_imp').count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Estatísticas Finais

# COMMAND ----------

print("=" * 60)
print("ESTATÍSTICAS EXPORTAÇÃO (EXP)")
print("=" * 60)

df_stats_exp = spark.sql("""
    SELECT
        CO_ANO,
        CO_MES,
        COUNT(*) as total_registros,
        SUM(quantidade_estatistica) as total_quantidade,
        SUM(peso_liquido_kg) as total_peso_kg,
        SUM(valor_fob) as total_valor_fob,
        MAX(data_processamento_fato) as ultima_atualizacao
    FROM gold.fato_exp
    GROUP BY CO_ANO, CO_MES
    ORDER BY CO_ANO DESC, CO_MES DESC
""")

df_stats_exp.limit(5).display()

# COMMAND ----------

print("=" * 60)
print("ESTATÍSTICAS IMPORTAÇÃO (IMP)")
print("=" * 60)

df_stats_imp = spark.sql("""
    SELECT
        CO_ANO,
        CO_MES,
        COUNT(*) as total_registros,
        SUM(quantidade_estatistica) as total_quantidade,
        SUM(peso_liquido_kg) as total_peso_kg,
        SUM(valor_fob) as total_valor_fob,
        SUM(valor_frete) as total_frete,
        SUM(valor_seguro) as total_seguro,
        MAX(data_processamento_fato) as ultima_atualizacao
    FROM gold.fato_imp
    GROUP BY CO_ANO, CO_MES
    ORDER BY CO_ANO DESC, CO_MES DESC
""")

df_stats_imp.limit(5).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_balanca_comercial_consolidada AS
# MAGIC SELECT 
# MAGIC     fk_tempo, 
# MAGIC     fk_produto, 
# MAGIC     fk_geografia, 
# MAGIC     CO_ANO, 
# MAGIC     CO_MES,
# MAGIC     valor_fob, 
# MAGIC     peso_liquido_kg, 
# MAGIC     tipo_operacao, 
# MAGIC     data_processamento_fato,
# MAGIC     0 AS valor_frete,  -- Exportação não tem frete
# MAGIC     0 AS valor_seguro  -- Exportação não tem seguro
# MAGIC FROM gold.fato_exp
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     fk_tempo, 
# MAGIC     fk_produto, 
# MAGIC     fk_geografia, 
# MAGIC     CO_ANO, 
# MAGIC     CO_MES,
# MAGIC     valor_fob, 
# MAGIC     peso_liquido_kg, 
# MAGIC     tipo_operacao, 
# MAGIC     data_processamento_fato,
# MAGIC     valor_frete, 
# MAGIC     valor_seguro
# MAGIC FROM gold.fato_imp;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from gold.vw_balanca_comercial_consolidada 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT tipo_operacao, SUM(valor_fob), SUM(valor_fob) 
# MAGIC FROM gold.vw_balanca_comercial_consolidada 
# MAGIC where CO_ANO = 2025
# MAGIC GROUP BY tipo_operacao;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     tipo_operacao, 
# MAGIC     SUM(valor_fob) AS total_fob, 
# MAGIC     SUM(valor_frete) AS total_frete,
# MAGIC     COUNT(*) AS total_linhas -- Isso vai nos dizer se tem linhas demais
# MAGIC FROM gold.vw_balanca_comercial_consolidada 
# MAGIC WHERE CO_ANO = 2025 -- Se o nome da coluna de ano na sua Gold for diferente, ajuste aqui
# MAGIC GROUP BY tipo_operacao;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     CO_ANO, 
# MAGIC     COUNT(*) AS total_registros,
# MAGIC     SUM(valor_fob) AS soma_valor_fob,
# MAGIC     SUM(peso_liquido_kg) AS soma_peso
# MAGIC FROM gold.vw_balanca_comercial_consolidada
# MAGIC WHERE CO_ANO = 2021
# MAGIC GROUP BY CO_ANO;
