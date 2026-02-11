from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    encode,
    decode,
    regexp_replace,
    translate,
    to_date,
    when,
    lit,
    trim,
)

# ---------------------------------------
# Project Functions
# ---------------------------------------

def latin1_to_utf8(df: DataFrame, columns: list[str]) -> DataFrame:
    """
    Converte colunas string de ISO-8859-1 (latin1) para UTF-8.

    Parâmetros:
        df (DataFrame): DataFrame com os dados
        columns (list[str]): Lista com os nomes das colunas a serem convertidas

    Retorno:
        DataFrame: DataFrame com as colunas convertidas
    """
    for column_name in columns:
        df = df.withColumn(
            column_name,
            decode(encode(col(column_name), "ISO-8859-1"), "UTF-8")
        )
    return df


def strip_df(df: DataFrame, columns: list[str]) -> DataFrame:
    """
    Aplica trim em cada coluna informada (remove espaços em branco dos lados).
    """
    for column_name in columns:
        df = df.withColumn(column_name, trim(col(column_name)))
    return df


def remove_accents(df: DataFrame, columns: list[str]) -> DataFrame:
    """
    Remove acentos e caracteres não alfanuméricos (mantém espaço e [a-zA-Z0-9]).

    Observação: funciona melhor se as colunas já estiverem normalizadas como string.
    """
    accented = "áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ"
    unaccented = "aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC"

    for column_name in columns:
        df = df.withColumn(
            column_name,
            regexp_replace(
                translate(col(column_name), accented, unaccented),
                r"[^a-zA-Z0-9\s]",
                ""
            )
        )
    return df


def cast_dates(df: DataFrame, columns: list[str], input_format: str = "yyyyMMdd") -> DataFrame:
    """
    Converte colunas para DateType usando o formato informado (padrão: 'yyyyMMdd').
    """
    for column_name in columns:
        df = df.withColumn(
            column_name,
            to_date(col(column_name).cast("string"), input_format)
        )
    return df


def normalize_blanks_to_null(df: DataFrame, columns: list[str] | None) -> DataFrame:
    """
    Converte para null:
      - valores já nulos (None/null)
      - strings vazias ("")
      - strings contendo apenas espaços (após trim)

    Aplica somente em colunas string.
    Se 'columns' for None, aplica a todas as colunas string do DataFrame.
    Se 'columns' contiver colunas não-string, elas serão ignoradas.
    """
    # Define colunas alvo
    if columns is None:
        target_cols = [c for c, t in df.dtypes if t == "string"]
    else:
        string_cols = {c for c, t in df.dtypes if t == "string"}
        target_cols = [c for c in columns if c in string_cols]

    for c in target_cols:
        df = df.withColumn(
            c,
            when(col(c).isNull() | (trim(col(c)) == ""), None).otherwise(col(c))
        )
    return df


def non_values_to_not_informed(
    df: DataFrame,
    columns: list[str] | None,
    target_values: list[str]
) -> DataFrame:
    """
    Converte para 'NAO INFORMADO':
      - valores contidos em 'target_values' (comparação exata)
      - strings com valor "0"
      - strings vazias ou apenas espaços (após trim)

    Aplica somente em colunas string.
    Se 'columns' for None, aplica a todas as colunas string do DataFrame.
    Se 'columns' contiver colunas não-string, elas serão ignoradas.
    """
    if columns is None:
        target_cols = [c for c, t in df.dtypes if t == "string"]
    else:
        string_cols = {c for c, t in df.dtypes if t == "string"}
        target_cols = [c for c in columns if c in string_cols]

    # Normaliza lista de alvos (evita None) e acrescenta "0" por padrão
    targets = set(target_values or [])
    targets.add("0")

    for c in target_cols:
        # Primeiro, marca vazios/brancos como "NAO INFORMADO"
        df = df.withColumn(
            c,
            when(col(c).isNull() | (trim(col(c)) == ""), lit("NAO INFORMADO")).otherwise(col(c))
        )
        # Em seguida, substitui alvos exatos por "NAO INFORMADO"
        for val in targets:
            df = df.withColumn(
                c,
                when(col(c) == lit(val), lit("NAO INFORMADO")).otherwise(col(c))
            )
    return df

from pyspark.sql.functions import (
    input_file_name,
    current_timestamp,
    col,
    lit
)

def ingestion_bronze(
    input_path,
    output_path,
    tipo_operacao,
    expected_cols,
    schema
):

    arquivos_novos = obter_arquivos_novos(input_path)

    if not arquivos_novos:
        print("✔ Nenhum arquivo novo para processar")
        return

    for a in arquivos_novos:

        print(f"Processando: {a['name']}")

        # === leitura robusta com aspas e multiline ===
        df_raw, df_corrupt, cols = read_csv_with_quotes(
            spark=spark,
            input_path=a["path"],
            delimiter=";",
            encoding="iso-8859-1",
            recursive=False,
            header=True,
            schema=schema,
            expected_cols=expected_cols,
            multiline=True,
            quote="\"",
            escape="\"",
            mode="PERMISSIVE",
            corrupt_col="_corrupt_record",
            ignore_leading_trailing_ws=True
        )

        # Bronze enrichment
        df_bronze = (
            df_raw
            .withColumn("origin_path_name", input_file_name())
            .withColumn("ingestion_dt", current_timestamp())
        )

        # Registro de controle SOMENTE após sucesso
        registrar_arquivo_processado(a, df_bronze)

    