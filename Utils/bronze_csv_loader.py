# Utils/bronze_csv_loader.py
# -*- coding: utf-8 -*-

"""
Bronze CSV Loader (PySpark)
---------------------------

Duas abordagens para leitura robusta de CSV na camada Bronze:

1) read_csv_with_quotes (RECOMENDADO):
   - Usa o leitor nativo do Spark configurado para:
       * ignorar delimitadores dentro de aspas (quote)
       * suportar quebra de linha dentro de campos entre aspas (multiLine)
       * preservar codificação (ex.: 'iso-8859-1')
       * operar em modo 'PERMISSIVE' e isolar linhas corrompidas em _corrupt_record
   - Retorna: df_ok, df_corrupt, header_cols

2) sanitize_csv_bronze (Fallback - texto bruto):
   - Lê como texto, detecta cabeçalho, conta delimitadores, separa linhas boas/ruins,
     remove cabeçalhos duplicados (mesmo com pequenas variações) e monta o DataFrame limpo.
   - Retorna: df_clean, df_bad, header_cols

Requisitos:
  - Databricks / PySpark 3.x

"""

from typing import Optional, List, Tuple
from functools import reduce
import re

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType


# =============================================================================
# 1) Leitura robusta com aspas (NATIVO SPARK) - RECOMENDADO
# =============================================================================

def _align_to_expected_columns(df: DataFrame, expected_cols: List[str], strings_only: bool = True) -> DataFrame:
    """
    Garante que o DataFrame possua exatamente as colunas esperadas, na ordem esperada.
      - Adiciona colunas faltantes com null
      - Remove colunas extras
      - (opcional) Converte tudo para string
    """
    current = set(df.columns)
    expected = list(expected_cols)

    # adiciona colunas faltantes como null
    for c in expected:
        if c not in current:
            df = df.withColumn(c, F.lit(None).cast("string"))

    # remove colunas extras
    df = df.select(*expected)

    if strings_only:
        for c in expected:
            df = df.withColumn(c, F.col(c).cast("string"))
    return df


def _write_quarantine(df_bad: DataFrame, quarantine_path: str, mode: str = "overwrite") -> None:
    """
    Grava linhas corrompidas em JSON (uma linha por registro), preservando _corrupt_record e, se existir, path de origem.
    """
    if df_bad is None or df_bad.rdd.isEmpty():
        return
    cols = [c for c in df_bad.columns if c in ("_corrupt_record", "path", "arquivo", "linha", "motivo", "qt_delim")]
    if not cols:
        cols = ["_corrupt_record"]
    df_bad.select(*cols).write.mode(mode).option("compression", "none").json(quarantine_path)


def read_csv_with_quotes(
    spark: SparkSession,
    input_path: str,
    delimiter: str = ";",
    encoding: str = "iso-8859-1",
    recursive: bool = True,
    path_glob_filter: Optional[str] = None,
    header: bool = True,
    schema: Optional[StructType] = None,
    expected_cols: Optional[List[str]] = None,
    multiline: bool = True,
    quote: str = "\"",
    escape: str = "\"",  # em muitos datasets com CSV RFC, aspas internas são representadas por duas aspas ("")
    mode: str = "PERMISSIVE",
    corrupt_col: str = "_corrupt_record",
    ignore_leading_trailing_ws: bool = True,
    quarantine_path: Optional[str] = None,
    quarantine_mode: str = "overwrite",
) -> Tuple[DataFrame, DataFrame, List[str]]:
    """
    Lê CSV respeitando aspas (ignora delimitadores dentro de quotes) e suporta linhas multiLine.

    Parâmetros:
      - schema: se informado, desativa inferência e usa as colunas/ordem exatas
      - expected_cols: se informado SEM schema, alinha o DF às colunas esperadas (ordem + colunas faltantes/extra)
      - mode='PERMISSIVE' + _corrupt_record: separa linhas problemáticas para auditoria
      - quarantine_path: se informado, grava a quarentena em JSON

    Retorna:
      df_ok       : DataFrame com dados válidos
      df_corrupt  : DataFrame com linhas problemáticas (_corrupt_record não-nulo)
      header_cols : lista de colunas resultante (se schema/expected_cols não passados, vem do arquivo)
    """
    reader = (
        spark.read
             .option("header", str(header).lower())
             .option("delimiter", delimiter)
             .option("quote", quote)
             .option("escape", escape)
             .option("multiLine", str(multiline).lower())
             .option("encoding", encoding)
             .option("recursiveFileLookup", str(recursive).lower())
             .option("ignoreLeadingWhiteSpace", str(ignore_leading_trailing_ws).lower())
             .option("ignoreTrailingWhiteSpace", str(ignore_leading_trailing_ws).lower())
    )
    if path_glob_filter:
        reader = reader.option("pathGlobFilter", path_glob_filter)

    if mode:
        reader = reader.option("mode", mode)
    if corrupt_col:
        reader = reader.option("columnNameOfCorruptRecord", corrupt_col)

    if schema is not None:
        reader = reader.schema(schema)

    df = reader.csv(input_path)

    # separa linhas corrompidas, se a coluna existir
    if corrupt_col in df.columns:
        df_corrupt = df.filter(F.col(corrupt_col).isNotNull())
        df_ok      = df.filter(F.col(corrupt_col).isNull()).drop(corrupt_col)
    else:
        df_corrupt = spark.createDataFrame([], df.schema)
        df_ok = df

    # alinha colunas, se necessário
    header_cols = df_ok.columns
    if expected_cols and schema is None:
        df_ok = _align_to_expected_columns(df_ok, expected_cols, strings_only=True)
        header_cols = expected_cols

    # grava quarentena (se pedido)
    if quarantine_path and df_corrupt is not None and not df_corrupt.rdd.isEmpty():
        _write_quarantine(df_corrupt, quarantine_path, mode=quarantine_mode)

    return df_ok, df_corrupt, header_cols


# =============================================================================
# 2) Fallback: Sanitizador por contagem de delimitadores (TEXTO BRUTO)
# =============================================================================

def _read_binary_files(
    spark: SparkSession,
    input_path: str,
    recursive: bool = True,
    path_glob_filter: Optional[str] = None,
) -> DataFrame:
    reader = (
        spark.read.format("binaryFile")
        .option("recursiveFileLookup", str(recursive).lower())
    )
    if path_glob_filter:
        reader = reader.option("pathGlobFilter", path_glob_filter)
    return reader.load(input_path)


def _decode_to_lines(
    df_bin: DataFrame,
    encoding: str = "iso-8859-1",
    drop_empty_lines: bool = True,
) -> DataFrame:
    df_lines = (
        df_bin
        .select(
            F.col("path").alias("arquivo"),
            F.explode(F.split(F.decode(F.col("content"), encoding), "\n")).alias("linha")
        )
        .select(
            "arquivo",
            F.regexp_replace(F.col("linha"), r"\r$", "").alias("linha")
        )
    )
    if drop_empty_lines:
        df_lines = df_lines.filter(F.length(F.trim(F.col("linha"))) > 0)
    return df_lines


def _normalize_token_py(s: str) -> str:
    if s is None:
        return ""
    s = s.lstrip("\ufeff")
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    s = s.upper()
    return s


def _normalize_col(c):
    return F.upper(
        F.regexp_replace(
            F.trim(
                F.regexp_replace(c, u"^\ufeff", "")
            ),
            r"\s+",
            " "
        )
    )


def _detect_global_header(
    df_lines: DataFrame,
    delimiter: str = ";",
) -> Tuple[str, List[str], int, List[str]]:
    df_freq = (
        df_lines
        .withColumn(
            "delims",
            F.length(F.col("linha")) - F.length(F.regexp_replace(F.col("linha"), delimiter, ""))
        )
        .filter(F.col("delims") > 0)
        .groupBy("linha")
        .count()
        .orderBy(F.col("count").desc())
    )
    row = df_freq.limit(1).collect()
    if not row:
        raise ValueError("Não foi possível detectar o cabeçalho global. Verifique os arquivos de entrada.")

    header_str = row[0]["linha"]
    header_cols_raw = [c.strip() for c in header_str.split(delimiter)]
    header_norm_tokens = [_normalize_token_py(tok) for tok in header_cols_raw]
    expected_delims = len(header_cols_raw) - 1
    return header_str, header_norm_tokens, expected_delims, header_cols_raw


def _is_header_like_expr(line_col, header_tokens: List[str], delimiter: str):
    arr = F.split(line_col, delimiter, -1)
    comparisons = []
    for i, htok in enumerate(header_tokens):
        tkn = F.element_at(arr, i + 1)
        comparisons.append(_normalize_col(tkn) == F.lit(htok))
    if comparisons:
        all_equal = reduce(lambda a, b: a & b, comparisons)
    else:
        all_equal = F.lit(False)
    size_ok = F.size(arr) >= F.lit(len(header_tokens))
    return size_ok & all_equal


def _split_good_bad_lines(
    df_lines: DataFrame,
    header_str: Optional[str],
    header_norm_tokens: List[str],
    expected_delims: int,
    delimiter: str = ";",
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    df_counted = df_lines.withColumn(
        "qt_delim",
        F.length(F.col("linha")) - F.length(F.regexp_replace(F.col("linha"), delimiter, ""))
    )

    is_exact_header = F.lit(False) if header_str is None else (F.col("linha") == F.lit(header_str))
    is_header_like = _is_header_like_expr(F.col("linha"), header_norm_tokens, delimiter)

    df_dup_headers = (
        df_counted
        .filter(is_exact_header | is_header_like)
        .withColumn("motivo", F.lit("duplicate_header"))
        .select("arquivo", "linha", "qt_delim", "motivo")
    )

    df_bad_structure = (
        df_counted
        .filter((F.col("qt_delim") != F.lit(expected_delims)) & ~(is_exact_header | is_header_like))
        .withColumn("motivo", F.lit("bad_delimiter_count"))
        .select("arquivo", "linha", "qt_delim", "motivo")
    )

    df_good_lines = (
        df_counted
        .filter((F.col("qt_delim") == F.lit(expected_delims)) & ~(is_exact_header | is_header_like))
        .select("arquivo", "linha")
    )

    return df_good_lines, df_bad_structure, df_dup_headers


def _parse_good_lines_to_df(
    df_good_lines: DataFrame,
    header_cols: List[str],
    delimiter: str = ";",
) -> DataFrame:
    arr = F.split(F.col("linha"), delimiter, -1)
    selects = [F.element_at(arr, i + 1).alias(header_cols[i]) for i in range(len(header_cols))]
    df_clean = df_good_lines.select(*selects)
    return df_clean


def _filter_residual_exact_header_rows(df_clean: DataFrame, header_cols: List[str]) -> DataFrame:
    cond = None
    for i, col_name in enumerate(header_cols):
        expr = (F.col(col_name) == F.lit(header_cols[i]))
        cond = expr if cond is None else (cond & expr)
    if cond is None:
        return df_clean
    return df_clean.filter(~cond)


def sanitize_csv_bronze(
    spark: SparkSession,
    input_path: str,
    delimiter: str = ";",
    encoding: str = "iso-8859-1",
    recursive: bool = True,
    path_glob_filter: Optional[str] = None,
    drop_empty_lines: bool = True,
    quarantine_path: Optional[str] = None,
    quarantine_mode: str = "overwrite",
    forced_header_cols: Optional[List[str]] = None,
) -> Tuple[DataFrame, DataFrame, List[str]]:
    """
    Fallback para CSVs que NÃO usam aspas corretamente ou estão muito quebrados.

    Retorna:
      df_clean : DataFrame limpo (strings)
      df_bad   : Linhas rejeitadas (arquivo, linha, qt_delim, motivo)
      header_cols : nomes de colunas detectados/forçados
    """
    df_bin = _read_binary_files(
        spark=spark,
        input_path=input_path,
        recursive=recursive,
        path_glob_filter=path_glob_filter,
    )
    df_lines = _decode_to_lines(
        df_bin=df_bin,
        encoding=encoding,
        drop_empty_lines=drop_empty_lines,
    )

    if forced_header_cols and len(forced_header_cols) > 0:
        header_cols_raw = forced_header_cols
        header_norm_tokens = [_normalize_token_py(tok) for tok in header_cols_raw]
        expected_delims = len(header_cols_raw) - 1
        header_str = None
    else:
        header_str, header_norm_tokens, expected_delims, header_cols_raw = _detect_global_header(
            df_lines=df_lines,
            delimiter=delimiter,
        )

    df_good_lines, df_bad_structure, df_dup_headers = _split_good_bad_lines(
        df_lines=df_lines,
        header_str=header_str,
        header_norm_tokens=header_norm_tokens,
        expected_delims=expected_delims,
        delimiter=delimiter,
    )

    df_bad = df_bad_structure.unionByName(df_dup_headers)

    if quarantine_path and not df_bad.rdd.isEmpty():
        _write_quarantine(df_bad, quarantine_path, mode=quarantine_mode)

    df_clean = _parse_good_lines_to_df(df_good_lines, header_cols_raw, delimiter)
    df_clean = _filter_residual_exact_header_rows(df_clean, header_cols_raw)

    return df_clean, df_bad, header_cols_raw


# =============================================================================
# Execução manual / Exemplo de uso (opcional)
# =============================================================================
if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()

    # Exemplo 1: leitura com aspas (recomendada)
    input_path = "/mnt/bronze/ncm"
    quarantine = "/mnt/bronze_quarentena/ncm"

    # Se já souber as colunas (recomendado):
    expected_cols = [
        "codigo", "NO_NCM_POR", "NO_NCM_ESP", "NO_NCM_ING"  # ... complete conforme sua tabela
    ]
    schema = StructType([StructField(c, StringType(), True) for c in expected_cols])

    df_ok, df_corrupt, cols = read_csv_with_quotes(
        spark=spark,
        input_path=input_path,
        delimiter=";",
        encoding="iso-8859-1",
        recursive=True,
        path_glob_filter="NCM.csv",
        header=True,
        schema=schema,               # ou None, se quiser inferir
        expected_cols=None,          # use expected_cols aqui se NÃO passar schema
        multiline=True,
        quote="\"",
        escape="\"",
        mode="PERMISSIVE",
        corrupt_col="_corrupt_record",
        ignore_leading_trailing_ws=True,
        quarantine_path=quarantine,
        quarantine_mode="overwrite",
    )

    print("Colunas:", cols)
    print("Registros OK:", df_ok.count())
    print("Registros corrompidos:", df_corrupt.count())

    # Exemplo 2: fallback por texto bruto (se o arquivo estiver muito quebrado)
    df_clean, df_bad, cols2 = sanitize_csv_bronze(
        spark=spark,
        input_path=input_path,
        delimiter=";",
        encoding="iso-8859-1",
        recursive=True,
        path_glob_filter="NCM.csv",
        quarantine_path="/mnt/bronze_quarentena/ncm_fallback",
        forced_header_cols=expected_cols,  # se souber as colunas
    )

    print("Cols (fallback):", cols2)
    print("Linhas limpas (fallback):", df_clean.count())
    print("Linhas ruins (fallback):", df_bad.count())