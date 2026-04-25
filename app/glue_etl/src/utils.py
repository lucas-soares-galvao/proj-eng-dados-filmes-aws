"""Funcoes utilitarias compartilhadas pela aplicacao."""

import awswrangler as wr
import pandas as pd


COLUNAS_SOT = [
    "id",
    "title",
    "original_title",
    "overview",
    "release_date",
    "original_language",
    "adult",
    "video",
    "genre_ids",
    "popularity",
    "vote_average",
    "vote_count",
    "year",
    "month",
]


def obter_valor_argumento(argv, arg_name):
    """Le um argumento no formato --ARG_NAME valor."""
    flag = f"--{arg_name}"
    for index, arg in enumerate(argv):
        if arg == flag and index + 1 < len(argv):
            return argv[index + 1]
    return None


def _normalizar_prefixo_s3(prefixo):
    """Garante um prefixo S3 com uma unica barra final."""
    return f"{prefixo.strip('/')}/"


def _garantir_colunas_sot(df):
    """Completa colunas opcionais antes de gravar a tabela curada."""
    for coluna in COLUNAS_SOT:
        if coluna not in df.columns:
            df[coluna] = None

    df_sot = df[COLUNAS_SOT].copy()
    df_sot["year"] = df_sot["year"].astype(str)
    df_sot["month"] = df_sot["month"].astype(str).str.zfill(2)
    return df_sot


def carregar_sor_json_para_tabela_sot(
    s3_bucket_sor,
    s3_bucket_sot,
    catalog_database,
    catalog_table,
    sor_prefix="tmdb/discover_movie/",
    sot_prefix="tmdb/movies_sot/",
    wr_module=None,
):
    """Le JSON da SOR e materializa tabela SOT em Parquet no Glue Catalog."""
    wr_module = wr_module or wr
    sor_prefix_normalizado = _normalizar_prefixo_s3(sor_prefix)
    sot_prefix_normalizado = _normalizar_prefixo_s3(sot_prefix)

    caminho_origem = f"s3://{s3_bucket_sor}/{sor_prefix_normalizado}"
    caminho_destino = f"s3://{s3_bucket_sot}/{sot_prefix_normalizado}"

    df = wr_module.s3.read_json(path=caminho_origem, dataset=True)
    if df.empty:
        return {
            "catalog_database": catalog_database,
            "catalog_table": catalog_table,
            "s3_source": caminho_origem,
            "s3_target": caminho_destino,
            "status": "no_data",
            "total_records": 0,
        }

    if "year" not in df.columns or "month" not in df.columns:
        raise ValueError("Arquivos SOR sem particoes year/month no caminho S3.")

    df_sot = _garantir_colunas_sot(df)

    wr_module.s3.to_parquet(
        df=df_sot,
        path=caminho_destino,
        dataset=True,
        mode="overwrite_partitions",
        compression="snappy",
        partition_cols=["year", "month"],
        database=catalog_database,
        table=catalog_table,
    )

    return {
        "catalog_database": catalog_database,
        "catalog_table": catalog_table,
        "s3_source": caminho_origem,
        "s3_target": caminho_destino,
        "status": "written",
        "total_records": len(df_sot.index),
    }
