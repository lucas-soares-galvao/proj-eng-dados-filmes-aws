"""Funcoes utilitarias compartilhadas pela aplicacao."""

import os
from importlib import import_module


def eh_par(num):
    """Retorna True quando o numero informado e par."""
    return num % 2 == 0


def processar_numero(numero):
    """Encapsula uma regra simples para facilitar exemplos e testes."""
    if eh_par(numero):
        return f"O número {numero} é par."
    return f"O número {numero} é ímpar."


def obter_valor_argumento(argv, arg_name):
    """Le um argumento no formato --ARG_NAME valor."""
    flag = f"--{arg_name}"
    for index, arg in enumerate(argv):
        if arg == flag and index + 1 < len(argv):
            return argv[index + 1]
    return None


def obter_arg_data_quality_job_name(argv):
    """Le o nome do job de Data Quality passado via argumentos do Glue."""
    return obter_valor_argumento(argv, "GLUE_DATA_QUALITY_JOB_NAME")

def ler_arquivo_do_s3(bucket_name, s3_key, s3_client=None):
    """Lê um arquivo do bucket S3 e retorna seu conteúdo."""
    if s3_client is None:
        boto3_module = import_module("boto3")
        s3_client = boto3_module.client("s3")
    
    response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
    conteudo = response['Body'].read().decode('utf-8')
    
    return conteudo


def escrever_arquivo_no_s3(bucket_name, s3_key, conteudo, s3_client=None):
    """Escreve um arquivo no bucket S3 especificado."""
    if s3_client is None:
        boto3_module = import_module("boto3")
        s3_client = boto3_module.client("s3")
    
    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=conteudo.encode('utf-8')
    )
    
    return {"bucket": bucket_name, "key": s3_key, "status": "written"}


def processar_arquivo_etl(conteudo_entrada):
    """Processa o conteúdo do arquivo lido do SOR e prepara para escrita no SOT."""
    # Adiciona um header indicando que foi processado pelo ETL
    conteudo_processado = f"[PROCESSADO PELO ETL]\n{conteudo_entrada}"
    return conteudo_processado


def processar_arquivo_sor_para_sot(s3_bucket_sor, s3_bucket_sot, s3_key_entrada, s3_key_saida):
    """Le arquivo do bucket SOR, processa e escreve no bucket SOT."""
    conteudo_entrada = ler_arquivo_do_s3(bucket_name=s3_bucket_sor, s3_key=s3_key_entrada)
    conteudo_processado = processar_arquivo_etl(conteudo_entrada)
    return escrever_arquivo_no_s3(
        bucket_name=s3_bucket_sot,
        s3_key=s3_key_saida,
        conteudo=conteudo_processado,
    )


def limpar_particoes_sot(bucket_name, sot_prefix, particoes, s3_client=None):
    """Remove objetos existentes das particoes SOT que serao regravadas."""
    if s3_client is None:
        boto3_module = import_module("boto3")
        s3_client = boto3_module.client("s3")

    prefixo_base = sot_prefix.rstrip("/")
    paginator = s3_client.get_paginator("list_objects_v2")

    for particao in particoes:
        year = particao.get("year")
        month = particao.get("month")
        if not year or not month:
            continue

        prefixo_particao = f"{prefixo_base}/year={year}/month={month}/"
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefixo_particao):
            objetos = page.get("Contents", [])
            if not objetos:
                continue

            for inicio in range(0, len(objetos), 1000):
                lote = objetos[inicio:inicio + 1000]
                s3_client.delete_objects(
                    Bucket=bucket_name,
                    Delete={"Objects": [{"Key": obj["Key"]} for obj in lote]},
                )


def carregar_sor_json_para_tabela_sot(
    s3_bucket_sor,
    s3_bucket_sot,
    catalog_database,
    catalog_table,
    sor_prefix="tmdb/discover_movie/",
    sot_prefix="tmdb/movies_sot/",
):
    """Le JSON da SOR e materializa tabela SOT em Parquet no Glue Catalog."""
    # Import lazy para permitir testes locais sem dependencias do Glue runtime.
    from awsglue.context import GlueContext
    from awsglue.dynamicframe import DynamicFrame
    from pyspark.context import SparkContext
    from pyspark.sql.functions import col, input_file_name, regexp_extract

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    caminho_origem = f"s3://{s3_bucket_sor}/{sor_prefix}"
    caminho_destino = f"s3://{s3_bucket_sot}/{sot_prefix}"

    df = spark.read.option("multiline", "true").json(caminho_origem)

    df = (
        df.withColumn("source_file", input_file_name())
        .withColumn("year", regexp_extract(col("source_file"), r"year=(\\d{4})", 1))
        .withColumn("month", regexp_extract(col("source_file"), r"month=(\\d{2})", 1))
        .drop("source_file")
    )

    colunas_sot = [
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
    df_sot = df.select(*[c for c in colunas_sot if c in df.columns])

    particoes = [
        {"year": row["year"], "month": row["month"]}
        for row in df_sot.select("year", "month").distinct().collect()
        if row["year"] and row["month"]
    ]

    limpar_particoes_sot(
        bucket_name=s3_bucket_sot,
        sot_prefix=sot_prefix,
        particoes=particoes,
    )

    dynamic_frame = DynamicFrame.fromDF(df_sot, glue_context, "dynamic_frame_movies_sot")
    sink = glue_context.getSink(
        path=caminho_destino,
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=["year", "month"],
        compression="snappy",
        enableUpdateCatalog=True,
        transformation_ctx="sink_movies_sot",
    )
    sink.setCatalogInfo(catalogDatabase=catalog_database, catalogTableName=catalog_table)
    sink.setFormat("glueparquet")
    sink.writeFrame(dynamic_frame)

    return {
        "catalog_database": catalog_database,
        "catalog_table": catalog_table,
        "s3_source": caminho_origem,
        "s3_target": caminho_destino,
        "status": "written",
    }


def chamar_glue_data_quality(data_quality_job_name=None, glue_client=None, job_arguments=None):
    """Dispara um job do Glue Data Quality e retorna metadados da execucao."""
    if glue_client is None:
        boto3_module = import_module("boto3")
        glue_client = boto3_module.client("glue")

    data_quality_job_name = data_quality_job_name or os.getenv("GLUE_DATA_QUALITY_JOB_NAME")
    if not data_quality_job_name:
        raise ValueError("Nome do job Glue Data Quality nao informado.")

    kwargs = {"JobName": data_quality_job_name}
    if job_arguments:
        kwargs["Arguments"] = job_arguments

    concurrent_exception = None
    if hasattr(glue_client, "exceptions"):
        concurrent_exception = getattr(
            glue_client.exceptions,
            "ConcurrentRunsExceededException",
            None,
        )

    try:
        response = glue_client.start_job_run(**kwargs)
        data_quality_job_run_id = response.get("JobRunId")
        status = "started"
    except Exception as exc:
        if concurrent_exception and isinstance(exc, concurrent_exception):
            data_quality_job_run_id = None
            status = "already_running"
        else:
            raise

    return {
        "data_quality_job_name": data_quality_job_name,
        "data_quality_job_run_id": data_quality_job_run_id,
        "data_quality_job_status": status,
    }
