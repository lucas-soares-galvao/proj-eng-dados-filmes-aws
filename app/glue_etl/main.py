from awsglue.utils import getResolvedOptions
import sys

from src.utils import processar_tmdb, chamar_glue_data_quality

args = getResolvedOptions(sys.argv, [
    "GLUE_CATALOG_DATABASE",
    "GLUE_CATALOG_TABLE",
    "S3_BUCKET_SOR",
    "S3_BUCKET_SOT"
])

database = args["GLUE_CATALOG_DATABASE"]
table = args["GLUE_CATALOG_TABLE"]
bucket_sor = args["S3_BUCKET_SOR"]
bucket_sot = args["S3_BUCKET_SOT"]

processar_tmdb(
    input_path=f"s3://{bucket_sor}/",
    output_path=f"s3://{bucket_sot}/",
    database=database,
    table=table
)

if processar_tmdb:
    glue = chamar_glue_data_quality("tmdb-data-quality")
else:    
    glue = None

if __name__ == "__main__":
    print("ETL de TMDB executado com sucesso!")
    if glue:
        print(f"Job de qualidade de dados iniciado: {glue}")