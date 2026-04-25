"""Testes do modulo principal da aplicacao."""

import unittest
from unittest.mock import patch

from app.glue_etl.main import main


class TestMain(unittest.TestCase):
    @patch("app.glue_etl.main.print")
    @patch(
        "app.glue_etl.main.sys.argv",
        [
            "main.py",
            "--S3_BUCKET_SOR",
            "lsg-sa-east-1-bucket-sor-dev",
            "--S3_BUCKET_SOT",
            "lsg-sa-east-1-bucket-sot-dev",
            "--GLUE_CATALOG_DATABASE",
            "tmdb_dev",
            "--GLUE_CATALOG_TABLE",
            "movies_sot",
        ],
    )
    @patch("app.glue_etl.main.carregar_sor_json_para_tabela_sot")
    def test_main_processa_sot_com_sucesso(self, mock_carregar_sot, mock_print):
        mock_carregar_sot.return_value = {
            "catalog_database": "tmdb_dev",
            "catalog_table": "movies_sot",
            "status": "written",
        }

        main()

        mock_carregar_sot.assert_called_once_with(
            s3_bucket_sor="lsg-sa-east-1-bucket-sor-dev",
            s3_bucket_sot="lsg-sa-east-1-bucket-sot-dev",
            catalog_database="tmdb_dev",
            catalog_table="movies_sot",
            sor_prefix="tmdb/discover_movie/",
            sot_prefix="tmdb/movies_sot/",
        )
        mock_print.assert_called_once()

    @patch("app.glue_etl.main.print")
    @patch("app.glue_etl.main.sys.argv", ["main.py", "--S3_BUCKET_SOR", "sor", "--S3_BUCKET_SOT", "sot"])
    def test_main_falha_sem_catalog_args(self, _mock_print):
        with self.assertRaises(ValueError):
            main()

    @patch.dict(
        "app.glue_etl.main.os.environ",
        {
            "S3_BUCKET_SOR": "bucket-sor-env",
            "S3_BUCKET_SOT": "bucket-sot-env",
            "GLUE_CATALOG_DATABASE": "tmdb_env",
            "GLUE_CATALOG_TABLE": "movies_env",
        },
        clear=True,
    )
    @patch("app.glue_etl.main.print")
    @patch("app.glue_etl.main.sys.argv", ["main.py"])
    @patch("app.glue_etl.main.carregar_sor_json_para_tabela_sot")
    def test_main_usa_variaveis_de_ambiente(self, mock_carregar_sot, _mock_print):
        mock_carregar_sot.return_value = {"status": "written"}

        main()

        mock_carregar_sot.assert_called_once_with(
            s3_bucket_sor="bucket-sor-env",
            s3_bucket_sot="bucket-sot-env",
            catalog_database="tmdb_env",
            catalog_table="movies_env",
            sor_prefix="tmdb/discover_movie/",
            sot_prefix="tmdb/movies_sot/",
        )


if __name__ == "__main__":
    unittest.main()
    