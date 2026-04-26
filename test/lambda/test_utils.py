import unittest
from unittest.mock import patch, MagicMock

from app.lambda_api.src import utils


class TestUtils(unittest.TestCase):

    @patch("boto3.client")
    def test_obter_tmdb_api_key(self, mock_boto):
        mock_client = MagicMock()
        mock_client.get_secret_value.return_value = {
            "SecretString": '{"tmdb_api_key": "123"}'
        }
        mock_boto.return_value = mock_client

        result = utils.obter_tmdb_api_key("fake_arn")

        self.assertEqual(result, "123")

    def test_gerar_periodos_mensais(self):
        periodos = utils.gerar_periodos_mensais(ano_inicio=2024)

        self.assertTrue(len(periodos) > 0)
        self.assertIn("data_inicio", periodos[0])
        self.assertIn("data_fim", periodos[0])

    @patch("requests.get")
    def test_buscar_filmes_por_periodo(self, mock_get):
        # Simula resposta vazia para pt-BR e resposta com filmes para en-US
        mock_response_pt = MagicMock()
        mock_response_pt.json.return_value = {
            "results": [],
            "total_pages": 1
        }
        mock_response_pt.raise_for_status.return_value = None

        mock_response_en = MagicMock()
        mock_response_en.json.return_value = {
            "results": [{"id": 1}, {"id": 2}],
            "total_pages": 1
        }
        mock_response_en.raise_for_status.return_value = None

        # O mock retorna pt-BR na primeira chamada, en-US na segunda
        mock_get.side_effect = [mock_response_pt, mock_response_en]

        periodo = {
            "data_inicio": "2024-01-01",
            "data_fim": "2024-01-31"
        }

        filmes = utils.buscar_filmes_por_periodo("fake_key", periodo)

        self.assertEqual(len(filmes), 2)
        self.assertEqual(filmes[0]["id"], 1)
        self.assertEqual(filmes[1]["id"], 2)

    @patch("boto3.client")
    def test_salvar_json_no_s3(self, mock_boto):
        mock_s3 = MagicMock()
        mock_boto.return_value = mock_s3

        utils.salvar_json_no_s3("bucket", "key.json", {"a": 1})

        mock_s3.put_object.assert_called_once()

    @patch("boto3.client")
    def test_chamar_glue_etl(self, mock_boto):
        mock_glue = MagicMock()
        mock_glue.start_job_run.return_value = {
            "JobRunId": "abc123"
        }
        mock_boto.return_value = mock_glue

        result = utils.chamar_glue_etl("job_test")

        self.assertEqual(result["job_name"], "job_test")
        self.assertEqual(result["job_run_id"], "abc123")


if __name__ == "__main__":
    unittest.main()