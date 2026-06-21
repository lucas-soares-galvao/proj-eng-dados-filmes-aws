# _setup_athena_mock() simula as 3 etapas do boto3 Athena:
#   start_query_execution → get_query_execution (polling) → get_paginator().paginate()
# O mock precisa dessas 3 chamadas encadeadas porque agent.py as chama em sequência.
#
# _mock_litellm() usa side_effect=[passo1, passo3] porque recomendar() chama
# o LLM duas vezes: 1ª para extrair filtros como JSON, 2ª para formatar respostas.

import json

import pytest
from unittest.mock import MagicMock, patch

import agent


TITULO_FAKE = {
    "title": "O Iluminado",
    "media_type": "movie",
    "year": "1980",
    "genre_names": "Terror, Drama",
    "overview": "Um escritor enlouquece num hotel isolado.",
    "vote_average": 8.4,
    "poster_url": "https://example.com/poster.jpg",
    "backdrop_url": None,
    "runtime_minutes": 146,
    "number_of_seasons": None,
    "number_of_episodes": None,
    "episode_runtime_minutes": None,
    "streaming_providers": "Netflix",
    "air_date": "1980-05-23",
}

RESPOSTA_LLM_FAKE = json.dumps(
    {
        "titulos": [
            {
                "titulo": "O Iluminado",
                "tipo": "filme",
                "ano": 1980,
                "generos": ["Terror", "Drama"],
                "sinopse": "Um escritor enlouquece num hotel isolado.",
                "nota": 8.4,
                "poster_url": "https://example.com/poster.jpg",
                "backdrop_url": None,
                "motivo": "Classico do terror psicologico.",
                "duracao": "2h 26min",
                "streaming_providers": "Netflix",
                "data_lancamento": "maio de 1980",
            }
        ]
    }
)

COLUMNS = [
    "title", "media_type", "year", "genre_names", "overview",
    "vote_average", "poster_url", "backdrop_url",
    "runtime_minutes", "number_of_seasons",
    "number_of_episodes", "episode_runtime_minutes",
    "streaming_providers", "air_date",
]


def _setup_athena_mock(mock_boto3, rows_data=None):
    """Configura o mock do boto3 para simular as tres etapas da API do Athena.

    A API nativa do Athena usada por buscar_titulos_spec() requer:
      1. start_query_execution() → inicia a query, retorna QueryExecutionId
      2. get_query_execution()   → polling ate o estado ser SUCCEEDED
      3. get_paginator().paginate() → le os resultados paginados

    Args:
        mock_boto3:  Mock do modulo boto3 injetado via @patch("agent.boto3").
        rows_data:   Lista de dicts com os dados de cada linha a retornar.
                     None ou lista vazia → retorna apenas o header (resultado vazio).

    Returns:
        mock_athena: Mock do client Athena (boto3.client("athena", ...)).
    """
    mock_athena = MagicMock()
    mock_boto3.client.return_value = mock_athena

    mock_athena.start_query_execution.return_value = {"QueryExecutionId": "test-exec-id"}
    mock_athena.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
    }

    header = {"Data": [{"VarCharValue": col} for col in COLUMNS]}
    if rows_data:
        data_rows = [
            {"Data": [{"VarCharValue": str(row.get(col) or "")} for col in COLUMNS]}
            for row in rows_data
        ]
        page = {"ResultSet": {"Rows": [header] + data_rows}}
    else:
        page = {"ResultSet": {"Rows": [header]}}

    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [page]
    mock_athena.get_paginator.return_value = mock_paginator

    return mock_athena


def _mock_litellm(tool_args: dict, resposta_final: str):
    """Retorna lista de 2 respostas para o side_effect de litellm.completion."""
    tool_call = MagicMock()
    tool_call.id = "call_test_123"
    tool_call.function.name = "buscar_titulos_spec"
    tool_call.function.arguments = json.dumps(tool_args)

    msg_passo1 = MagicMock()
    msg_passo1.content = None
    msg_passo1.tool_calls = [tool_call]

    msg_passo3 = MagicMock()
    msg_passo3.content = resposta_final

    passo1 = MagicMock()
    passo1.choices = [MagicMock(message=msg_passo1)]

    passo3 = MagicMock()
    passo3.choices = [MagicMock(message=msg_passo3)]

    return [passo1, passo3]


class TestValidarWhere:

    def test_aceita_clausula_valida(self):
        resultado = agent._validar_where("media_type = 'movie' AND vote_average >= 7.0")
        assert resultado == "media_type = 'movie' AND vote_average >= 7.0"

    def test_rejeita_ponto_e_virgula(self):
        with pytest.raises(ValueError, match="contém ';'"):
            agent._validar_where("media_type = 'movie'; DROP TABLE x")

    def test_rejeita_drop(self):
        with pytest.raises(ValueError, match="palavra SQL proibida"):
            agent._validar_where("DROP TABLE spec")

    def test_rejeita_delete(self):
        with pytest.raises(ValueError, match="palavra SQL proibida"):
            agent._validar_where("DELETE FROM spec WHERE 1=1")

    def test_rejeita_insert(self):
        with pytest.raises(ValueError, match="palavra SQL proibida"):
            agent._validar_where("1=1 INSERT INTO spec VALUES (1)")

    def test_rejeita_subquery_select(self):
        with pytest.raises(ValueError, match="contém subquery"):
            agent._validar_where("id IN (SELECT id FROM outra_tabela)")

    def test_remove_espacos_nas_pontas(self):
        resultado = agent._validar_where("  media_type = 'movie'  ")
        assert resultado == "media_type = 'movie'"


class TestBuscarTitulosSpec:

    def test_retorna_lista_vazia_sem_resultados(self):
        with patch("agent.boto3") as mock_boto3:
            _setup_athena_mock(mock_boto3)
            resultado = agent.buscar_titulos_spec("vote_average >= 6.0")

        assert resultado == []

    def test_retorna_registros_como_lista_de_dicts(self):
        with patch("agent.boto3") as mock_boto3:
            _setup_athena_mock(mock_boto3, rows_data=[TITULO_FAKE])
            resultado = agent.buscar_titulos_spec("vote_average >= 6.0")

        assert isinstance(resultado, list)
        assert len(resultado) == 1
        assert resultado[0]["title"] == "O Iluminado"

    def test_filtro_where_incluido_na_query(self):
        filtro = "media_type = 'movie' AND lower(genre_names) LIKE '%terror%'"
        with patch("agent.boto3") as mock_boto3:
            mock_athena = _setup_athena_mock(mock_boto3)
            agent.buscar_titulos_spec(filtro)

        sql_executado = mock_athena.start_query_execution.call_args.kwargs["QueryString"]
        assert "media_type = 'movie'" in sql_executado
        assert "lower(genre_names) LIKE '%terror%'" in sql_executado

    def test_vote_count_fixo_sempre_presente(self):
        with patch("agent.boto3") as mock_boto3:
            mock_athena = _setup_athena_mock(mock_boto3)
            agent.buscar_titulos_spec("media_type = 'movie'")

        sql_executado = mock_athena.start_query_execution.call_args.kwargs["QueryString"]
        assert "vote_count >= 50" in sql_executado

    def test_filtro_idioma_na_query(self):
        with patch("agent.boto3") as mock_boto3:
            mock_athena = _setup_athena_mock(mock_boto3)
            agent.buscar_titulos_spec("original_language = 'ko'")

        sql_executado = mock_athena.start_query_execution.call_args.kwargs["QueryString"]
        assert "original_language = 'ko'" in sql_executado

    def test_filtro_duracao_na_query(self):
        with patch("agent.boto3") as mock_boto3:
            mock_athena = _setup_athena_mock(mock_boto3)
            agent.buscar_titulos_spec("runtime_minutes <= 90 AND media_type = 'movie'")

        sql_executado = mock_athena.start_query_execution.call_args.kwargs["QueryString"]
        assert "runtime_minutes <= 90" in sql_executado

    def test_filtro_temporadas_na_query(self):
        with patch("agent.boto3") as mock_boto3:
            mock_athena = _setup_athena_mock(mock_boto3)
            agent.buscar_titulos_spec("number_of_seasons = 1 AND media_type = 'tv'")

        sql_executado = mock_athena.start_query_execution.call_args.kwargs["QueryString"]
        assert "number_of_seasons = 1" in sql_executado

    def test_filtro_em_cartaz_na_query(self):
        with patch("agent.boto3") as mock_boto3:
            mock_athena = _setup_athena_mock(mock_boto3)
            agent.buscar_titulos_spec("in_theaters = true AND media_type = 'movie'")

        sql_executado = mock_athena.start_query_execution.call_args.kwargs["QueryString"]
        assert "in_theaters = true" in sql_executado

    def test_filtro_plataforma_na_query(self):
        with patch("agent.boto3") as mock_boto3:
            mock_athena = _setup_athena_mock(mock_boto3)
            agent.buscar_titulos_spec("lower(streaming_providers) LIKE '%netflix%'")

        sql_executado = mock_athena.start_query_execution.call_args.kwargs["QueryString"]
        assert "lower(streaming_providers) LIKE '%netflix%'" in sql_executado

    def test_filtro_faixa_de_ano_na_query(self):
        with patch("agent.boto3") as mock_boto3:
            mock_athena = _setup_athena_mock(mock_boto3)
            agent.buscar_titulos_spec("year BETWEEN '2000' AND '2010'")

        sql_executado = mock_athena.start_query_execution.call_args.kwargs["QueryString"]
        assert "year BETWEEN '2000' AND '2010'" in sql_executado

    def test_limite_aplicado_na_query(self):
        with patch("agent.boto3") as mock_boto3:
            mock_athena = _setup_athena_mock(mock_boto3)
            agent.buscar_titulos_spec("vote_average >= 6.0", limite=10)

        sql_executado = mock_athena.start_query_execution.call_args.kwargs["QueryString"]
        assert "LIMIT 10" in sql_executado

    def test_limite_e_limitado_ao_maximo_de_30(self):
        with patch("agent.boto3") as mock_boto3:
            mock_athena = _setup_athena_mock(mock_boto3)
            agent.buscar_titulos_spec("vote_average >= 6.0", limite=100)

        sql_executado = mock_athena.start_query_execution.call_args.kwargs["QueryString"]
        assert "LIMIT 30" in sql_executado
        assert "LIMIT 100" not in sql_executado

    def test_limite_minimo_e_1(self):
        with patch("agent.boto3") as mock_boto3:
            mock_athena = _setup_athena_mock(mock_boto3)
            agent.buscar_titulos_spec("vote_average >= 6.0", limite=0)

        sql_executado = mock_athena.start_query_execution.call_args.kwargs["QueryString"]
        assert "LIMIT 1" in sql_executado

    def test_rejeita_where_com_sql_perigoso(self):
        with pytest.raises(ValueError):
            agent.buscar_titulos_spec("1=1; DROP TABLE spec")


class TestRecomendar:

    def test_retorna_lista_vazia_se_athena_sem_resultados(self):
        with (
            patch("agent.buscar_titulos_spec", return_value=[]),
            patch("agent.litellm.completion") as mock_completion,
        ):
            mock_completion.side_effect = _mock_litellm(
                {"filtro_where": "media_type = 'movie'"}, ""
            )
            resultado = agent.recomendar("filmes de terror")

        assert resultado == []

    def test_chama_llm_duas_vezes(self):
        with (
            patch("agent.buscar_titulos_spec", return_value=[TITULO_FAKE]),
            patch("agent.litellm.completion") as mock_completion,
        ):
            mock_completion.side_effect = _mock_litellm(
                {"filtro_where": "media_type = 'movie'"}, RESPOSTA_LLM_FAKE
            )
            agent.recomendar("filmes de terror")

        assert mock_completion.call_count == 2

    def test_retorna_lista_de_titulos(self):
        with (
            patch("agent.buscar_titulos_spec", return_value=[TITULO_FAKE]),
            patch("agent.litellm.completion") as mock_completion,
        ):
            mock_completion.side_effect = _mock_litellm(
                {"filtro_where": "media_type = 'movie'"}, RESPOSTA_LLM_FAKE
            )
            resultado = agent.recomendar("filmes de terror")

        assert isinstance(resultado, list)
        assert len(resultado) == 1
        assert resultado[0]["titulo"] == "O Iluminado"

    def test_remove_markdown_code_block_do_json(self):
        resposta_com_markdown = f"```json\n{RESPOSTA_LLM_FAKE}\n```"
        with (
            patch("agent.buscar_titulos_spec", return_value=[TITULO_FAKE]),
            patch("agent.litellm.completion") as mock_completion,
        ):
            mock_completion.side_effect = _mock_litellm(
                {"filtro_where": "media_type = 'movie'"}, resposta_com_markdown
            )
            resultado = agent.recomendar("filmes de terror")

        assert len(resultado) == 1

    def test_retorna_lista_vazia_se_llm_retorna_string_vazia(self):
        with (
            patch("agent.buscar_titulos_spec", return_value=[TITULO_FAKE]),
            patch("agent.litellm.completion") as mock_completion,
        ):
            mock_completion.side_effect = _mock_litellm(
                {"filtro_where": "media_type = 'movie'"}, ""
            )
            resultado = agent.recomendar("filmes de terror")

        assert resultado == []

    def test_passa_filtros_extraidos_pelo_llm_para_athena(self):
        filtros = {
            "filtro_where": "media_type = 'movie' AND lower(genre_names) LIKE '%terror%' AND vote_average >= 7.0",
            "limite": 5,
        }
        with (
            patch("agent.buscar_titulos_spec", return_value=[TITULO_FAKE]) as mock_buscar,
            patch("agent.litellm.completion") as mock_completion,
        ):
            mock_completion.side_effect = _mock_litellm(filtros, RESPOSTA_LLM_FAKE)
            agent.recomendar("filmes de terror dos anos 80")

        mock_buscar.assert_called_once_with(**filtros)

    def test_retorna_lista_vazia_se_llm_nao_chama_tool(self):
        msg_sem_tool = MagicMock()
        msg_sem_tool.content = None
        msg_sem_tool.tool_calls = None

        passo1_sem_tool = MagicMock()
        passo1_sem_tool.choices = [MagicMock(message=msg_sem_tool)]

        with (
            patch("agent.buscar_titulos_spec") as mock_buscar,
            patch("agent.litellm.completion", return_value=passo1_sem_tool),
        ):
            resultado = agent.recomendar("filmes de terror")

        assert resultado == []
        mock_buscar.assert_not_called()

    def test_retorna_data_lancamento_formatada(self):
        with (
            patch("agent.buscar_titulos_spec", return_value=[TITULO_FAKE]),
            patch("agent.litellm.completion") as mock_completion,
        ):
            mock_completion.side_effect = _mock_litellm(
                {"filtro_where": "media_type = 'movie'"}, RESPOSTA_LLM_FAKE
            )
            resultado = agent.recomendar("filmes de terror")

        assert "data_lancamento" in resultado[0]
        assert resultado[0]["data_lancamento"] == "maio de 1980"


class TestLimparDuracao:
    def test_retorna_vazio_para_string_vazia(self):
        assert agent.limpar_duracao("") == ""

    def test_remove_null_isolado(self):
        assert agent.limpar_duracao("~null") == ""

    def test_remove_null_no_fim(self):
        assert agent.limpar_duracao("3 temporadas · ~null") == "3 temporadas"

    def test_remove_null_no_inicio(self):
        assert agent.limpar_duracao("~null · 36 eps") == "36 eps"

    def test_remove_multiplos_nulls(self):
        assert agent.limpar_duracao("~null · 36 eps · ~null") == "36 eps"

    def test_preserva_duracao_normal(self):
        assert agent.limpar_duracao("2h 26min") == "2h 26min"

    def test_preserva_duracao_composta(self):
        assert agent.limpar_duracao("3 temporadas · 36 eps · 45min") == "3 temporadas · 36 eps · 45min"

    def test_remove_separadores_vazios(self):
        assert agent.limpar_duracao(" · 36 eps · ") == "36 eps"
