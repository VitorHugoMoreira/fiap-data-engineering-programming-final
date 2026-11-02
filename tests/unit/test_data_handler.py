"""Testes da camada de I/O com arquivos temporários (sem os datasets reais)."""

from __future__ import annotations

import gzip
import json
import logging
from datetime import datetime
from decimal import Decimal

import pytest

from data_engineering_pyspark.config.settings import (
    CsvSourceConfig,
    JsonSourceConfig,
    ParquetOutputConfig,
)
from data_engineering_pyspark.io_utils.data_handler import DataHandler
from data_engineering_pyspark.io_utils.exceptions import DataReadException, DataWriteException
from data_engineering_pyspark.schemas.data_schemas import DataSchemas
from tests.helpers import DadosDeTeste

pytestmark = pytest.mark.unit


@pytest.fixture
def input_dirs(tmp_path):
    """Cria pedidos (.csv.gz) e pagamentos (.json.gz) no layout real, mais um arquivo intruso."""
    pedidos_dir = tmp_path / "pedidos"
    pagamentos_dir = tmp_path / "pagamentos"
    pedidos_dir.mkdir()
    pagamentos_dir.mkdir()
    linhas = [
        "ID_PEDIDO;PRODUTO;VALOR_UNITARIO;QUANTIDADE;DATA_CRIACAO;UF;ID_CLIENTE",
        "p1;TV;1500.0;2;2025-03-10T10:00:00;SP;10",
        "p2;LIVRO;50.5;1;2025-03-11T11:30:00;RJ;20",
    ]
    with gzip.open(pedidos_dir / "pedidos-2025-03.csv.gz", "wt", encoding="utf-8") as f:
        f.write("\n".join(linhas) + "\n")
    (pedidos_dir / "LEIAME.txt").write_text("arquivo que não deve ser lido", encoding="utf-8")
    pagamentos = [
        {
            "id_pedido": "p1",
            "forma_pagamento": "PIX",
            "valor_pagamento": 2850.0,
            "status": False,
            "data_processamento": "2025-03-10T12:00:00.123456",
            "avaliacao_fraude": {"fraude": False, "score": 0.25},
        }
    ]
    with gzip.open(pagamentos_dir / "pagamentos-2025-03.json.gz", "wt", encoding="utf-8") as f:
        for registro in pagamentos:
            f.write(json.dumps(registro) + "\n")
    return pedidos_dir, pagamentos_dir


@pytest.fixture
def handler(spark, input_dirs, tmp_path):
    pedidos_dir, pagamentos_dir = input_dirs
    return DataHandler(
        spark,
        CsvSourceConfig(path=str(pedidos_dir)),
        JsonSourceConfig(path=str(pagamentos_dir)),
        ParquetOutputConfig(path=str(tmp_path / "saida")),
    )


class TestReadPedidos:
    def test_le_csv_compactado_com_schema_explicito(self, handler):
        df = handler.read_pedidos()
        assert df.schema == DataSchemas.PEDIDOS  # cabeçalho MAIÚSCULO não altera o schema
        assert df.count() == 2  # LEIAME.txt ignorado pelo pathGlobFilter
        linha = df.filter("id_pedido = 'p1'").first()
        assert linha.valor_unitario == Decimal("1500.00")
        assert linha.data_criacao == datetime(2025, 3, 10, 10, 0, 0)

    def test_caminho_inexistente_lanca_excecao_de_dominio(self, spark, tmp_path):
        handler = DataHandler(
            spark,
            CsvSourceConfig(path=str(tmp_path / "nao_existe")),
            JsonSourceConfig(path=str(tmp_path)),
            ParquetOutputConfig(path=str(tmp_path / "saida")),
        )
        with pytest.raises(DataReadException) as erro:
            handler.read_pedidos()
        assert erro.value.__cause__ is not None


class TestReadPagamentos:
    def test_le_json_com_objeto_aninhado(self, handler):
        df = handler.read_pagamentos()
        assert df.schema == DataSchemas.PAGAMENTOS
        linha = df.first()
        assert linha.avaliacao_fraude.fraude is False
        assert linha.avaliacao_fraude.score == pytest.approx(0.25)
        assert linha.valor_pagamento == Decimal("2850.00")
        assert linha.data_processamento == datetime(2025, 3, 10, 12, 0, 0, 123456)


class TestParquet:
    def test_grava_e_rele_preservando_tipos_e_ordem(self, spark, handler):
        linhas = [
            ("p2", "AC", "PIX", Decimal("10.00"), datetime(2025, 1, 1)),
            ("p1", "SP", "PIX", Decimal("20.50"), datetime(2025, 2, 1)),
        ]
        handler.write_parquet(spark.createDataFrame(linhas, DataSchemas.RELATORIO).orderBy("uf"))
        relido = handler.read_parquet()
        assert DataSchemas.same_structure(relido.schema, DataSchemas.RELATORIO)
        assert [tuple(r) for r in relido.collect()] == linhas


class TestRobustezDaLeitura:
    def test_cabecalho_fora_de_ordem_falha_em_vez_de_trocar_colunas(self, spark, tmp_path):
        """enforceSchema=false: PRODUTO;ID_PEDIDO;... não pode virar id_pedido='TV' em silêncio."""
        DadosDeTeste.write_pedidos_gz(
            tmp_path / "pedidos" / "p.csv.gz",
            ["TV;p1;1500.0;2;2025-03-10T10:00:00;SP;10"],
            cabecalho="PRODUTO;ID_PEDIDO;VALOR_UNITARIO;QUANTIDADE;DATA_CRIACAO;UF;ID_CLIENTE",
        )
        handler = DataHandler(
            spark,
            CsvSourceConfig(path=str(tmp_path / "pedidos")),
            JsonSourceConfig(path=str(tmp_path)),
            ParquetOutputConfig(path=str(tmp_path / "saida")),
        )
        with pytest.raises(Exception, match="CSV header does not conform to the schema"):
            handler.read_pedidos().first()

    def test_dataset_vazio_gera_warning(self, spark, tmp_path, caplog):
        DadosDeTeste.write_pedidos_gz(tmp_path / "vazio" / "p.csv.gz", [])
        handler = DataHandler(
            spark,
            CsvSourceConfig(path=str(tmp_path / "vazio")),
            JsonSourceConfig(path=str(tmp_path)),
            ParquetOutputConfig(path=str(tmp_path / "saida")),
        )
        with caplog.at_level(logging.WARNING):
            assert handler.read_pedidos().count() == 0
        assert any("não contém registros" in r.getMessage() for r in caplog.records)

    def test_arquivo_corrompido_falha_de_forma_explicita(self, spark, tmp_path):
        """Conteúdo inválido não pode virar um dataset vazio silencioso (Passo 9 do material)."""
        (tmp_path / "corrompido").mkdir()
        (tmp_path / "corrompido" / "p.csv.gz").write_text("isto não é gzip", encoding="utf-8")
        handler = DataHandler(
            spark,
            CsvSourceConfig(path=str(tmp_path / "corrompido")),
            JsonSourceConfig(path=str(tmp_path)),
            ParquetOutputConfig(path=str(tmp_path / "saida")),
        )
        with pytest.raises(Exception, match="(?i)gzip"):
            handler.read_pedidos().count()


class TestErrosDeEscrita:
    def test_falha_na_escrita_vira_data_write_exception(self, spark, tmp_path):
        destino = tmp_path / "ja_existe"
        destino.mkdir()
        handler = DataHandler(
            spark,
            CsvSourceConfig(path=str(tmp_path)),
            JsonSourceConfig(path=str(tmp_path)),
            ParquetOutputConfig(path=str(destino), mode="error"),
        )
        df = spark.createDataFrame([], DataSchemas.RELATORIO)
        with pytest.raises(DataWriteException) as erro:
            handler.write_parquet(df)
        assert erro.value.__cause__ is not None

    def test_releitura_de_saida_inexistente_vira_data_write_exception(self, spark, tmp_path):
        handler = DataHandler(
            spark,
            CsvSourceConfig(path=str(tmp_path)),
            JsonSourceConfig(path=str(tmp_path)),
            ParquetOutputConfig(path=str(tmp_path / "nao_gravado")),
        )
        with pytest.raises(DataWriteException):
            handler.read_parquet()
