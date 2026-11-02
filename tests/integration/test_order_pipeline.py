"""Testes de integração: orquestração com mock e fluxo completo com arquivos reais pequenos."""

from __future__ import annotations

import logging
import subprocess
import sys
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

import main
from data_engineering_pyspark.business.exceptions import DataQualityException
from data_engineering_pyspark.config.settings import (
    CsvSourceConfig,
    JsonSourceConfig,
    ParquetOutputConfig,
    PipelineConfig,
)
from data_engineering_pyspark.io_utils.data_handler import DataHandler
from data_engineering_pyspark.io_utils.exceptions import DataReadException
from data_engineering_pyspark.pipeline.exceptions import PipelineException
from data_engineering_pyspark.pipeline.order_pipeline import OrderPipeline
from data_engineering_pyspark.schemas.data_schemas import DataSchemas
from tests.helpers import PROJECT_ROOT, DadosDeTeste

pytestmark = pytest.mark.integration


@pytest.fixture
def entradas(make_pedidos, make_pagamentos):
    pedidos = make_pedidos(
        [
            ("p1", "TV", 1500, 1, "2025-02-01 10:00:00", "SP", 1),
            ("p2", "TV", 1500, 1, "2025-02-01 10:00:00", "RJ", 2),
        ]
    )
    pagamentos = make_pagamentos(
        [
            ("p1", "PIX", 1425, False, "2025-02-01 11:00:00", False, 0.1),
            ("p2", "PIX", 1425, True, "2025-02-01 11:00:00", False, 0.1),
        ]
    )
    return pedidos, pagamentos


@pytest.fixture
def handler_mock(entradas):
    """DataHandler falso: devolve DataFrames prontos e 'relê' o que foi gravado."""
    pedidos, pagamentos = entradas
    handler = MagicMock(spec=DataHandler)
    handler.read_pedidos.return_value = pedidos
    handler.read_pagamentos.return_value = pagamentos
    gravado = {}
    handler.write_parquet.side_effect = lambda df: gravado.update(df=df)
    handler.read_parquet.side_effect = lambda: gravado["df"]
    return handler


class TestOrquestracao:
    def test_executa_as_etapas_e_retorna_metricas(self, handler_mock, business):
        metricas = OrderPipeline(handler_mock, business, PipelineConfig()).run()
        handler_mock.read_pedidos.assert_called_once_with()
        handler_mock.read_pagamentos.assert_called_once_with()
        handler_mock.write_parquet.assert_called_once()
        assert metricas.pedidos_lidos == 2
        assert metricas.pagamentos_recusados == 1
        assert metricas.pedidos_elegiveis == metricas.registros_gravados == 1

    def test_divergencia_entre_calculado_e_gravado_falha(self, spark, handler_mock, business):
        vazio = spark.createDataFrame([], DataSchemas.RELATORIO)
        handler_mock.read_parquet.side_effect = None
        handler_mock.read_parquet.return_value = vazio
        with pytest.raises(PipelineException, match="Divergência"):
            OrderPipeline(handler_mock, business, PipelineConfig()).run()

    def test_libera_o_cache_mesmo_quando_falha(self, handler_mock, business, entradas):
        handler_mock.write_parquet.side_effect = DataQualityException("falha simulada")
        with pytest.raises(DataQualityException):
            OrderPipeline(handler_mock, business, PipelineConfig(cache_inputs=True)).run()
        assert not any(df.is_cached for df in entradas)

    def test_funciona_sem_cache_das_entradas(self, handler_mock, business, entradas):
        metricas = OrderPipeline(handler_mock, business, PipelineConfig(cache_inputs=False)).run()
        assert metricas.registros_gravados == 1
        assert not any(df.is_cached for df in entradas)

    def test_libera_o_cache_quando_a_segunda_leitura_falha(self, handler_mock, business, entradas):
        handler_mock.read_pagamentos.side_effect = DataReadException("falha simulada")
        with pytest.raises(DataReadException):
            OrderPipeline(handler_mock, business, PipelineConfig(cache_inputs=True)).run()
        assert not entradas[0].is_cached


class TestFluxoCompleto:
    PEDIDOS = [
        "a;TV;1500.0;2;2025-05-01T10:00:00;SP;1",
        "a;CELULAR;1000.0;1;2025-05-01T10:00:00;SP;1",
        "b;LIVRO;50.0;1;2025-04-01T10:00:00;AC;2",
        "c;LIVRO;50.0;1;2024-04-01T10:00:00;AC;3",
        "d;TV;1500.0;1;2025-06-01T10:00:00;RJ;4",
    ]
    PAGAMENTOS = [
        DadosDeTeste.pagamento("a", "PIX", status=False, fraude=False),
        DadosDeTeste.pagamento("b", "BOLETO", status=False, fraude=False),
        DadosDeTeste.pagamento("c", "PIX", status=False, fraude=False),
        DadosDeTeste.pagamento("d", "PIX", status=False, fraude=True),
    ]
    ESPERADO = [("b", "AC", Decimal("50.00")), ("a", "SP", Decimal("4000.00"))]

    @classmethod
    def criar_dados(cls, base):
        DadosDeTeste.write_pedidos_gz(base / "pedidos" / "pedidos.csv.gz", cls.PEDIDOS)
        DadosDeTeste.write_pagamentos_gz(base / "pagamentos" / "pagamentos.json.gz", cls.PAGAMENTOS)

    def test_gera_parquet_correto_a_partir_de_arquivos(self, spark, business, tmp_path):
        self.criar_dados(tmp_path)
        saida = tmp_path / "saida"
        handler = DataHandler(
            spark,
            CsvSourceConfig(path=str(tmp_path / "pedidos")),
            JsonSourceConfig(path=str(tmp_path / "pagamentos")),
            ParquetOutputConfig(path=str(saida)),
        )

        metricas = OrderPipeline(handler, business, PipelineConfig()).run()

        assert list(saida.glob("part-*.parquet"))
        resultado = spark.read.schema(DataSchemas.RELATORIO).parquet(str(saida)).collect()
        assert [(r.id_pedido, r.uf, r.valor_total_pedido) for r in resultado] == self.ESPERADO
        assert metricas.pedidos_lidos == 5 and metricas.pedidos_apos_agregacao == 4
        assert metricas.registros_gravados == 2


class TestAggregationRoot:
    def test_configuracao_inexistente_encerra_com_codigo_2(self, tmp_path):
        root = logging.getLogger()
        handlers = root.handlers[:]
        try:
            assert main.main(["--config", str(tmp_path / "nao_existe.yaml")]) == 2
        finally:
            root.handlers[:] = handlers


class TestAggregationRootEmProcessoIsolado:
    """Executa o src/main.py real (nova JVM) a partir de OUTRO diretório de trabalho."""

    @staticmethod
    def preparar_projeto(tmp_path, caminho_pedidos="./dados/pedidos"):
        raiz = tmp_path / "projeto"
        TestFluxoCompleto.criar_dados(raiz / "dados")
        config = DadosDeTeste.project_config()
        config["spark"]["master"] = "local[2]"
        config["spark"]["options"]["spark.sql.shuffle.partitions"] = "2"
        config["sources"]["pedidos"]["path"] = caminho_pedidos
        config["sources"]["pagamentos"]["path"] = "./dados/pagamentos"
        config["output"]["path"] = "./saida/relatorio"
        arquivo = DadosDeTeste.write_yaml(raiz / "config" / "settings.yaml", config)
        outro_dir = tmp_path / "outro_diretorio"
        outro_dir.mkdir()
        return raiz, arquivo, outro_dir

    @staticmethod
    def executar_main(arquivo, cwd):
        return subprocess.run(
            [sys.executable, str(PROJECT_ROOT / "src" / "main.py"), "--config", arquivo],
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=600,
        )

    def test_compoe_as_dependencias_e_gera_o_relatorio(self, spark, tmp_path):
        raiz, arquivo, outro_dir = self.preparar_projeto(tmp_path)
        execucao = self.executar_main(arquivo, outro_dir)
        assert execucao.returncode == 0, execucao.stdout[-2000:] + execucao.stderr[-2000:]
        resultado = spark.read.schema(DataSchemas.RELATORIO).parquet(
            str(raiz / "saida" / "relatorio")
        )
        linhas = [(r.id_pedido, r.uf, r.valor_total_pedido) for r in resultado.collect()]
        assert linhas == TestFluxoCompleto.ESPERADO
        log = (raiz / "logs" / "pipeline.log").read_text(encoding="utf-8")
        assert "registros_gravados" in log and "SparkSession encerrada." in log

    def test_falha_de_leitura_encerra_com_codigo_1_e_fecha_a_sessao(self, tmp_path):
        _, arquivo, outro_dir = self.preparar_projeto(tmp_path, caminho_pedidos="./nao_existe")
        execucao = self.executar_main(arquivo, outro_dir)
        assert execucao.returncode == 1
        assert "DataReadException" in execucao.stdout
        assert "SparkSession encerrada." in execucao.stdout
