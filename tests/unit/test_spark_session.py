"""Testes do gerenciador da sessão Spark."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from data_engineering_pyspark.config.settings import SparkConfig
from data_engineering_pyspark.session.spark_session import SparkSessionManager

pytestmark = pytest.mark.unit


class TestSparkSessionManager:
    def test_retorna_e_reutiliza_a_mesma_sessao(self, spark):
        manager = SparkSessionManager(SparkConfig(app_name="teste", log_level="ERROR"))
        primeira = manager.get_session()
        assert isinstance(primeira, SparkSession)
        assert manager.get_session() is primeira

    def test_stop_sem_sessao_e_idempotente(self):
        manager = SparkSessionManager(SparkConfig(app_name="teste"))
        manager.stop()
        manager.stop()

    def test_aplica_as_opcoes_da_configuracao(self, spark):
        options = {"spark.sql.shuffle.partitions": "3"}
        try:
            sessao = SparkSessionManager(SparkConfig(app_name="t", options=options)).get_session()
            assert sessao.conf.get("spark.sql.shuffle.partitions") == "3"
        finally:
            spark.conf.set("spark.sql.shuffle.partitions", "2")
