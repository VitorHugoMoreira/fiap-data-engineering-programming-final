"""Orquestração do fluxo: leitura → regras de negócio → validação → Parquet → verificação."""

from __future__ import annotations

import logging
import time
from dataclasses import asdict, dataclass

from pyspark.sql import DataFrame

from data_engineering_pyspark.business.order_business import OrderBusiness, ReportBuildResult
from data_engineering_pyspark.config.settings import PipelineConfig
from data_engineering_pyspark.io_utils.data_handler import DataHandler
from data_engineering_pyspark.pipeline.exceptions import PipelineException

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PipelineMetrics:
    """Resumo quantitativo da execução (todos os valores vêm de ações reais do Spark)."""

    pedidos_lidos: int
    pagamentos_lidos: int
    pedidos_apos_agregacao: int
    pedidos_no_ano_referencia: int
    pagamentos_recusados: int
    pagamentos_recusados_legitimos: int
    pedidos_elegiveis: int
    registros_gravados: int


class OrderPipeline:
    """Coordena as dependências injetadas; não contém regras de negócio."""

    def __init__(
        self,
        data_handler: DataHandler,
        business: OrderBusiness,
        config: PipelineConfig,
    ) -> None:
        self._data_handler = data_handler
        self._business = business
        self._config = config

    def run(self) -> PipelineMetrics:
        """Executa o pipeline completo e devolve as métricas da execução."""
        started = time.perf_counter()
        logger.info("===== Pipeline de pedidos recusados/legítimos iniciado =====")

        cached: list[DataFrame] = []
        try:
            logger.info("[1/5] Leitura dos datasets de entrada")
            pedidos = self._cache(self._data_handler.read_pedidos(), cached)
            pagamentos = self._cache(self._data_handler.read_pagamentos(), cached)

            logger.info("[2/5] Execução das regras de negócio")
            result = self._business.build_report(pedidos, pagamentos)

            logger.info("[3/5] Validação do resultado antes da escrita")
            self._business.validate_report(result.report)

            logger.info("[4/5] Persistência do relatório em Parquet")
            self._data_handler.write_parquet(result.report)

            logger.info("[5/5] Verificação pós-escrita (releitura do Parquet)")
            written = self._data_handler.read_parquet()
            self._business.validate_report(written)

            metrics = self._collect_metrics(pedidos, pagamentos, result, written)
            if metrics.registros_gravados != metrics.pedidos_elegiveis:
                raise PipelineException(
                    f"Divergência: {metrics.pedidos_elegiveis} calculados x "
                    f"{metrics.registros_gravados} gravados"
                )
        finally:
            for df in cached:  # libera o cache inclusive quando uma etapa falha
                df.unpersist()

        self._log_metrics(metrics)
        logger.info(
            "===== Pipeline concluído com sucesso em %.1fs =====", time.perf_counter() - started
        )
        return metrics

    def _cache(self, df: DataFrame, cached: list[DataFrame]) -> DataFrame:
        """Mantém a entrada em memória quando configurado (várias ações reutilizam a leitura)."""
        if not self._config.cache_inputs:
            return df
        cached.append(df.cache())
        return df

    @staticmethod
    def _collect_metrics(
        pedidos: DataFrame,
        pagamentos: DataFrame,
        result: ReportBuildResult,
        written: DataFrame,
    ) -> PipelineMetrics:
        """Conta os registros de cada etapa para o resumo quantitativo."""
        return PipelineMetrics(
            pedidos_lidos=pedidos.count(),
            pagamentos_lidos=pagamentos.count(),
            pedidos_apos_agregacao=result.pedidos_agregados.count(),
            pedidos_no_ano_referencia=result.pedidos_ano_referencia.count(),
            pagamentos_recusados=result.pagamentos_recusados.count(),
            pagamentos_recusados_legitimos=result.pagamentos_recusados_legitimos.count(),
            pedidos_elegiveis=result.report.count(),
            registros_gravados=written.count(),
        )

    @staticmethod
    def _log_metrics(metrics: PipelineMetrics) -> None:
        """Registra o resumo quantitativo da execução."""
        logger.info("Resumo da execução:")
        for name, value in asdict(metrics).items():
            logger.info("  %-32s %8d", name, value)
