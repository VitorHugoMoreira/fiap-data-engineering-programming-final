"""Leitura dos datasets de entrada e escrita/releitura do relatório em Parquet."""

from __future__ import annotations

import logging
from collections.abc import Callable

from pyspark.errors import AnalysisException, PySparkException
from pyspark.sql import DataFrame, SparkSession

from data_engineering_pyspark.config.settings import (
    CsvSourceConfig,
    JsonSourceConfig,
    ParquetOutputConfig,
)
from data_engineering_pyspark.io_utils.exceptions import DataReadException, DataWriteException
from data_engineering_pyspark.schemas.data_schemas import DataSchemas

logger = logging.getLogger(__name__)


class DataHandler:
    """Responsável exclusivamente por I/O: não aplica nenhuma regra de negócio."""

    def __init__(
        self,
        spark: SparkSession,
        pedidos_config: CsvSourceConfig,
        pagamentos_config: JsonSourceConfig,
        output_config: ParquetOutputConfig,
    ) -> None:
        self._spark = spark
        self._pedidos = pedidos_config
        self._pagamentos = pagamentos_config
        self._output = output_config

    def read_pedidos(self) -> DataFrame:
        """Lê os pedidos (CSV ';' com cabeçalho, gzip) aplicando o schema explícito."""
        cfg = self._pedidos
        return self._read(
            "pedidos",
            cfg.path,
            lambda: self._spark.read.schema(DataSchemas.PEDIDOS)
            .option("header", cfg.header)
            .option("enforceSchema", cfg.enforce_schema)
            .option("sep", cfg.sep)
            .option("encoding", cfg.encoding)
            .option("compression", cfg.compression)
            .option("timestampNTZFormat", cfg.timestamp_format)
            .option("mode", cfg.mode)
            .option("pathGlobFilter", cfg.file_pattern)
            .csv(cfg.path),
        )

    def read_pagamentos(self) -> DataFrame:
        """Lê os pagamentos (JSON Lines, gzip) com o objeto aninhado ``avaliacao_fraude``."""
        cfg = self._pagamentos
        return self._read(
            "pagamentos",
            cfg.path,
            lambda: self._spark.read.schema(DataSchemas.PAGAMENTOS)
            .option("compression", cfg.compression)
            .option("timestampNTZFormat", cfg.timestamp_format)
            .option("mode", cfg.mode)
            .option("pathGlobFilter", cfg.file_pattern)
            .json(cfg.path),
        )

    def write_parquet(self, df: DataFrame) -> None:
        """Grava o relatório em Parquet; ``coalesce`` preserva a ordenação global."""
        cfg = self._output
        logger.info(
            "Gravando Parquet em '%s' (mode=%s, compression=%s, arquivos=%d)",
            cfg.path,
            cfg.mode,
            cfg.compression,
            cfg.num_files,
        )
        try:
            (
                df.coalesce(cfg.num_files)
                .write.mode(cfg.mode)
                .option("compression", cfg.compression)
                .parquet(cfg.path)
            )
        except PySparkException as exc:
            logger.error("Falha ao gravar Parquet [classe=%s]", exc.getCondition())
            raise DataWriteException(f"Falha ao gravar o relatório em '{cfg.path}'") from exc
        logger.info("Parquet gravado com sucesso em '%s'", cfg.path)

    def read_parquet(self) -> DataFrame:
        """Relê o relatório gravado, com schema explícito, para validação pós-escrita."""
        cfg = self._output
        try:
            return self._spark.read.schema(DataSchemas.RELATORIO).parquet(cfg.path)
        except PySparkException as exc:
            logger.error("Falha ao reler Parquet [classe=%s]", exc.getCondition())
            raise DataWriteException(f"Não foi possível reler o relatório em '{cfg.path}'") from exc

    def _read(self, dataset: str, path: str, reader: Callable[[], DataFrame]) -> DataFrame:
        """Executa a leitura traduzindo erros do Spark para exceções de domínio."""
        logger.info("Lendo dataset de %s em '%s'", dataset, path)
        try:
            df = reader()
            if df.isEmpty():
                logger.warning(
                    "ATENÇÃO: dataset de %s em '%s' não contém registros.", dataset, path
                )
            return df
        except AnalysisException as exc:
            logger.error("Erro de análise ao ler %s [classe=%s]", dataset, exc.getCondition())
            raise DataReadException(f"Falha ao carregar {dataset} a partir de '{path}'") from exc
        except PySparkException as exc:
            logger.error("Erro do Spark ao ler %s [classe=%s]", dataset, exc.getCondition())
            raise DataReadException(f"Erro no motor Spark ao ler {dataset} em '{path}'") from exc
