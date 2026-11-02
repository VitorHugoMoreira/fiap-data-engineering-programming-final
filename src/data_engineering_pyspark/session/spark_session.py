"""Gerenciamento do ciclo de vida da SparkSession."""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession

from data_engineering_pyspark.config.settings import SparkConfig

logger = logging.getLogger(__name__)


class SparkSessionManager:
    """Cria, disponibiliza e encerra a SparkSession a partir da configuração injetada."""

    def __init__(self, config: SparkConfig) -> None:
        self._config = config
        self._session: SparkSession | None = None

    def get_session(self) -> SparkSession:
        """Retorna a sessão ativa, criando-a na primeira chamada (``getOrCreate``)."""
        if self._session is None:
            builder = SparkSession.builder.appName(self._config.app_name)
            if self._config.master:  # sem master no YAML, vale o --master do spark-submit
                builder = builder.master(self._config.master)
            for key, value in self._config.options.items():
                builder = builder.config(key, value)
            self._session = builder.getOrCreate()
            self._session.sparkContext.setLogLevel(self._config.log_level)
            logger.info(
                "SparkSession iniciada: app='%s', master='%s', versão Spark=%s",
                self._config.app_name,
                self._session.sparkContext.master,
                self._session.version,
            )
        return self._session

    def stop(self) -> None:
        """Encerra a sessão, se existir. É idempotente e seguro em blocos ``finally``."""
        if self._session is not None:
            self._session.stop()
            self._session = None
            logger.info("SparkSession encerrada.")
