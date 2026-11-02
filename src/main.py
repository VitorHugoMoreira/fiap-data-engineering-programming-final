"""Aggregation Root (Composition Root): instancia e injeta todas as dependências."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from pyspark.errors import PySparkException

from data_engineering_pyspark.business.exceptions import BusinessRuleException
from data_engineering_pyspark.business.order_business import OrderBusiness
from data_engineering_pyspark.config.settings import (
    ConfigLoader,
    ConfigurationError,
    LoggingConfig,
    LoggingConfigurator,
)
from data_engineering_pyspark.io_utils.data_handler import DataHandler
from data_engineering_pyspark.io_utils.exceptions import DataHandlerException
from data_engineering_pyspark.pipeline.exceptions import PipelineException
from data_engineering_pyspark.pipeline.order_pipeline import OrderPipeline
from data_engineering_pyspark.session.spark_session import SparkSessionManager

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # src/main.py -> raiz do projeto


def main(argv: list[str] | None = None) -> int:
    """Ponto de entrada: monta o grafo de dependências e executa o pipeline."""
    parser = argparse.ArgumentParser(description="Relatório de pedidos recusados e legítimos")
    parser.add_argument("--config", help="caminho do settings.yaml (opcional)")
    args = parser.parse_args(argv)

    LoggingConfigurator(LoggingConfig()).configure()  # logging mínimo até ler o YAML
    try:
        config = ConfigLoader(args.config, PROJECT_ROOT).load()  # 1. classe de configuração
    except ConfigurationError:
        logger.exception("Configuração inválida. Execução abortada.")
        return 2
    LoggingConfigurator(config.logging).configure()
    logger.info("Iniciando job '%s' (ambiente=%s)", config.spark.app_name, config.environment)

    spark_manager = SparkSessionManager(config.spark)  # 2. gerenciador da sessão Spark
    try:
        spark = spark_manager.get_session()
        data_handler = DataHandler(  # 3. leitura e escrita
            spark, config.pedidos, config.pagamentos, config.output
        )
        business = OrderBusiness(config.business)  # 4. lógica de negócio
        pipeline = OrderPipeline(data_handler, business, config.pipeline)  # 5. orquestração
        pipeline.run()
        return 0
    except DataHandlerException:
        logger.exception("Falha na camada de leitura/escrita de dados.")
    except BusinessRuleException:
        logger.exception("Falha na aplicação das regras de negócio.")
    except PipelineException:
        logger.exception("Falha na orquestração do pipeline.")
    except PySparkException:
        logger.exception("Erro originado no PySpark.")
    except Exception:
        logger.exception("Erro inesperado durante a execução do job.")
    finally:
        spark_manager.stop()
    return 1


if __name__ == "__main__":
    sys.exit(main())
