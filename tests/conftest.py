"""Fixtures compartilhadas: SparkSession única e fábricas de DataFrames com schema explícito."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from data_engineering_pyspark.business.order_business import OrderBusiness
from data_engineering_pyspark.config.settings import BusinessRulesConfig
from tests.helpers import FabricaDataFrames


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """SparkSession local compartilhada por toda a suíte e encerrada ao final."""
    session = (
        SparkSession.builder.appName("tests-relatorio-pedidos")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def business() -> OrderBusiness:
    """Regras do enunciado: ano 2025, status=false, fraude=false."""
    return OrderBusiness(BusinessRulesConfig(reference_year=2025))


@pytest.fixture
def make_pedidos(spark: SparkSession):
    """Fábrica de pedidos com DataSchemas.PEDIDOS."""
    return FabricaDataFrames(spark).pedidos


@pytest.fixture
def make_pagamentos(spark: SparkSession):
    """Fábrica de pagamentos com DataSchemas.PAGAMENTOS."""
    return FabricaDataFrames(spark).pagamentos
