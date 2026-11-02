"""Utilitários compartilhados pelos testes, encapsulados em classe."""

from __future__ import annotations

import gzip
import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import yaml
from pyspark.sql import DataFrame, SparkSession

from data_engineering_pyspark.schemas.data_schemas import DataSchemas

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class DadosDeTeste:
    """Fábricas de valores e arquivos usados pelos testes."""

    PROJECT_CONFIG = PROJECT_ROOT / "config" / "settings.yaml"
    CABECALHO_PEDIDOS = "ID_PEDIDO;PRODUTO;VALOR_UNITARIO;QUANTIDADE;DATA_CRIACAO;UF;ID_CLIENTE"

    @staticmethod
    def ts(value: str) -> datetime:
        """Converte 'AAAA-MM-DD HH:MM:SS' em datetime."""
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")

    @staticmethod
    def ids(df: DataFrame) -> list[str]:
        """Lista os id_pedido do DataFrame, na ordem em que são devolvidos."""
        return [row.id_pedido for row in df.collect()]

    @staticmethod
    def write_yaml(path: Path, data: dict) -> str:
        """Grava um dicionário como YAML e devolve o caminho."""
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(data, allow_unicode=True), encoding="utf-8")
        return str(path)

    @classmethod
    def project_config(cls) -> dict:
        """Conteúdo do config/settings.yaml do projeto."""
        with cls.PROJECT_CONFIG.open(encoding="utf-8") as file:
            return yaml.safe_load(file)

    @classmethod
    def write_pedidos_gz(cls, path: Path, linhas: list[str], cabecalho: str | None = None) -> None:
        """Grava um CSV gzip no layout oficial (separador ';', com cabeçalho)."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "wt", encoding="utf-8") as file:
            file.write("\n".join([cabecalho or cls.CABECALHO_PEDIDOS, *linhas]) + "\n")

    @staticmethod
    def write_pagamentos_gz(path: Path, registros: list[dict]) -> None:
        """Grava um JSON Lines gzip no layout oficial."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "wt", encoding="utf-8") as file:
            for registro in registros:
                file.write(json.dumps(registro) + "\n")

    @staticmethod
    def pagamento(idp: str, forma: str, status: bool, fraude: bool) -> dict:
        """Registro de pagamento no formato do dataset oficial."""
        return {
            "id_pedido": idp,
            "forma_pagamento": forma,
            "valor_pagamento": 10.0,
            "status": status,
            "data_processamento": "2025-05-01T11:00:00.000001",
            "avaliacao_fraude": {"fraude": fraude, "score": 0.1},
        }


class FabricaDataFrames:
    """Cria DataFrames de teste sempre com os schemas explícitos de DataSchemas."""

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    def pedidos(self, rows: list[tuple]) -> DataFrame:
        """Linhas: (id, produto, valor_unitario, quantidade, data_criacao, uf, id_cliente)."""
        data = [
            (idp, prod, self._dec(vu), qtd, self._ts(dt), uf, cli)
            for idp, prod, vu, qtd, dt, uf, cli in rows
        ]
        return self._spark.createDataFrame(data, DataSchemas.PEDIDOS)

    def pagamentos(self, rows: list[tuple]) -> DataFrame:
        """Linhas: (id, forma, valor, status, data_processamento, fraude, score)."""
        data = [
            (
                idp,
                forma,
                self._dec(valor),
                status,
                self._ts(dt),
                None if fraude is None and score is None else (fraude, score),
            )
            for idp, forma, valor, status, dt, fraude, score in rows
        ]
        return self._spark.createDataFrame(data, DataSchemas.PAGAMENTOS)

    @staticmethod
    def _dec(value) -> Decimal | None:
        return None if value is None else Decimal(str(value))

    @staticmethod
    def _ts(value: str | None) -> datetime | None:
        return None if value is None else DadosDeTeste.ts(value)
