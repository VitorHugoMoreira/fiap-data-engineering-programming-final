"""Catálogo central de schemas explícitos — nenhum DataFrame da aplicação usa inferência."""

from __future__ import annotations

from typing import ClassVar

from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)


class DataSchemas:
    """Schemas de entrada/saída; datas em TimestampNTZType (origem sem fuso horário)."""

    MONEY: ClassVar[DecimalType] = DecimalType(10, 2)
    TOTAL_MONEY: ClassVar[DecimalType] = DecimalType(18, 2)

    PEDIDOS: ClassVar[StructType] = StructType(
        [
            StructField("id_pedido", StringType(), True),
            StructField("produto", StringType(), True),
            StructField("valor_unitario", MONEY, True),
            StructField("quantidade", LongType(), True),
            StructField("data_criacao", TimestampNTZType(), True),
            StructField("uf", StringType(), True),
            StructField("id_cliente", LongType(), True),
        ]
    )

    AVALIACAO_FRAUDE: ClassVar[StructType] = StructType(
        [
            StructField("fraude", BooleanType(), True),
            StructField("score", DoubleType(), True),
        ]
    )

    PAGAMENTOS: ClassVar[StructType] = StructType(
        [
            StructField("id_pedido", StringType(), True),
            StructField("forma_pagamento", StringType(), True),
            StructField("valor_pagamento", MONEY, True),
            StructField("status", BooleanType(), True),
            StructField("data_processamento", TimestampNTZType(), True),
            StructField("avaliacao_fraude", AVALIACAO_FRAUDE, True),
        ]
    )

    RELATORIO: ClassVar[StructType] = StructType(
        [
            StructField("id_pedido", StringType(), True),
            StructField("uf", StringType(), True),
            StructField("forma_pagamento", StringType(), True),
            StructField("valor_total_pedido", TOTAL_MONEY, True),
            StructField("data_pedido", TimestampNTZType(), True),
        ]
    )

    @classmethod
    def same_structure(cls, actual: StructType, expected: StructType) -> bool:
        """Compara nomes e tipos das colunas (ignora nulabilidade, que o Spark recalcula)."""
        return [(f.name, f.dataType) for f in actual.fields] == [
            (f.name, f.dataType) for f in expected.fields
        ]
