"""Regras de negócio: pedidos com pagamento recusado e avaliação de fraude legítima."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import ClassVar

from pyspark.errors import PySparkException
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from data_engineering_pyspark.business.exceptions import (
    BusinessRuleException,
    DataQualityException,
)
from data_engineering_pyspark.config.settings import BusinessRulesConfig
from data_engineering_pyspark.schemas.data_schemas import DataSchemas

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReportBuildResult:
    """Relatório final e DataFrames intermediários (usados para métricas de execução)."""

    report: DataFrame
    pedidos_agregados: DataFrame
    pedidos_ano_referencia: DataFrame
    pagamentos_recusados: DataFrame
    pagamentos_recusados_legitimos: DataFrame


class OrderBusiness:
    """Concentra as transformações PySpark e as regras de negócio do relatório."""

    REPORT_SOURCE_COLUMNS: ClassVar[dict[str, str]] = {
        "id_pedido": "id_pedido",
        "uf": "uf",
        "forma_pagamento": "forma_pagamento",
        "valor_total_pedido": "valor_total_pedido",
        "data_pedido": "data_criacao",
    }
    REPORT_SORT_KEYS: ClassVar[tuple[str, ...]] = ("uf", "forma_pagamento", "data_pedido")
    TIEBREAKER_KEY: ClassVar[str] = "id_pedido"

    def __init__(self, rules: BusinessRulesConfig) -> None:
        self._rules = rules

    # ------------------------------------------------------------------ fluxo
    def build_report(self, pedidos_df: DataFrame, pagamentos_df: DataFrame) -> ReportBuildResult:
        """Compõe todas as regras na ordem correta e devolve o relatório ordenado."""
        logger.info(
            "Aplicando regras de negócio (ano de referência=%d)", self._rules.reference_year
        )
        try:
            pedidos_agregados = self.calculate_order_totals(self.prepare_orders(pedidos_df))
            pedidos_ano = self.filter_reference_year(pedidos_agregados)

            pagamentos = self.prepare_payments(pagamentos_df)
            recusados = self.filter_refused_payments(pagamentos)
            recusados_legitimos = self.filter_legitimate_payments(recusados)
            self.ensure_single_payment_per_order(recusados_legitimos)

            relacionados = self.join_orders_payments(pedidos_ano, recusados_legitimos)
            report = self.sort_report(self.select_report_columns(relacionados))
        except DataQualityException:
            logger.error("Regras de negócio interrompidas por inconsistência nos dados.")
            raise
        except PySparkException as exc:
            logger.exception("Falha do Spark ao aplicar as regras de negócio.")
            raise BusinessRuleException("Erro ao construir o relatório de pedidos") from exc

        logger.info("Regras de negócio aplicadas com sucesso.")
        return ReportBuildResult(
            report=report,
            pedidos_agregados=pedidos_agregados,
            pedidos_ano_referencia=pedidos_ano,
            pagamentos_recusados=recusados,
            pagamentos_recusados_legitimos=recusados_legitimos,
        )

    # ------------------------------------------------------------ preparação
    def prepare_orders(self, pedidos_df: DataFrame) -> DataFrame:
        """Padroniza chaves, descarta itens inválidos e remove linhas 100% duplicadas."""
        logger.info("Preparando dados de pedidos")
        df = pedidos_df.withColumn("id_pedido", F.trim("id_pedido")).withColumn(
            "uf", F.upper(F.trim("uf"))
        )
        valid = (
            F.col("id_pedido").isNotNull()
            & (F.col("id_pedido") != "")
            & F.col("uf").isNotNull()
            & F.col("data_criacao").isNotNull()
            & F.col("valor_unitario").isNotNull()
            & F.col("quantidade").isNotNull()
            & (F.col("valor_unitario") >= 0)
            & (F.col("quantidade") >= 0)
        )
        return self._clean(df, valid, "pedidos")

    def prepare_payments(self, pagamentos_df: DataFrame) -> DataFrame:
        """Achata o objeto ``avaliacao_fraude`` e descarta pagamentos não classificáveis."""
        logger.info("Preparando dados de pagamentos (achatando avaliacao_fraude)")
        df = pagamentos_df.select(
            F.trim("id_pedido").alias("id_pedido"),
            F.upper(F.trim("forma_pagamento")).alias("forma_pagamento"),
            F.col("valor_pagamento"),
            F.col("status"),
            F.col("data_processamento"),
            F.col("avaliacao_fraude.fraude").alias("fraude"),
            F.col("avaliacao_fraude.score").alias("score_fraude"),
        )
        valid = (
            F.col("id_pedido").isNotNull()
            & (F.col("id_pedido") != "")
            & F.col("forma_pagamento").isNotNull()
            & F.col("status").isNotNull()
            & F.col("fraude").isNotNull()
        )
        return self._clean(df, valid, "pagamentos")

    # --------------------------------------------------------------- cálculo
    def calculate_order_totals(self, pedidos_df: DataFrame) -> DataFrame:
        """Agrega itens por pedido: valor_total_pedido = SUM(valor_unitario * quantidade)."""
        logger.info("Calculando valor total por pedido: SUM(valor_unitario * quantidade)")
        totals = pedidos_df.groupBy("id_pedido", "uf", "data_criacao").agg(
            F.sum(F.col("valor_unitario") * F.col("quantidade"))
            .cast(DataSchemas.TOTAL_MONEY)
            .alias("valor_total_pedido")
        )
        self._assert_unique(totals, "id_pedido", "pedidos com UF/data divergentes entre itens")
        return totals

    # --------------------------------------------------------------- filtros
    def filter_refused_payments(self, pagamentos_df: DataFrame) -> DataFrame:
        """Mantém apenas pagamentos recusados (status = false)."""
        logger.info("Filtrando pagamentos recusados (status=%s)", self._rules.payment_status)
        return pagamentos_df.filter(F.col("status") == F.lit(self._rules.payment_status))

    def filter_legitimate_payments(self, pagamentos_df: DataFrame) -> DataFrame:
        """Mantém apenas transações classificadas como legítimas (fraude = false)."""
        logger.info("Filtrando avaliações legítimas (fraude=%s)", self._rules.fraud_flag)
        return pagamentos_df.filter(F.col("fraude") == F.lit(self._rules.fraud_flag))

    def filter_reference_year(self, pedidos_df: DataFrame) -> DataFrame:
        """Mantém pedidos criados entre 01/01 e 31/12 do ano de referência (inclusive)."""
        year = self._rules.reference_year
        logger.info("Filtrando pedidos criados em %d (data_criacao)", year)
        return pedidos_df.filter(F.year("data_criacao") == F.lit(year))

    def ensure_single_payment_per_order(self, pagamentos_df: DataFrame) -> DataFrame:
        """Interrompe o fluxo se houver mais de um pagamento elegível para o mesmo pedido."""
        self._assert_unique(
            pagamentos_df, "id_pedido", "pedidos com múltiplos pagamentos elegíveis"
        )
        return pagamentos_df

    # ---------------------------------------------------------- relacionamento
    def join_orders_payments(self, pedidos_df: DataFrame, pagamentos_df: DataFrame) -> DataFrame:
        """Relaciona pedidos (1 linha/pedido) e pagamentos (1 linha/pedido) por id_pedido."""
        logger.info("Relacionando pedidos e pagamentos por id_pedido (inner join)")
        return pedidos_df.join(pagamentos_df, on="id_pedido", how="inner")

    # ------------------------------------------------------------- projeção
    def select_report_columns(self, df: DataFrame) -> DataFrame:
        """Projeta exatamente os cinco atributos exigidos, nos tipos de DataSchemas.RELATORIO."""
        logger.info("Projetando os atributos do relatório")
        return df.select(
            *[
                F.col(self.REPORT_SOURCE_COLUMNS[field.name]).cast(field.dataType).alias(field.name)
                for field in DataSchemas.RELATORIO.fields
            ]
        )

    def sort_report(self, df: DataFrame) -> DataFrame:
        """Ordena por UF, forma de pagamento e data do pedido (crescente).

        ``id_pedido`` entra apenas como critério técnico de desempate, para que o
        resultado seja determinístico entre execuções.
        """
        logger.info("Ordenando por %s", ", ".join(self.REPORT_SORT_KEYS))
        keys = [F.col(key).asc() for key in (*self.REPORT_SORT_KEYS, self.TIEBREAKER_KEY)]
        return df.orderBy(*keys)

    # ------------------------------------------------------------- validação
    def validate_report(self, report_df: DataFrame) -> None:
        """Valida schema, unicidade, obrigatoriedade e ano de referência do resultado."""
        logger.info("Validando consistência do relatório")
        if not DataSchemas.same_structure(report_df.schema, DataSchemas.RELATORIO):
            logger.error("Schema do relatório divergente: %s", report_df.schema.simpleString())
            raise DataQualityException("Schema do relatório diverge de DataSchemas.RELATORIO")
        try:
            stats = report_df.agg(
                F.count(F.lit(1)).alias("linhas"),
                F.countDistinct("id_pedido").alias("pedidos_distintos"),
                F.sum(self._any_null(report_df.columns).cast("int")).alias("linhas_com_nulo"),
                F.sum((F.year("data_pedido") != self._rules.reference_year).cast("int")).alias(
                    "fora_do_ano"
                ),
            ).first()
        except PySparkException as exc:
            logger.exception("Falha do Spark ao validar o relatório.")
            raise BusinessRuleException("Erro ao validar o relatório") from exc

        problems = []
        if stats["linhas"] != stats["pedidos_distintos"]:
            problems.append(f"{stats['linhas'] - stats['pedidos_distintos']} pedidos duplicados")
        if stats["linhas_com_nulo"]:
            problems.append(f"{stats['linhas_com_nulo']} linhas com atributos nulos")
        if stats["fora_do_ano"]:
            problems.append(f"{stats['fora_do_ano']} pedidos fora de {self._rules.reference_year}")
        if problems:
            logger.error("Relatório inconsistente: %s", "; ".join(problems))
            raise DataQualityException("Relatório inconsistente: " + "; ".join(problems))
        logger.info("Relatório consistente: %d pedidos, 1 linha por pedido.", stats["linhas"])

    # ------------------------------------------------------------- auxiliares
    def _clean(self, df: DataFrame, valid: Column, dataset: str) -> DataFrame:
        """Descarta registros inválidos e duplicatas exatas, registrando as quantidades."""
        try:
            invalid = df.filter(~valid).count()
            valid_df = df.filter(valid)
            total_valid = valid_df.count()
            deduplicated = valid_df.dropDuplicates()
            duplicates = total_valid - deduplicated.count()
        except PySparkException as exc:
            logger.exception("Falha do Spark ao preparar %s.", dataset)
            raise BusinessRuleException(f"Erro ao preparar {dataset}") from exc
        if invalid:
            logger.warning(
                "%d registros de %s descartados por atributos nulos/inválidos", invalid, dataset
            )
        if duplicates:
            logger.warning("%d linhas duplicadas de %s removidas", duplicates, dataset)
        logger.info(
            "%s preparados: %d registros válidos", dataset.capitalize(), total_valid - duplicates
        )
        return deduplicated

    def _assert_unique(self, df: DataFrame, key: str, description: str) -> None:
        """Lança ``DataQualityException`` se ``key`` não for única no DataFrame."""
        duplicated = df.groupBy(key).count().filter(F.col("count") > 1)
        samples = [row[key] for row in duplicated.limit(5).collect()]
        if samples:
            logger.error("Violação de unicidade (%s). Exemplos: %s", description, samples)
            raise DataQualityException(
                f"Violação de unicidade em '{key}': {description}. Exemplos: {samples}"
            )

    @staticmethod
    def _any_null(columns: list[str]) -> Column:
        """Expressão verdadeira quando qualquer coluna da linha é nula."""
        condition = F.lit(False)
        for name in columns:
            condition = condition | F.col(name).isNull()
        return condition
