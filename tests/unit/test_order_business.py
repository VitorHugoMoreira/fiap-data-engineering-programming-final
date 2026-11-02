"""Testes unitários da classe de lógica de negócio OrderBusiness."""

from __future__ import annotations

import logging
from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType, StructField, StructType, TimestampType

from data_engineering_pyspark.business.exceptions import (
    BusinessRuleException,
    DataQualityException,
)
from data_engineering_pyspark.business.order_business import OrderBusiness
from data_engineering_pyspark.config.settings import BusinessRulesConfig
from data_engineering_pyspark.schemas.data_schemas import DataSchemas
from tests.helpers import DadosDeTeste

pytestmark = pytest.mark.unit

REPORT_COLUMNS = ["id_pedido", "uf", "forma_pagamento", "valor_total_pedido", "data_pedido"]


class TestPrepareOrders:
    def test_descarta_registros_nulos_ou_invalidos(self, business, make_pedidos):
        pedidos = make_pedidos(
            [
                ("ok", "TV", 1500, 1, "2025-01-10 10:00:00", "SP", 1),
                (None, "TV", 1500, 1, "2025-01-10 10:00:00", "SP", 1),
                ("sem-valor", "TV", None, 1, "2025-01-10 10:00:00", "SP", 1),
                ("sem-qtd", "TV", 1500, None, "2025-01-10 10:00:00", "SP", 1),
                ("sem-data", "TV", 1500, 1, None, "SP", 1),
                ("sem-uf", "TV", 1500, 1, "2025-01-10 10:00:00", None, 1),
                ("negativo", "TV", -1, 1, "2025-01-10 10:00:00", "SP", 1),
            ]
        )
        assert DadosDeTeste.ids(business.prepare_orders(pedidos)) == ["ok"]

    def test_remove_linhas_identicas_mas_preserva_itens_distintos(self, business, make_pedidos):
        linha = ("p1", "TV", 1500, 1, "2025-01-10 10:00:00", "SP", 1)
        pedidos = make_pedidos(
            [linha, linha, ("p1", "CELULAR", 1000, 1, "2025-01-10 10:00:00", "SP", 1)]
        )
        assert business.prepare_orders(pedidos).count() == 2

    def test_padroniza_uf(self, business, make_pedidos):
        pedidos = make_pedidos([("p1", "TV", 1500, 1, "2025-01-10 10:00:00", " sp ", 1)])
        assert business.prepare_orders(pedidos).first().uf == "SP"


class TestPreparePayments:
    def test_achata_avaliacao_fraude(self, business, make_pagamentos):
        pagamentos = make_pagamentos([("p1", "PIX", 95, False, "2025-01-10 11:00:00", False, 0.2)])
        resultado = business.prepare_payments(pagamentos)
        assert "avaliacao_fraude" not in resultado.columns
        linha = resultado.first()
        assert (linha.fraude, linha.score_fraude) == (False, pytest.approx(0.2))

    def test_descarta_pagamentos_nao_classificaveis(self, business, make_pagamentos):
        pagamentos = make_pagamentos(
            [
                ("ok", "PIX", 95, False, "2025-01-10 11:00:00", False, 0.2),
                ("sem-status", "PIX", 95, None, "2025-01-10 11:00:00", False, 0.2),
                ("sem-avaliacao", "PIX", 95, False, "2025-01-10 11:00:00", None, None),
                ("sem-forma", None, 95, False, "2025-01-10 11:00:00", False, 0.2),
            ]
        )
        assert DadosDeTeste.ids(business.prepare_payments(pagamentos)) == ["ok"]


class TestCalculateOrderTotals:
    @pytest.mark.parametrize(
        "itens, esperado",
        [
            ([(1500, 2)], Decimal("3000.00")),  # um item, quantidade 2
            ([(1500, 2), (1000, 1)], Decimal("4000.00")),  # pedido com dois itens
            ([(0.1, 3)], Decimal("0.30")),  # Decimal evita 0.30000000000000004
            ([(500, 0)], Decimal("0.00")),  # quantidade zero
        ],
    )
    def test_soma_valor_unitario_vezes_quantidade(self, business, make_pedidos, itens, esperado):
        pedidos = make_pedidos(
            [
                ("p1", f"PROD{i}", vu, q, "2025-05-05 12:00:00", "SP", 1)
                for i, (vu, q) in enumerate(itens)
            ]
        )
        resultado = business.calculate_order_totals(pedidos).collect()
        assert len(resultado) == 1
        assert resultado[0].valor_total_pedido == esperado

    def test_total_e_decimal_com_duas_casas(self, business, make_pedidos):
        pedidos = make_pedidos([("p1", "TV", 1500, 1, "2025-05-05 12:00:00", "SP", 1)])
        tipo = business.calculate_order_totals(pedidos).schema["valor_total_pedido"].dataType
        assert tipo == DecimalType(18, 2)

    def test_pedido_com_atributos_divergentes_interrompe(self, business, make_pedidos):
        pedidos = make_pedidos(
            [
                ("p1", "TV", 1500, 1, "2025-05-05 12:00:00", "SP", 1),
                ("p1", "CELULAR", 1000, 1, "2025-05-05 12:00:00", "RJ", 1),
            ]
        )
        with pytest.raises(DataQualityException, match="id_pedido"):
            business.calculate_order_totals(pedidos)


class TestFiltrosDePagamento:
    @pytest.fixture
    def pagamentos(self, business, make_pagamentos):
        return business.prepare_payments(
            make_pagamentos(
                [
                    ("aprovado-legitimo", "PIX", 95, True, "2025-01-10 11:00:00", False, 0.1),
                    ("recusado-fraude", "PIX", 95, False, "2025-01-10 11:00:00", True, 0.9),
                    ("recusado-legitimo", "PIX", 95, False, "2025-01-10 11:00:00", False, 0.2),
                ]
            )
        )

    def test_exclui_pagamentos_aprovados(self, business, pagamentos):
        assert sorted(DadosDeTeste.ids(business.filter_refused_payments(pagamentos))) == [
            "recusado-fraude",
            "recusado-legitimo",
        ]

    def test_exclui_pagamentos_classificados_como_fraude(self, business, pagamentos):
        recusados = business.filter_refused_payments(pagamentos)
        assert DadosDeTeste.ids(business.filter_legitimate_payments(recusados)) == [
            "recusado-legitimo"
        ]

    def test_multiplos_pagamentos_elegiveis_interrompem(self, business, make_pagamentos):
        pagamentos = business.prepare_payments(
            make_pagamentos(
                [
                    ("p1", "PIX", 95, False, "2025-01-10 11:00:00", False, 0.2),
                    ("p1", "BOLETO", 95, False, "2025-01-11 11:00:00", False, 0.3),
                ]
            )
        )
        with pytest.raises(DataQualityException, match="múltiplos pagamentos"):
            business.ensure_single_payment_per_order(pagamentos)


class TestFilterReferenceYear:
    @pytest.mark.parametrize(
        "data_criacao, incluido",
        [
            ("2024-12-31 23:59:59", False),
            ("2025-01-01 00:00:00", True),
            ("2025-07-15 12:30:00", True),
            ("2025-12-31 23:59:59", True),
            ("2026-01-01 00:00:00", False),
        ],
    )
    def test_limites_do_ano_de_2025(self, business, make_pedidos, data_criacao, incluido):
        pedidos = business.calculate_order_totals(
            make_pedidos([("p1", "TV", 1500, 1, data_criacao, "SP", 1)])
        )
        assert (business.filter_reference_year(pedidos).count() == 1) is incluido

    def test_ano_de_referencia_e_configuravel(self, make_pedidos):
        business_2024 = OrderBusiness(BusinessRulesConfig(reference_year=2024))
        pedidos = business_2024.calculate_order_totals(
            make_pedidos(
                [
                    ("p2024", "TV", 1500, 1, "2024-03-01 10:00:00", "SP", 1),
                    ("p2025", "TV", 1500, 1, "2025-03-01 10:00:00", "SP", 1),
                ]
            )
        )
        assert DadosDeTeste.ids(business_2024.filter_reference_year(pedidos)) == ["p2024"]


class TestJoinOrdersPayments:
    def test_inner_join_sem_multiplicar_o_valor_total(
        self, business, make_pedidos, make_pagamentos
    ):
        pedidos = business.calculate_order_totals(
            make_pedidos(
                [
                    ("p1", "TV", 1500, 2, "2025-03-10 10:00:00", "SP", 1),
                    ("p1", "CELULAR", 1000, 1, "2025-03-10 10:00:00", "SP", 1),
                    ("sem-pagamento", "TV", 1500, 1, "2025-03-10 10:00:00", "SP", 2),
                ]
            )
        )
        pagamentos = business.prepare_payments(
            make_pagamentos(
                [
                    ("p1", "PIX", 3800, False, "2025-03-10 11:00:00", False, 0.2),
                    ("sem-pedido", "PIX", 100, False, "2025-03-10 11:00:00", False, 0.2),
                ]
            )
        )
        resultado = business.join_orders_payments(pedidos, pagamentos).collect()
        assert [r.id_pedido for r in resultado] == ["p1"]
        assert resultado[0].valor_total_pedido == Decimal("4000.00")  # e não valor_pagamento


class TestProjecaoEOrdenacao:
    @pytest.fixture
    def relacionados(self, business, make_pedidos, make_pagamentos):
        pedidos = business.calculate_order_totals(
            make_pedidos(
                [
                    ("sp-pix-10h", "TV", 100, 1, "2025-03-10 10:00:00", "SP", 1),
                    ("rj-boleto", "TV", 100, 1, "2025-09-01 08:00:00", "RJ", 1),
                    ("sp-pix-09h", "TV", 100, 1, "2025-03-10 09:00:00", "SP", 1),
                    ("sp-cartao", "TV", 100, 1, "2025-12-01 08:00:00", "SP", 1),
                    ("ac-pix", "TV", 100, 1, "2025-12-31 23:00:00", "AC", 1),
                ]
            )
        )
        pagamentos = business.prepare_payments(
            make_pagamentos(
                [
                    ("sp-pix-10h", "PIX", 95, False, "2025-03-10 11:00:00", False, 0.1),
                    ("rj-boleto", "BOLETO", 95, False, "2025-09-01 09:00:00", False, 0.1),
                    ("sp-pix-09h", "PIX", 95, False, "2025-03-10 10:00:00", False, 0.1),
                    ("sp-cartao", "CARTAO_CREDITO", 100, False, "2025-12-01 09:00:00", False, 0.1),
                    ("ac-pix", "PIX", 95, False, "2025-12-31 23:30:00", False, 0.1),
                ]
            )
        )
        return business.join_orders_payments(pedidos, pagamentos)

    def test_projeta_exatamente_os_cinco_atributos(self, business, relacionados):
        relatorio = business.select_report_columns(relacionados)
        assert relatorio.columns == REPORT_COLUMNS
        assert DataSchemas.same_structure(relatorio.schema, DataSchemas.RELATORIO)

    def test_ordena_por_uf_forma_pagamento_e_data(self, business, relacionados):
        relatorio = business.sort_report(business.select_report_columns(relacionados))
        assert DadosDeTeste.ids(relatorio) == [
            "ac-pix",
            "rj-boleto",
            "sp-cartao",
            "sp-pix-09h",
            "sp-pix-10h",
        ]


class TestValidateReport:
    SCHEMA = DataSchemas.RELATORIO

    def _relatorio(self, spark, linhas):
        return spark.createDataFrame(linhas, self.SCHEMA)

    def test_relatorio_consistente_passa(self, spark, business):
        df = self._relatorio(spark, [("p1", "SP", "PIX", Decimal("10.00"), datetime(2025, 1, 1))])
        business.validate_report(df)

    @pytest.mark.parametrize(
        "linhas, motivo",
        [
            (
                [
                    ("p1", "SP", "PIX", Decimal("10.00"), datetime(2025, 1, 1)),
                    ("p1", "SP", "PIX", Decimal("10.00"), datetime(2025, 1, 1)),
                ],
                "duplicados",
            ),
            ([("p1", "SP", "PIX", Decimal("10.00"), datetime(2024, 12, 31))], "fora de 2025"),
            ([("p1", None, "PIX", Decimal("10.00"), datetime(2025, 1, 1))], "nulos"),
        ],
    )
    def test_relatorio_inconsistente_falha(self, spark, business, linhas, motivo):
        with pytest.raises(DataQualityException, match=motivo):
            business.validate_report(self._relatorio(spark, linhas))

    def test_schema_divergente_falha(self, spark, business):
        schema = StructType(
            [StructField("id_pedido", StringType()), StructField("x", TimestampType())]
        )
        with pytest.raises(DataQualityException, match="Schema"):
            business.validate_report(spark.createDataFrame([("p1", datetime(2025, 1, 1))], schema))


class TestBuildReport:
    @pytest.fixture
    def pedidos(self, make_pedidos):
        return make_pedidos(
            [
                ("p1", "TV", 1500, 2, "2025-03-10 10:00:00", "SP", 1),  # 2 itens -> 4000.00
                ("p1", "CELULAR", 1000, 1, "2025-03-10 10:00:00", "SP", 1),
                ("p2", "LIVRO", 50.5, 3, "2025-01-01 00:00:00", "RJ", 2),  # 1º instante de 2025
                ("p3", "MONITOR", 600, 1, "2025-12-31 23:59:59", "SP", 3),  # último instante
                ("p4", "TV", 1500, 1, "2025-06-01 10:00:00", "SP", 4),  # pagamento aprovado
                ("p5", "TV", 1500, 1, "2025-06-02 10:00:00", "MG", 5),  # fraude
                ("p6", "TV", 1500, 1, "2024-12-31 23:59:59", "SP", 6),  # 2024
                ("p7", "TV", 1500, 1, "2026-01-01 00:00:00", "SP", 7),  # 2026
                ("p8", "TV", 1500, 1, "2025-06-03 10:00:00", "BA", 8),  # sem pagamento
                ("p10", "LIQUIDIFICADOR", 300, 1, "2025-03-10 09:00:00", "SP", 10),
            ]
        )

    @pytest.fixture
    def pagamentos(self, make_pagamentos):
        return make_pagamentos(
            [
                ("p1", "PIX", 3800, False, "2025-03-10 11:00:00", False, 0.2),
                ("p2", "BOLETO", 143.92, False, "2025-01-01 01:00:00", False, 0.3),
                ("p3", "CARTAO_CREDITO", 600, False, "2026-01-01 00:10:00", False, 0.1),
                ("p4", "PIX", 1425, True, "2025-06-01 11:00:00", False, 0.1),
                ("p5", "PIX", 1425, False, "2025-06-02 11:00:00", True, 0.97),
                ("p6", "PIX", 1425, False, "2025-01-01 01:00:00", False, 0.2),
                ("p7", "PIX", 1425, False, "2026-01-01 01:00:00", False, 0.2),
                ("p9", "PIX", 1425, False, "2025-06-04 11:00:00", False, 0.2),  # sem pedido
                ("p10", "PIX", 285, False, "2025-03-10 09:30:00", False, 0.4),
            ]
        )

    def test_relatorio_final_completo_e_ordenado(self, business, pedidos, pagamentos):
        relatorio = business.build_report(pedidos, pagamentos).report
        assert relatorio.columns == REPORT_COLUMNS
        assert [tuple(r) for r in relatorio.collect()] == [
            ("p2", "RJ", "BOLETO", Decimal("151.50"), datetime(2025, 1, 1, 0, 0, 0)),
            ("p3", "SP", "CARTAO_CREDITO", Decimal("600.00"), datetime(2025, 12, 31, 23, 59, 59)),
            ("p10", "SP", "PIX", Decimal("300.00"), datetime(2025, 3, 10, 9, 0, 0)),
            ("p1", "SP", "PIX", Decimal("4000.00"), datetime(2025, 3, 10, 10, 0, 0)),
        ]
        business.validate_report(relatorio)

    def test_registra_as_etapas_no_log(self, business, pedidos, pagamentos, caplog):
        with caplog.at_level(logging.INFO):
            business.build_report(pedidos, pagamentos)
        mensagens = " | ".join(caplog.messages)
        for etapa in ("recusados", "legítimas", "Relacionando", "Ordenando"):
            assert etapa in mensagens

    def test_erro_do_spark_vira_excecao_de_negocio_e_e_logado(
        self, spark, business, pedidos, caplog
    ):
        sem_avaliacao = spark.createDataFrame(
            [("p1", "PIX")],
            StructType(
                [
                    StructField("id_pedido", StringType()),
                    StructField("forma_pagamento", StringType()),
                ]
            ),
        )
        with caplog.at_level(logging.ERROR), pytest.raises(BusinessRuleException) as erro:
            business.build_report(pedidos, sem_avaliacao)
        assert erro.value.__cause__ is not None  # exceção original preservada (raise ... from)
        assert any(r.exc_info for r in caplog.records if r.levelno == logging.ERROR)


class TestRegistroDeDescartes:
    def test_registra_em_warning_os_descartes_e_duplicatas(self, business, make_pedidos, caplog):
        linha = ("p1", "TV", 1500, 1, "2025-01-10 10:00:00", "SP", 1)
        pedidos = make_pedidos(
            [linha, linha, ("p2", "TV", None, 1, "2025-01-10 10:00:00", "SP", 1)]
        )
        with caplog.at_level(logging.WARNING):
            business.prepare_orders(pedidos)
        avisos = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
        assert "1 registros de pedidos descartados por atributos nulos/inválidos" in avisos
        assert "1 linhas duplicadas de pedidos removidas" in avisos


class TestTratamentoDeErrosDaClasseDeNegocio:
    """Cobre os blocos try/except da OrderBusiness com falhas reais do Spark."""

    def test_multiplos_pagamentos_elegiveis_interrompem_o_relatorio(
        self, business, make_pedidos, make_pagamentos, caplog
    ):
        pedidos = make_pedidos([("p1", "TV", 1500, 1, "2025-03-10 10:00:00", "SP", 1)])
        pagamentos = make_pagamentos(
            [
                ("p1", "PIX", 1425, False, "2025-03-10 11:00:00", False, 0.1),
                ("p1", "BOLETO", 1425, False, "2025-03-11 11:00:00", False, 0.2),
            ]
        )
        with caplog.at_level(logging.ERROR), pytest.raises(DataQualityException):
            business.build_report(pedidos, pagamentos)
        assert any("interrompidas" in r.getMessage() for r in caplog.records)

    def test_falha_na_validacao_vira_excecao_de_negocio_logada(self, spark, business, caplog):
        linhas = [("p1", "SP", "PIX", Decimal("1.00"), datetime(2025, 1, 1))]
        relatorio = spark.createDataFrame(linhas, DataSchemas.RELATORIO).withColumn(
            "uf", F.raise_error(F.lit("falha simulada")).cast("string")
        )
        with caplog.at_level(logging.ERROR), pytest.raises(BusinessRuleException) as erro:
            business.validate_report(relatorio)
        assert "falha simulada" in str(erro.value.__cause__)
        assert any(r.exc_info for r in caplog.records if r.levelno == logging.ERROR)

    def test_falha_na_preparacao_vira_excecao_de_negocio_logada(
        self, business, make_pedidos, caplog
    ):
        pedidos = make_pedidos([("p1", "TV", 1500, 1, "2025-01-10 10:00:00", "SP", 1)]).withColumn(
            "uf", F.raise_error(F.lit("falha simulada")).cast("string")
        )
        with caplog.at_level(logging.ERROR), pytest.raises(BusinessRuleException) as erro:
            business.prepare_orders(pedidos)
        assert "falha simulada" in str(erro.value.__cause__)
        assert any(r.exc_info for r in caplog.records if r.levelno == logging.ERROR)
