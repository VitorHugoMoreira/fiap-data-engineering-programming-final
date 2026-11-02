"""Testes das classes de configuração (não dependem de Spark)."""

from __future__ import annotations

import logging

import pytest

from data_engineering_pyspark.config.settings import (
    AppConfig,
    ConfigLoader,
    ConfigurationError,
    LoggingConfig,
    LoggingConfigurator,
)
from tests.helpers import PROJECT_ROOT, DadosDeTeste

pytestmark = pytest.mark.unit

PROJECT_CONFIG = DadosDeTeste.PROJECT_CONFIG


@pytest.fixture
def raw_config() -> dict:
    return DadosDeTeste.project_config()


class TestConfigLoader:
    def test_carrega_o_settings_do_projeto(self):
        config = ConfigLoader(str(PROJECT_CONFIG)).load()
        assert isinstance(config, AppConfig)
        assert config.pedidos.sep == ";" and config.pedidos.header is True
        assert config.business.reference_year == 2025
        assert config.business.payment_status is False and config.business.fraud_flag is False
        assert config.spark.options["spark.sql.session.timeZone"] == "UTC"
        assert config.spark.master is None  # não sobrescreve o --master do spark-submit
        assert config.pedidos.enforce_schema is False

    def test_variavel_de_ambiente_define_o_arquivo(self, tmp_path, raw_config, monkeypatch):
        raw_config["business"]["reference_year"] = 2024
        monkeypatch.setenv(
            ConfigLoader.ENV_VAR, DadosDeTeste.write_yaml(tmp_path / "settings.yaml", raw_config)
        )
        assert ConfigLoader().load().business.reference_year == 2024

    def test_arquivo_inexistente_falha_de_forma_explicita(self, tmp_path):
        with pytest.raises(ConfigurationError, match="não encontrado"):
            ConfigLoader(str(tmp_path / "nao_existe.yaml")).load()

    def test_secao_obrigatoria_ausente(self, tmp_path, raw_config):
        del raw_config["sources"]
        with pytest.raises(ConfigurationError, match="sources"):
            ConfigLoader(DadosDeTeste.write_yaml(tmp_path / "settings.yaml", raw_config)).load()

    def test_modo_de_leitura_invalido(self, tmp_path, raw_config):
        raw_config["sources"]["pedidos"]["mode"] = "QUALQUER"
        with pytest.raises(ConfigurationError, match="mode"):
            ConfigLoader(DadosDeTeste.write_yaml(tmp_path / "settings.yaml", raw_config)).load()

    @pytest.mark.parametrize(
        "secao, chave, valor, mensagem",
        [
            ("output", "mode", "sobrescrever", "output.mode"),
            ("output", "num_files", 0, "num_files"),
            ("business", "reference_year", "2025", "reference_year"),
        ],
    )
    def test_valores_semanticamente_invalidos(
        self, tmp_path, raw_config, secao, chave, valor, mensagem
    ):
        raw_config[secao][chave] = valor
        with pytest.raises(ConfigurationError, match=mensagem):
            ConfigLoader(DadosDeTeste.write_yaml(tmp_path / "settings.yaml", raw_config)).load()


class TestResolucaoDeCaminhos:
    def test_relativos_resolvidos_pela_raiz_que_contem_config(self, tmp_path, raw_config):
        raw_config["output"]["path"] = "s3a://bucket/relatorio"
        raw_config["sources"]["pagamentos"]["path"] = "/dados/absolutos/pagamentos"
        arquivo = DadosDeTeste.write_yaml(
            tmp_path / "projeto" / "config" / "settings.yaml", raw_config
        )
        config = ConfigLoader(arquivo).load()
        raiz = (tmp_path / "projeto").resolve()
        assert config.pedidos.path == str(raiz / "data/input/datasets-csv-pedidos/data/pedidos")
        assert config.logging.file == str(raiz / "logs" / "pipeline.log")
        assert config.output.path == "s3a://bucket/relatorio"  # URI mantida
        assert config.pagamentos.path == "/dados/absolutos/pagamentos"  # absoluto mantido

    def test_encontra_a_configuracao_fora_da_raiz_do_projeto(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)  # diretório sem config/
        monkeypatch.delenv(ConfigLoader.ENV_VAR, raising=False)
        config = ConfigLoader(project_root=PROJECT_ROOT).load()
        assert config.pedidos.path.startswith(str(PROJECT_ROOT))


class TestLoggingConfigurator:
    @pytest.fixture(autouse=True)
    def _restaura_logging(self):
        root = logging.getLogger()
        handlers, level = root.handlers[:], root.level
        yield
        for handler in root.handlers:
            if handler not in handlers:
                handler.close()
        root.handlers[:] = handlers
        root.setLevel(level)

    def test_configura_console_e_arquivo(self, tmp_path):
        log_file = tmp_path / "logs" / "pipeline.log"
        LoggingConfigurator(LoggingConfig(level="INFO", file=str(log_file))).configure()
        logging.getLogger("teste").info("mensagem de teste")
        for handler in logging.getLogger().handlers:
            handler.flush()
        assert "INFO - mensagem de teste" in log_file.read_text(encoding="utf-8")
