"""Classes de configuração: modelo tipado, carregamento do YAML e configuração do logging."""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

_READ_MODES = {"PERMISSIVE", "DROPMALFORMED", "FAILFAST"}
_WRITE_MODES = {"overwrite", "append", "error", "errorifexists", "ignore"}


class ConfigurationError(Exception):
    """Lançada quando o arquivo de configuração está ausente ou inválido."""


@dataclass(frozen=True)
class SparkConfig:
    """Parâmetros de criação da SparkSession."""

    app_name: str
    master: str | None = None  # None: respeita --master do spark-submit (padrão local[*])
    log_level: str = "WARN"
    options: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class CsvSourceConfig:
    """Opções de leitura do dataset de pedidos (CSV compactado)."""

    path: str
    file_pattern: str = "*.csv.gz"
    compression: str = "gzip"
    header: bool = True
    enforce_schema: bool = False  # False: valida o cabeçalho contra o schema (posição/nome)
    sep: str = ";"
    encoding: str = "UTF-8"
    timestamp_format: str = "yyyy-MM-dd'T'HH:mm:ss"
    mode: str = "PERMISSIVE"


@dataclass(frozen=True)
class JsonSourceConfig:
    """Opções de leitura do dataset de pagamentos (JSON Lines compactado)."""

    path: str
    file_pattern: str = "*.json.gz"
    compression: str = "gzip"
    timestamp_format: str = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"
    mode: str = "PERMISSIVE"


@dataclass(frozen=True)
class ParquetOutputConfig:
    """Opções de escrita do relatório em Parquet."""

    path: str
    mode: str = "overwrite"
    compression: str = "snappy"
    num_files: int = 1


@dataclass(frozen=True)
class BusinessRulesConfig:
    """Parâmetros das regras de negócio do relatório."""

    reference_year: int = 2025
    payment_status: bool = False
    fraud_flag: bool = False


@dataclass(frozen=True)
class PipelineConfig:
    """Parâmetros de execução da orquestração."""

    cache_inputs: bool = True


@dataclass(frozen=True)
class LoggingConfig:
    """Parâmetros do logging (formato compatível com logging.basicConfig)."""

    level: str = "INFO"
    format: str = "%(asctime)s - %(levelname)s - %(message)s"
    datefmt: str = "%Y-%m-%d %H:%M:%S"
    file: str | None = None


@dataclass(frozen=True)
class AppConfig:
    """Configuração completa e imutável da aplicação."""

    environment: str
    spark: SparkConfig
    pedidos: CsvSourceConfig
    pagamentos: JsonSourceConfig
    output: ParquetOutputConfig
    business: BusinessRulesConfig
    pipeline: PipelineConfig
    logging: LoggingConfig

    @classmethod
    def from_dict(cls, raw: dict[str, Any], base_dir: Path | None = None) -> AppConfig:
        """Constrói e valida a configuração a partir do dicionário lido do YAML.

        Caminhos relativos são resolvidos a partir de ``base_dir`` (raiz do projeto), o que
        torna a execução independente do diretório corrente. URIs (``s3a://``) são mantidas.
        """
        try:
            sources = raw["sources"]
            config = cls(
                environment=str(raw.get("environment", "local")),
                spark=SparkConfig(
                    app_name=raw["spark"]["app_name"],
                    master=raw["spark"].get("master"),
                    log_level=raw["spark"].get("log_level", "WARN"),
                    options={k: str(v) for k, v in (raw["spark"].get("options") or {}).items()},
                ),
                pedidos=CsvSourceConfig(**cls._section(sources["pedidos"], base_dir, "path")),
                pagamentos=JsonSourceConfig(
                    **cls._section(sources["pagamentos"], base_dir, "path")
                ),
                output=ParquetOutputConfig(**cls._section(raw["output"], base_dir, "path")),
                business=BusinessRulesConfig(**(raw.get("business") or {})),
                pipeline=PipelineConfig(**(raw.get("pipeline") or {})),
                logging=LoggingConfig(**cls._section(raw.get("logging") or {}, base_dir, "file")),
            )
        except (KeyError, TypeError) as exc:
            raise ConfigurationError(f"Configuração inválida ou incompleta: {exc}") from exc
        config.validate()
        return config

    def validate(self) -> None:
        """Valida regras semânticas que o YAML não consegue expressar."""
        problems: list[str] = []
        if self.pedidos.mode not in _READ_MODES or self.pagamentos.mode not in _READ_MODES:
            problems.append(f"mode de leitura deve ser um de {sorted(_READ_MODES)}")
        if self.output.mode not in _WRITE_MODES:
            problems.append(f"output.mode deve ser um de {sorted(_WRITE_MODES)}")
        if self.output.num_files < 1:
            problems.append("output.num_files deve ser >= 1")
        if not isinstance(self.business.reference_year, int):
            problems.append("business.reference_year deve ser inteiro")
        if problems:
            raise ConfigurationError("; ".join(problems))

    @staticmethod
    def _section(section: dict[str, Any], base_dir: Path | None, path_key: str) -> dict[str, Any]:
        """Copia a seção sem chaves descritivas ('format') e resolve o caminho relativo."""
        values = {k: v for k, v in section.items() if k != "format"}
        value = values.get(path_key)
        if base_dir is not None and value and "://" not in value and not Path(value).is_absolute():
            values[path_key] = str((base_dir / value).resolve())
        return values


class ConfigLoader:
    """Localiza e carrega o arquivo YAML de configuração.

    Ordem de resolução do caminho:
    1. caminho explícito (argumento ``--config``);
    2. variável de ambiente ``PIPELINE_CONFIG``;
    3. ``settings.yaml`` no diretório de execução (``spark-submit --files``);
    4. ``config/settings.yaml`` no diretório de execução;
    5. ``config/settings.yaml`` na raiz do projeto informada pelo ``main.py``.
    """

    ENV_VAR = "PIPELINE_CONFIG"

    def __init__(self, path: str | None = None, project_root: Path | None = None) -> None:
        self._explicit_path = path
        self._project_root = project_root

    def candidates(self) -> list[Path]:
        """Lista, em ordem de prioridade, os caminhos de configuração possíveis."""
        explicit = self._explicit_path or os.environ.get(self.ENV_VAR)
        if explicit:
            return [Path(explicit)]
        paths = [Path("settings.yaml"), Path("config") / "settings.yaml"]
        if self._project_root is not None:
            paths.append(self._project_root / "config" / "settings.yaml")
        return paths

    def resolve_path(self) -> Path:
        """Retorna o primeiro candidato existente (ou o último, para a mensagem de erro)."""
        paths = self.candidates()
        return next((path for path in paths if path.is_file()), paths[-1])

    def load(self) -> AppConfig:
        """Lê o YAML e devolve a configuração tipada, validada e com caminhos resolvidos."""
        path = self.resolve_path().resolve()
        if not path.is_file():
            raise ConfigurationError(
                f"Arquivo de configuração não encontrado: '{path}'. "
                "Informe --config ou a variável PIPELINE_CONFIG."
            )
        with path.open("r", encoding="utf-8") as file:
            raw = yaml.safe_load(file) or {}
        # <raiz>/config/settings.yaml -> caminhos relativos à <raiz>; senão, à pasta do arquivo
        base_dir = path.parent.parent if path.parent.name == "config" else path.parent
        logger.info("Configuração carregada de '%s' (base dos caminhos: '%s')", path, base_dir)
        return AppConfig.from_dict(raw, base_dir=base_dir)


class LoggingConfigurator:
    """Configura o logging da aplicação via ``logging.basicConfig`` (console + arquivo)."""

    def __init__(self, config: LoggingConfig) -> None:
        self._config = config

    def configure(self) -> None:
        """Aplica a configuração ao logger raiz; deve ser chamado apenas no main.py."""
        handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
        if self._config.file:
            log_path = Path(self._config.file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            handlers.append(logging.FileHandler(log_path, encoding="utf-8"))
        logging.basicConfig(
            level=getattr(logging, self._config.level.upper(), logging.INFO),
            format=self._config.format,
            datefmt=self._config.datefmt,
            handlers=handlers,
            force=True,
        )
        logging.getLogger("py4j").setLevel(logging.WARNING)
