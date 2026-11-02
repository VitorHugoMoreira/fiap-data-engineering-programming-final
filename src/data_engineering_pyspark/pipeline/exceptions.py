"""Exceções da camada de orquestração."""


class PipelineException(Exception):
    """Falha de orquestração (ex.: divergência entre o calculado e o gravado)."""
