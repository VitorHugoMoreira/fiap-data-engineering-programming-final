"""Exceções de domínio da camada de negócio."""


class BusinessRuleException(Exception):
    """Exceção base: falha ao aplicar as regras de negócio do relatório."""


class DataQualityException(BusinessRuleException):
    """Os dados violam uma premissa de consistência (ex.: pedido duplicado no resultado)."""
