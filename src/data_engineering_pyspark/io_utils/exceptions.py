"""Exceções de domínio da camada de I/O (desacoplam o chamador das exceções do Spark)."""


class DataHandlerException(Exception):
    """Exceção base para qualquer falha na camada de I/O."""


class DataReadException(DataHandlerException):
    """Falha ao localizar ou ler um dataset de entrada."""


class DataWriteException(DataHandlerException):
    """Falha ao gravar ou reler o dataset de saída."""
