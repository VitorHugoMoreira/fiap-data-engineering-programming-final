.PHONY: help install data test coverage lint format run submit build clean check

PYTHON := python3
PIP := pip
INPUT_DIR := data/input

help:
	@echo "Comandos disponíveis:"
	@echo "  make install  - Instala as dependências (requirements.txt)"
	@echo "  make data     - Baixa os datasets oficiais (git clone) em $(INPUT_DIR)"
	@echo "  make test     - Executa a suíte de testes (pytest)"
	@echo "  make coverage - Testes com relatório de cobertura (pytest-cov)"
	@echo "  make lint     - Verifica formatação (black) e qualidade (ruff)"
	@echo "  make format   - Formata o código com black"
	@echo "  make run      - Executa o pipeline (python src/main.py)"
	@echo "  make submit   - Executa o pipeline via spark-submit"
	@echo "  make build    - Gera o pacote distribuível (.whl e .tar.gz)"
	@echo "  make clean    - Remove artefatos, caches e saídas"
	@echo "  make check    - Quality gate (lint + test)"

install:
	$(PIP) install -r requirements.txt

data:
	@test -d $(INPUT_DIR)/datasets-csv-pedidos || git clone --depth 1 https://github.com/infobarbosa/datasets-csv-pedidos $(INPUT_DIR)/datasets-csv-pedidos
	@test -d $(INPUT_DIR)/dataset-json-pagamentos || git clone --depth 1 https://github.com/infobarbosa/dataset-json-pagamentos $(INPUT_DIR)/dataset-json-pagamentos
	@ls $(INPUT_DIR)/datasets-csv-pedidos/data/pedidos | head -3
	@ls $(INPUT_DIR)/dataset-json-pagamentos/data/pagamentos | head -3

test:
	pytest

coverage:
	pytest --cov=data_engineering_pyspark --cov=main --cov-report=term-missing

lint:
	black --check src tests
	ruff check src tests

format:
	black src tests

run:
	$(PYTHON) src/main.py

submit:
	spark-submit --master "local[*]" src/main.py

build: clean
	$(PYTHON) -m build

clean:
	rm -rf dist build src/*.egg-info .pytest_cache .ruff_cache htmlcov .coverage
	find . -path ./.venv -prune -o -type d -name "__pycache__" -exec rm -rf {} +
	rm -rf data/output/*/

check: lint test
