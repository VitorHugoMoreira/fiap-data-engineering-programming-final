# Relatório de Pedidos Recusados e Legítimos — PySpark

> **Trabalho Final — Data Engineering Programming (FIAP)**
> Repositório: https://github.com/VitorHugoMoreira/fiap-data-engineering-programming-final

Aplicação PySpark orientada a objetos que gera, em **Parquet**, o relatório de pedidos de **2025** cujo pagamento foi **recusado** (`status = false`) e cuja avaliação de fraude os classificou como **legítimos** (`fraude = false`).

---

## Início rápido

```bash
git clone https://github.com/VitorHugoMoreira/fiap-data-engineering-programming-final.git
cd fiap-data-engineering-programming-final
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
make data          # baixa os datasets oficiais (git clone) em data/input/
pytest             # 67 testes
python src/main.py # gera data/output/relatorio_pedidos_recusados_legitimos_2025/
```

---

## 1. Contextualização acadêmica

| Item | Descrição |
|---|---|
| Instituição | FIAP — Pós-graduação em Engenharia de Dados |
| Disciplina | Data Engineering Programming |
| Professor | Marcelo Barbosa Pinto |
| Material de apoio | https://github.com/infobarbosa/pyspark-poo |
| Integrantes | *Vitor Hugo Moreira Luiz da Silva — RM374115*  *Vinicius Lima Baldin - RM374277*  *Felippe Machoski de Souza - RM374591* |

O projeto segue a evolução apresentada em aula (passos 1 a 14 do `pyspark-poo`): schemas explícitos, configuração externa em YAML, `SparkSessionManager`, pacote de I/O, lógica de negócio isolada, injeção de dependências com `main.py` como *Aggregation Root*, logging, tratamento de erros com exceções de domínio, empacotamento (`pyproject.toml`) e testes com `pytest`.

## 2. Objetivo

Entregar uma aplicação PySpark **executável, testada e agnóstica à plataforma** que resolva o problema de negócio e demonstre boas práticas de engenharia de software aplicadas à engenharia de dados.

## 3. Problema de negócio

A alta gestão deseja um relatório dos pedidos de venda cujo pagamento foi **recusado** mas que, na avaliação de fraude, foram considerados **legítimos**. São vendas perdidas sem justificativa de fraude, ou seja, oportunidades de recuperação de receita.

O relatório contém exatamente:

| # | Atributo | Coluna | Tipo |
|---|---|---|---|
| 1 | Identificador do pedido | `id_pedido` | `string` |
| 2 | Estado (UF) do pedido | `uf` | `string` |
| 3 | Forma de pagamento | `forma_pagamento` | `string` |
| 4 | Valor total do pedido | `valor_total_pedido` | `decimal(18,2)` |
| 5 | Data do pedido | `data_pedido` | `timestamp_ntz` |

## 4. Regras de negócio

| Regra | Implementação | Método |
|---|---|---|
| R1 — Integração | `inner join` entre pedidos e pagamentos por `id_pedido` | `join_orders_payments` |
| R2 — Pagamento recusado | `status == false` | `filter_refused_payments` |
| R3 — Fraude legítima | `avaliacao_fraude.fraude == false` (campo aninhado) | `filter_legitimate_payments` |
| R4 — Ano de referência | `year(data_criacao) == 2025` (01/01 a 31/12, inclusive); usa a data de **criação do pedido**, não a de processamento do pagamento | `filter_reference_year` |
| R5 — Valor total | `SUM(valor_unitario * quantidade)` agrupado por pedido; **não** usa `valor_pagamento` (PIX/Boleto têm 5% de desconto) | `calculate_order_totals` |
| R6 — Unicidade | 1 linha por pedido; se houver mais de um pagamento elegível para o mesmo pedido, o pipeline **falha com erro explicativo** (não escolhe arbitrariamente) | `ensure_single_payment_per_order`, `validate_report` |
| R7 — Ordenação | `uf ↑`, `forma_pagamento ↑`, `data_pedido ↑` (+ `id_pedido` como desempate técnico) | `sort_report` |
| R8 — Persistência | Parquet (snappy), 1 arquivo, releitura e validação pós-escrita | `DataHandler.write_parquet` |

Os parâmetros das regras (`reference_year`, `payment_status`, `fraud_flag`) ficam em `config/settings.yaml`.

## 5. Fontes de dados

| Dataset | Repositório oficial | Caminho utilizado |
|---|---|---|
| Pedidos | https://github.com/infobarbosa/datasets-csv-pedidos | `datasets-csv-pedidos/data/pedidos/` |
| Pagamentos | https://github.com/infobarbosa/dataset-json-pagamentos | `dataset-json-pagamentos/data/pagamentos/` |

Os datasets **não** são versionados neste repositório. Eles são baixados por `make data` (seção 20).

## 6. Descrição dos datasets

Perfil obtido na inspeção dos arquivos reais:

**Pedidos**: 25 arquivos `pedidos-AAAA-MM.csv.gz` (2024-01 a 2026-01), gzip, separador `;`, com cabeçalho em maiúsculas.

| Coluna | Exemplo | Tipo no schema |
|---|---|---|
| `id_pedido` | `09faf14c-3def-…` | `StringType` (UUID) |
| `produto` | `MONITOR` | `StringType` |
| `valor_unitario` | `600.0` | `DecimalType(10,2)` |
| `quantidade` | `1` | `LongType` |
| `data_criacao` | `2025-01-03T07:26:01` | `TimestampNTZType` |
| `uf` | `PE` | `StringType` |
| `id_cliente` | `100` | `LongType` |

- 60.096 linhas: 26.400 de 2024, **31.680 de 2025** e 2.016 de 2026.
- Nenhum nulo, nenhuma linha duplicada. Cada `id_pedido` aparece uma vez (1 item por pedido). Mesmo assim, a agregação por pedido é aplicada, como exige a regra.

**Pagamentos**: 25 arquivos `pagamentos-AAAA-MM.json.gz` (JSON Lines, gzip).

| Coluna | Exemplo | Tipo no schema |
|---|---|---|
| `id_pedido` | `09faf14c-3def-…` | `StringType` |
| `forma_pagamento` | `PIX`, `BOLETO`, `CARTAO_CREDITO` | `StringType` |
| `valor_pagamento` | `1045.0` | `DecimalType(10,2)` |
| `status` | `false` | `BooleanType` |
| `data_processamento` | `2025-01-18T03:11:27.267872` | `TimestampNTZType` |
| `avaliacao_fraude` | `{"fraude": false, "score": 0.23}` | `StructType(fraude: Boolean, score: Double)` |

- 60.096 registros, **1 pagamento por pedido** e relação 1:1 completa com os pedidos.
- Distribuição de `status`/`fraude`: (true, false) 51.751 · (false, true) 5.598 · **(false, false) 2.747**.

## 7. Arquitetura da solução

```text
                 ┌──────────────────────── src/main.py (Aggregation Root) ───────────────────────┐
                 │ ConfigLoader → AppConfig ─┬─► LoggingConfigurator                              │
                 │                           ├─► SparkSessionManager ─► SparkSession             │
                 │                           ├─► DataHandler(spark, pedidos, pagamentos, output) │
                 │                           ├─► OrderBusiness(business_rules)                   │
                 │                           └─► OrderPipeline(data_handler, business, pipeline) │
                 └───────────────────────────────────────────────────────────────────────────────┘
 CSV.gz ─┐                                                                                   
         ├─► DataHandler.read_* ─► OrderBusiness.build_report ─► validate ─► write_parquet ─► read_parquet ─► validate
 JSON.gz ┘        (I/O)                  (regras de negócio)                       (I/O)                 (verificação)
```

Princípios aplicados:

- **SRP:** cada classe tem uma única responsabilidade (configuração, sessão, I/O, negócio, orquestração).
- **DIP:** nenhuma classe cria as próprias dependências. Todas são recebidas pelo construtor e montadas apenas no `main.py`.
- **OCP:** parâmetros de negócio e de I/O são externos (YAML). Mudar ano, caminhos ou formatos não exige alterar código.

## 8. Estrutura de diretórios

```text
fiap-data-engineering-programming-final/
├── config/settings.yaml                 # configuração externa (Twelve-Factor)
├── src/
│   ├── main.py                          # Aggregation Root
│   └── data_engineering_pyspark/
│       ├── config/settings.py           # AppConfig, ConfigLoader, LoggingConfigurator
│       ├── session/spark_session.py     # SparkSessionManager
│       ├── schemas/data_schemas.py      # DataSchemas (StructType explícitos)
│       ├── io_utils/data_handler.py     # DataHandler (+ exceptions.py)
│       ├── business/order_business.py   # OrderBusiness (+ exceptions.py)
│       └── pipeline/order_pipeline.py   # OrderPipeline (+ exceptions.py)
├── tests/
│   ├── conftest.py                      # SparkSession de teste e fixtures
│   ├── helpers.py                       # classes utilitárias (DadosDeTeste, FabricaDataFrames)
│   ├── unit/                            # business, I/O, configuração, sessão
│   └── integration/                     # pipeline (mock e fluxo completo), main
├── docs/technical_documentation.md      # decisões técnicas e checklist de avaliação
├── data/input/   data/output/   logs/   # gerados em tempo de execução (fora do Git)
├── pyproject.toml  requirements.txt  MANIFEST.in  Makefile  README.md  .gitignore
```

## 9. Responsabilidade de cada classe

| Classe | Pacote | Responsabilidade |
|---|---|---|
| `AppConfig` (+ `SparkConfig`, `CsvSourceConfig`, `JsonSourceConfig`, `ParquetOutputConfig`, `BusinessRulesConfig`, `PipelineConfig`, `LoggingConfig`) | `config` | Modelo tipado, imutável e validado da configuração |
| `ConfigLoader` | `config` | Localiza e lê o `settings.yaml` (argumento → `PIPELINE_CONFIG` → `./settings.yaml` → `config/settings.yaml`) |
| `LoggingConfigurator` | `config` | Configura o logging com `logging.basicConfig` (console + arquivo) |
| `SparkSessionManager` | `session` | Cria (`SparkSession.builder`), disponibiliza e encerra a sessão Spark |
| `DataSchemas` | `schemas` | Catálogo dos `StructType` de pedidos, pagamentos e relatório |
| `DataHandler` | `io_utils` | Lê CSV/JSON com schema explícito, grava e relê Parquet. Traduz erros do Spark em exceções de domínio |
| `OrderBusiness` | `business` | Todas as transformações e regras de negócio, além da validação do resultado |
| `OrderPipeline` | `pipeline` | Orquestra as etapas, gerencia o cache, verifica a escrita e coleta métricas |
| `DataHandlerException`, `BusinessRuleException`, `DataQualityException`, `PipelineException` | `*/exceptions.py` | Exceções de domínio por camada |

## 10. Fluxo de execução

1. `main.py` carrega o `AppConfig` e configura o logging.
2. `SparkSessionManager` cria a sessão.
3. `main.py` instancia `DataHandler`, `OrderBusiness` e `OrderPipeline`, injetando as dependências.
4. `OrderPipeline.run()`:
   1. **Leitura:** `read_pedidos()` e `read_pagamentos()`, com cache das entradas.
   2. **Negócio:** `build_report()` prepara, agrega, filtra 2025, filtra recusados e legítimos, garante a unicidade, faz o join, projeta e ordena.
   3. **Validação:** `validate_report()` confere schema, unicidade, ausência de nulos e o ano.
   4. **Persistência:** `write_parquet()`.
   5. **Verificação:** `read_parquet()` com schema explícito, nova validação e conferência de contagem (calculado = gravado).
5. Ao final, as métricas da execução vão para o log, e o `finally` do `main.py` encerra a SparkSession, com ou sem erro.

## 11. Estratégia de injeção de dependências

O `src/main.py` é o **único** lugar que conhece as implementações concretas:

```python
config = ConfigLoader(args.config).load()                         # classe de configuração
spark_manager = SparkSessionManager(config.spark)                 # sessão Spark
spark = spark_manager.get_session()
data_handler = DataHandler(spark, config.pedidos, config.pagamentos, config.output)  # I/O
business = OrderBusiness(config.business)                         # lógica de negócio
pipeline = OrderPipeline(data_handler, business, config.pipeline) # orquestração
pipeline.run()
```

Nem a `OrderBusiness` nem o `OrderPipeline` criam SparkSession ou leem arquivos. Por isso os testes injetam um `MagicMock(spec=DataHandler)` no pipeline e DataFrames em memória na camada de negócio.

## 12. Schemas utilizados

Todos os DataFrames usam `StructType` explícito (`DataSchemas`). Não há `inferSchema` nem inferência de JSON em lugar nenhum, nem nos testes, que criam DataFrames com `spark.createDataFrame(dados, DataSchemas.X)`.

- **Dinheiro:** `DecimalType(10,2)` na entrada e `DecimalType(18,2)` no total, para evitar erros de ponto flutuante. Por exemplo, `0.1 × 3 = 0.30` exato, testado.
- **Datas:** `TimestampNTZType` (timestamp *sem* fuso), lido com formatos explícitos (`timestampNTZFormat` = `yyyy-MM-dd'T'HH:mm:ss` e `…ss.SSSSSS`). Os arquivos de origem não trazem offset. Com NTZ, o horário de parede é preservado: o filtro de ano e o Parquet gravado dão o mesmo resultado em qualquer fuso do sistema operacional ou da sessão Spark de quem lê. Com `TimestampType`, um pedido de `01/01/2025 00:00` apareceria como `31/12/2024 21:00` numa sessão em São Paulo. Validado com São Paulo, Tóquio e Honolulu.
- **Campo aninhado:** `avaliacao_fraude` é lido como `StructType(fraude: Boolean, score: Double)` e achatado em `fraude`/`score_fraude`.
- **Cabeçalho validado:** o CSV tem cabeçalho em MAIÚSCULAS e é lido com `enforceSchema=false`, que confere nome e posição de cada coluna contra o schema (sem diferenciar maiúsculas). Um arquivo com colunas fora de ordem falha com `CSV header does not conform to the schema`, em vez de trocar `id_pedido` por `produto` em silêncio, o que acontece com o padrão `enforceSchema=true` (testado).

## 13. Transformações PySpark

`trim`/`upper` (padronização), `filter` (validade, status, fraude, ano), `dropDuplicates` (linhas 100% idênticas), `groupBy().agg(sum(valor_unitario * quantidade))`, `join(how="inner")`, `select` dirigido pelo schema de saída, `orderBy` e `coalesce(1)`, que preserva a ordenação global no arquivo. Todo o processamento roda em transformações distribuídas do Spark. Não há Pandas nem `collect()` de datasets: só agregados e amostras de até 5 chaves em mensagens de erro chegam ao driver.

## 14. Estratégia de logging

- `LoggingConfigurator` (pacote `config`) chama `logging.basicConfig(level=…, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[console, arquivo])`, o mesmo formato do exemplo do enunciado, com parâmetros vindos do YAML. É executado **somente** no `main.py`.
- Cada módulo, inclusive `order_business.py`, faz `import logging` e `logger = logging.getLogger(__name__)`.
- São registrados: início do job e do pipeline, leitura de cada dataset, cada regra de negócio, cálculo, join, filtros, ordenação, escrita, verificação, contagens de cada etapa, conclusão com duração e erros.
- Destinos: console e `logs/pipeline.log`. O código não tem nenhum `print()`.

## 15. Tratamento de erros

| Camada | Captura | Ação |
|---|---|---|
| `DataHandler` | `AnalysisException` (ex.: `PATH_NOT_FOUND`), `PySparkException` | `logger.error` com a classe do erro e `raise DataReadException/DataWriteException(...) from exc` |
| `OrderBusiness` | `PySparkException` em `build_report`/`validate_report`/`_clean`; violações de dados | `logger.exception` e `raise BusinessRuleException(...) from exc`; `DataQualityException` para inconsistências |
| `OrderPipeline` | `try/finally` | Libera o cache das entradas mesmo quando uma leitura ou etapa falha. Divergência entre calculado e gravado gera `PipelineException` |
| `main.py` | Exceções de domínio, `PySparkException`, `Exception` | `logger.exception`, código de saída `1` (`2` para configuração inválida) e `spark_manager.stop()` no `finally` |

Exemplo validado (também coberto por teste em processo isolado): com o caminho de pedidos inválido, o log mostra `[classe=PATH_NOT_FOUND]`, a cadeia `AnalysisException → DataReadException`, `SparkSession encerrada.` e o processo termina com código `1`.

Falhas de *conteúdo*, como arquivo `.gz` corrompido ou cabeçalho fora de ordem, só aparecem quando o Spark executa uma ação (avaliação preguiçosa). Elas chegam ao `main.py` e são registradas pela última linha de defesa (`except Exception` com `logger.exception`, código `1`), exatamente como no "Teste de Arquivo Corrompido" do Passo 9 do material. O projeto não captura `Py4JJavaError`, seguindo a recomendação do material.

## 16. Testes automatizados

**67 testes** com `pytest`, todos passando, com **98% de cobertura** de linhas (`make coverage`):

| Arquivo | Qtde | Cobertura |
|---|---|---|
| `tests/unit/test_order_business.py` | 35 | Cálculo do total (parametrizado, Decimal exato), descarte de nulos/inválidos com WARNING, duplicatas, exclusão de aprovados e de fraudes, limites de 2025 (parametrizado), ano configurável, join sem multiplicar valores, projeção, ordenação, validação (duplicado, nulo, fora do ano, schema), múltiplos pagamentos elegíveis, cenário completo e os três blocos `try/except` com falhas reais do Spark → `BusinessRuleException` logada |
| `tests/unit/test_data_handler.py` | 9 | CSV/JSON gzip com schema explícito, campo aninhado, `pathGlobFilter`, cabeçalho fora de ordem, arquivo corrompido, dataset vazio (WARNING), caminho inexistente → `DataReadException`, falha de escrita/releitura → `DataWriteException`, ida e volta do Parquet |
| `tests/unit/test_settings.py` | 11 | YAML do projeto, variável de ambiente, arquivo/seção ausente, valores inválidos (parametrizado), resolução de caminhos pela raiz do projeto (URI e absoluto preservados), localização da configuração fora da raiz, logging em arquivo |
| `tests/unit/test_spark_session.py` | 3 | Reutilização da sessão, aplicação das opções da configuração e `stop()` idempotente |
| `tests/integration/test_order_pipeline.py` | 9 | Orquestração com mock, divergência calculado×gravado, liberação de cache em falhas, execução sem cache, fluxo completo `.gz` → Parquet e o **`src/main.py` real executado em processo isolado** a partir de outro diretório (sucesso: código 0, Parquet correto, sessão encerrada; falha: código 1) |

Os testes usam dados pequenos e controlados, schemas explícitos, SparkSession local compartilhada (`scope="session"`) e `tmp_path` para arquivos. Os utilitários de teste também são classes (`tests/helpers.py`). Para comprovar que os testes detectam defeitos reais, oito mutações foram plantadas na `OrderBusiness`, e todas fizeram a suíte falhar (ver `docs/technical_documentation.md`).

## 17. Tecnologias e dependências

| Tecnologia | Versão validada |
|---|---|
| Python | 3.10 e 3.13 (clone limpo + `make check` + execução validados nas duas) |
| Java (OpenJDK) | 21 (Spark 4.2 aceita 17 ou 21) |
| PySpark | 4.2.0 |
| PyYAML | 6.0.3 |
| pytest / pytest-cov | 8.4.1 / 6.0.0 |
| ruff / black / build | 0.12.9 / 25.1.0 / 1.3.0 |

## 18. Pré-requisitos

- Linux, macOS ou WSL (validado em Ubuntu 24.04; o professor usa Codespaces/Ubuntu 22.04)
- Python ≥ 3.10 com `venv`
- Java 17 ou 21 no `PATH` (`java -version`). Se faltar no Ubuntu: `sudo apt-get install -y openjdk-17-jre-headless`
- Git e GNU Make. Sem Make, use os comandos equivalentes indicados abaixo.

## 19. Instalação

```bash
git clone https://github.com/VitorHugoMoreira/fiap-data-engineering-programming-final.git
cd fiap-data-engineering-programming-final
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

O wheel (`make build`) contém apenas o pacote `data_engineering_pyspark`, sem módulos soltos no `site-packages`. O ponto de entrada continua sendo `src/main.py`.

## 20. Download dos datasets

```bash
make data
```

Equivalente sem Make:

```bash
git clone --depth 1 https://github.com/infobarbosa/datasets-csv-pedidos   data/input/datasets-csv-pedidos
git clone --depth 1 https://github.com/infobarbosa/dataset-json-pagamentos data/input/dataset-json-pagamentos
```

## 21. Configuração dos caminhos

Todos os caminhos ficam em `config/settings.yaml`. Caminhos relativos são resolvidos a partir da **raiz do projeto** (a pasta que contém `config/`), **qualquer que seja o diretório de execução**. Caminhos absolutos e URIs (`s3a://…`, `hdfs://…`) são mantidos como estão:

```yaml
sources:
  pedidos:    { path: "./data/input/datasets-csv-pedidos/data/pedidos/",    file_pattern: "*.csv.gz", sep: ";", header: true, enforce_schema: false }
  pagamentos: { path: "./data/input/dataset-json-pagamentos/data/pagamentos/", file_pattern: "*.json.gz" }
output:
  path: "./data/output/relatorio_pedidos_recusados_legitimos_2025"
```

Outro arquivo pode ser usado com `python src/main.py --config caminho/settings.yaml` ou com `export PIPELINE_CONFIG=caminho/settings.yaml`.

## 22. Como executar a aplicação

Com o `.venv` ativo, de qualquer diretório (validado a partir da raiz, de `src/` e de `/tmp`):

```bash
python src/main.py
```

Alternativas validadas:

```bash
make run                                   # python src/main.py
make submit                                # spark-submit --master "local[*]" src/main.py
make build && spark-submit --master "local[*]" \
  --py-files dist/data_engineering_pyspark-1.0.0-py3-none-any.whl \
  src/main.py                              # pacote distribuído via --py-files (Passo 12)
```

O YAML usa `master: null`, então o `--master` informado no `spark-submit` é respeitado (validado com `local[2]` e `local[3]`) e o job pode ser submetido a um cluster sem alterar código. Sem `--master`, o Spark usa o padrão `local[*]`.

Saída esperada no fim do log (tempo de cerca de 20 s):

```text
... - INFO - Resumo da execução:
... - INFO -   pedidos_lidos                       60096
... - INFO -   pagamentos_lidos                    60096
... - INFO -   pedidos_apos_agregacao              60096
... - INFO -   pedidos_no_ano_referencia           31680
... - INFO -   pagamentos_recusados                 8345
... - INFO -   pagamentos_recusados_legitimos       2747
... - INFO -   pedidos_elegiveis                    1473
... - INFO -   registros_gravados                   1473
... - INFO - ===== Pipeline concluído com sucesso em 18.8s =====
```

## 23. Como executar os testes

```bash
pytest                       # suíte completa (67 testes, ~1,5 min)
pytest tests/unit            # apenas unitários
pytest -m integration        # apenas integração
make check                   # black --check + ruff + pytest
make coverage                # pytest + relatório de cobertura
```

## 24. Localização do resultado Parquet

```text
data/output/relatorio_pedidos_recusados_legitimos_2025/
├── _SUCCESS
└── part-00000-<uuid>-c000.snappy.parquet     # 1.473 linhas, ordenadas
```

Leitura posterior pelo Spark, também com schema explícito:

```python
from data_engineering_pyspark.schemas.data_schemas import DataSchemas

spark.read.schema(DataSchemas.RELATORIO).parquet("data/output/relatorio_pedidos_recusados_legitimos_2025").show()
```

O arquivo também é legível por ferramentas externas baseadas em pyarrow, como o `parquet-tools` usado em aula: `data_pedido` aparece como `timestamp[us]` sem fuso e `valor_total_pedido` como `decimal128(18, 2)`.

## 25. Exemplo de saída

```text
+------------------------------------+---+---------------+------------------+-------------------+
|id_pedido                           |uf |forma_pagamento|valor_total_pedido|data_pedido        |
+------------------------------------+---+---------------+------------------+-------------------+
|acbf9b42-eaca-4cd9-914a-109de59aa943|AL |BOLETO         |2500.00           |2025-02-02 17:02:14|
|df280f1a-b8f4-4a29-be64-66fb7fab2a25|AL |BOLETO         |1100.00           |2025-06-12 12:29:22|
|394c5640-fae2-413f-bdfb-52f316fa6782|AL |CARTAO_CREDITO |5000.00           |2025-01-03 18:49:13|
|4440113d-717d-4887-8889-574853e47e20|AL |CARTAO_CREDITO |1100.00           |2025-03-06 06:33:11|
|cdbf1791-ad2f-4357-af93-1af7e9545214|AL |CARTAO_CREDITO |600.00            |2025-03-15 13:24:44|
+------------------------------------+---+---------------+------------------+-------------------+
root
 |-- id_pedido: string
 |-- uf: string
 |-- forma_pagamento: string
 |-- valor_total_pedido: decimal(18,2)
 |-- data_pedido: timestamp_ntz
```

Totais do resultado: **1.473 pedidos** (CARTAO_CREDITO 950 · PIX 442 · BOLETO 81), 20 UFs, soma de `valor_total_pedido` = **R$ 1.994.160,00**.

## 26. Decisões e premissas

- **`data_pedido` como timestamp sem fuso (`TIMESTAMP_NTZ`):** a origem (`data_criacao`) tem data e hora, mas nenhum fuso. Manter data e hora preserva a informação e torna exata a ordenação por "data de criação do pedido". O tipo NTZ garante que o valor seja exibido igual em qualquer máquina.
- **Desempate por `id_pedido`:** é o 4º critério de ordenação e só garante determinismo. Nos dados atuais não há empates em (uf, forma_pagamento, data_pedido).
- **Linhas 100% idênticas** são tratadas como duplicidade de ingestão e removidas, com WARNING no log. Registros com campos obrigatórios nulos ou valores negativos são descartados, também com WARNING. Nos dados oficiais, ambas as contagens são zero.
- **`mode: PERMISSIVE`** na leitura, para que um registro malformado não derrube o relatório. Ele vira nulo e é descartado com log. Para falhar no primeiro registro malformado, troque para `FAILFAST` no YAML.
- **`master: null`:** o projeto não fixa onde roda. O `spark-submit` (ou o padrão `local[*]`) decide.
- **Caminhos resolvidos pela raiz do projeto:** o professor pode executar `python src/main.py` de qualquer pasta.

## 27. Considerações finais

O projeto cumpre todos os critérios gerais e específicos do enunciado. A matriz de conformidade, com arquivo, evidência e prova de execução de cada item, e o relatório da auditoria técnica (não conformidades encontradas e corrigidas) estão em [`docs/technical_documentation.md`](docs/technical_documentation.md). O resultado foi auditado contra um cálculo independente em Python puro, com 12/12 verificações OK: contagem, conteúdo, unicidade, ano, status, fraude, valores e ordenação física.
