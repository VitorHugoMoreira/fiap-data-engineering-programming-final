# Documentação Técnica — Relatório de Pedidos Recusados e Legítimos

Complementa o [`README.md`](../README.md) com as decisões de engenharia, a validação end-to-end e a auditoria dos critérios do enunciado.

---

## 1. Discovery dos dados (inspeção dos arquivos oficiais)

| Verificação | Pedidos (CSV) | Pagamentos (JSON) |
|---|---|---|
| Arquivos | 25 × `pedidos-AAAA-MM.csv.gz` | 25 × `pagamentos-AAAA-MM.json.gz` |
| Formato | gzip, `;`, cabeçalho em MAIÚSCULAS | gzip, JSON Lines |
| Registros | 60.096 | 60.096 |
| `id_pedido` distintos | 60.096 (1 linha por pedido) | 60.096 (1 pagamento por pedido) |
| Nulos / linhas duplicadas | 0 / 0 | 0 / 0 |
| Datas | `yyyy-MM-ddTHH:mm:ss` | `yyyy-MM-ddTHH:mm:ss.SSSSSS` |
| Anos | 2024: 26.400 · 2025: 31.680 · 2026: 2.016 | idem (por `data_processamento`) |
| Relacionamento | 1:1 completo (nenhum órfão em nenhum dos lados) | |

Achados que orientaram a implementação:

1. **`valor_pagamento ≠ valor do pedido`**: a razão `valor_pagamento / Σ(valor×qtd)` é 1,00 para CARTAO_CREDITO e 0,95 para PIX e BOLETO (desconto de 5%). Por isso a regra R5 usa obrigatoriamente `SUM(valor_unitario * quantidade)`.
2. **Granularidade:** o enunciado descreve itens, e os dados reais trazem 1 item por pedido. A agregação por pedido é aplicada assim mesmo, e a unicidade é verificada (`_assert_unique`). Assim, um pedido com vários itens é somado e nunca multiplicado pelo join.
3. **Múltiplos pagamentos elegíveis:** não existem nos dados (0 casos). Regra determinística adotada: se surgirem, o pipeline **falha** com `DataQualityException`, listando até 5 `id_pedido` de exemplo. Escolher um pagamento arbitrariamente distorceria `forma_pagamento`.
4. **Distribuição status × fraude:** (true, false) = 51.751; (false, true) = 5.598; (false, false) = 2.747. Não há caso (true, true).
5. **Coerência temporal:** nenhum pagamento foi processado antes da criação do pedido, e o mês de cada linha coincide com o mês do nome do arquivo.

## 2. Decisões técnicas

| Decisão | Justificativa |
|---|---|
| PySpark 4.2.0 | Mesma versão usada em aula. `pyspark.errors` (`AnalysisException`, `PySparkException`, `getCondition()`) disponível |
| Layout `src/` + `src/main.py` | Igual ao Passo 12 do material: `python src/main.py` e `spark-submit src/main.py` funcionam sem instalar o pacote, e o wheel funciona via `--py-files` |
| Configuração em YAML + `dataclasses` imutáveis | Twelve-Factor (Princípio III). Tipagem e validação ao carregar, com erro de configuração detectado antes de subir o Spark |
| `DecimalType` para dinheiro | Elimina erro de ponto flutuante. O total sai em `DecimalType(18,2)` |
| `TimestampNTZType` (timestamp sem fuso) | A origem não tem offset. NTZ preserva o horário de parede no parsing, no `year()` e no Parquet (`timestamp_ntz`), então leitores em qualquer fuso veem os mesmos valores. Com `TimestampType`, uma sessão em São Paulo veria `2025-01-01 00:00` como `2024-12-31 21:00`. Validado com pipeline em TZ=America/Sao_Paulo, Asia/Tokyo e Pacific/Honolulu, auditoria lendo com sessões São Paulo e Tóquio e testes rodando em TZ=Asia/Tokyo |
| `data_pedido` com data e hora | Preserva a informação da origem e torna exata a ordenação "por data de criação" |
| `pathGlobFilter` (`*.csv.gz` / `*.json.gz`) | Arquivos estranhos no diretório (ex.: README) não contaminam a leitura |
| `mode = PERMISSIVE` + descarte logado | Um registro malformado não derruba o relatório inteiro, e a quantidade descartada aparece como WARNING. `FAILFAST` é configurável |
| Filtro de 2025 antes do join | Reduz o volume do join. Como `data_criacao` é chave do `groupBy`, o resultado é idêntico |
| `cache()` das entradas no pipeline | As contagens e validações reutilizam a leitura dos `.gz`, que não são divisíveis |
| `coalesce(1)` após `orderBy` | Um único arquivo com ordenação global preservada (validado lendo a ordem física do arquivo) |
| Validação antes e depois da escrita | Garante que o que foi gravado é o que foi calculado (contagem, schema, unicidade, nulos, ano) |

## 3. Validação end-to-end (execução real)

Ambiente: Ubuntu 24.04, Python 3.13.18, OpenJDK 21.0.12, PySpark 4.2.0.

### 3.1 Resumo quantitativo (log da execução real)

| Métrica | Valor |
|---|---:|
| Registros lidos de pedidos | 60.096 |
| Registros lidos de pagamentos | 60.096 |
| Pedidos após agregação | 60.096 |
| Pedidos criados em 2025 | 31.680 |
| Pagamentos recusados (`status=false`) | 8.345 |
| Pagamentos recusados e legítimos (`fraude=false`) | 2.747 |
| **Pedidos elegíveis em 2025** | **1.473** |
| **Registros gravados no Parquet** | **1.473** |

Distribuição do resultado: CARTAO_CREDITO 950 · PIX 442 · BOLETO 81, em 20 UFs. Soma de `valor_total_pedido`: R$ 1.994.160,00.

### 3.2 Auditoria independente do Parquet

Um script de auditoria fora do projeto recalculou o resultado em **Python puro** (`csv`/`json`/`gzip`, sem Spark) e o comparou com o Parquet gravado:

| Verificação | Resultado |
|---|---|
| Diretório de saída existe | OK |
| Arquivo(s) `.parquet` e `_SUCCESS` existem | OK (1 arquivo) |
| Schema físico `id_pedido:string, uf:string, forma_pagamento:string, valor_total_pedido:decimal(18,2), data_pedido:timestamp_ntz` | OK |
| Quantidade = oráculo (1.473 = 1.473) | OK |
| Conteúdo idêntico ao oráculo (conjunto de tuplas) | OK |
| Sem pedidos duplicados | OK |
| Todos os pedidos em 2025 | OK |
| Nenhum pagamento aprovado | OK |
| Nenhuma transação classificada como fraude | OK |
| Valores = `SUM(valor_unitario * quantidade)` | OK |
| Ordem física do arquivo = `uf, forma_pagamento, data_pedido` | OK |

**12/12 verificações OK**, repetidas com a sessão de leitura em `America/Sao_Paulo` e em `Asia/Tokyo`, com resultado idêntico.

### 3.3 Outras execuções validadas

| Cenário | Resultado |
|---|---|
| `python src/main.py` (raiz do projeto) | exit 0, 1.473 gravados |
| `cd src && python main.py` | exit 0, 1.473 gravados |
| `cd /tmp && python <projeto>/src/main.py` | exit 0, 1.473 gravados (saída na pasta do projeto) |
| `spark-submit --master "local[2]"` e `"local[3]"` | o master informado é respeitado (log `master='local[2]'` / `'local[3]'`), 1.473 gravados |
| `python -m build` (sem avisos) + `spark-submit --py-files dist/*.whl src/main.py` | exit 0, 1.473 gravados; o wheel contém apenas `data_engineering_pyspark/` |
| Execução com `TZ` = America/Sao_Paulo, Asia/Tokyo e Pacific/Honolulu | 1.473 gravados, auditoria 12/12 |
| Leitura do Parquet com pyarrow 26 (base do `parquet-tools`) | 1.473 linhas, `decimal128(18,2)`, `timestamp[us]` sem fuso |
| Caminho de pedidos inválido | exit **1**, log `[classe=PATH_NOT_FOUND]`, cadeia `AnalysisException → DataReadException`, `SparkSession encerrada.` |
| Clone limpo + **Python 3.10.20** (mínimo declarado) + `pip install -r requirements.txt` + `make data` + `make check` + execução a partir de `/tmp` | lint OK, 67 testes OK, 1.473 gravados |
| Clone limpo + Python 3.13.18 + `make check` + execução | lint OK, testes OK, 1.473 gravados |

### 3.4 Qualidade dos testes (mutação manual)

Para comprovar que os testes detectam defeitos reais, oito mutações foram aplicadas, uma de cada vez, em `order_business.py`, e a suíte da `OrderBusiness` (35 testes) foi executada contra cada uma:

| Mutação plantada | Testes que falharam |
|---|---:|
| `status == false` → `status != false` | 4 |
| Filtro de fraude removido | 2 |
| Total sem multiplicar pela quantidade | 6 |
| Total com `max` em vez de `sum` (ignora itens) | 3 |
| Filtro de 2025 removido | 4 |
| Ordenação sem `forma_pagamento` | 2 |
| `inner join` → `left join` | 2 |
| Validação deixa de detectar pedidos duplicados | 1 |

Todas as 8 foram detectadas, e o arquivo original foi restaurado (`cmp` idêntico). A cobertura de linhas da suíte completa é de **98%**. As linhas não cobertas são ramos defensivos: o `except PySparkException` genérico da leitura, que no modo clássico do Spark não ocorre na definição da leitura, e os `except` de última linha do `main.py` para erros de negócio e de orquestração dentro de um processo real.

## 4. Checklist de avaliação

Todas as imagens de evidência estão em `docs/evidencias/` e têm no máximo 20 linhas cada.

| Critério | Implementação | Arquivo | Evidência | Status |
|---|---|---|---|---|
| Schemas explícitos | `DataSchemas` com `StructType` para pedidos, pagamentos (com `avaliacao_fraude` aninhado) e relatório; `.schema(...)` em todas as leituras; testes usam `createDataFrame(dados, schema)` | `schemas/data_schemas.py`, `io_utils/data_handler.py` | `01_schemas_pedidos.png`, `02_schemas_pagamentos.png`, `03_leitura_csv_com_schema.png`, `04_leitura_json_com_schema.png` | CONCLUÍDO |
| Orientação a objetos | Todos os componentes em classes (`AppConfig`, `ConfigLoader`, `LoggingConfigurator`, `SparkSessionManager`, `DataSchemas`, `DataHandler`, `OrderBusiness`, `OrderPipeline` e exceções) | `src/data_engineering_pyspark/**` | `09_classe_configuracao.png`, `10_spark_session.png`, `11_logica_negocio_try_except.png`, `17_orquestracao_pipeline.png` | CONCLUÍDO |
| Aggregation Root | `src/main.py` instancia configuração, sessão, I/O, negócio e pipeline; testado executando o `main.py` real em processo isolado | `src/main.py` | `06_aggregation_root.png`, `25_teste_aggregation_root.png` | CONCLUÍDO |
| Injeção de dependências | Dependências recebidas pelo construtor; nenhuma classe cria as próprias dependências | `src/main.py`, `pipeline/order_pipeline.py` | `06_aggregation_root.png`, `17_orquestracao_pipeline.png` | CONCLUÍDO |
| Configurações centralizadas | Pacote `config`, classes `AppConfig` (+ sub-configs) e `ConfigLoader`, YAML externo, usados no `main.py` | `config/settings.py`, `config/settings.yaml` | `08_configuracao_yaml.png`, `09_classe_configuracao.png` | CONCLUÍDO |
| Gerenciamento Spark | Pacote `session`, `SparkSessionManager` (`builder`, `getOrCreate`, `stop` idempotente), usado no `main.py` com `finally`; não fixa `master` (respeita o `spark-submit`) | `session/spark_session.py`, `src/main.py` | `10_spark_session.png`, `07_main_tratamento_erros.png` | CONCLUÍDO |
| Leitura e escrita | Pacote `io_utils`, `DataHandler.read_pedidos/read_pagamentos/write_parquet/read_parquet`; cabeçalho do CSV validado (`enforceSchema=false`) | `io_utils/data_handler.py` | `03_leitura_csv_com_schema.png`, `04_leitura_json_com_schema.png`, `05_escrita_parquet.png` | CONCLUÍDO |
| Lógica de negócio | Pacote `business`, `OrderBusiness` com os 10 métodos de regra e a composição `build_report` | `business/order_business.py` | `11_logica_negocio_try_except.png`, `12_regras_calculo_filtros.png`, `13_regras_ano_join.png`, `14_projecao_ordenacao.png` | CONCLUÍDO |
| Orquestração | Pacote `pipeline`, `OrderPipeline.run()` (ler → negócio → validar → gravar → verificar) | `pipeline/order_pipeline.py` | `17_orquestracao_pipeline.png` | CONCLUÍDO |
| Logging | `import logging` na classe de negócio; `logging.basicConfig(level, format='%(asctime)s - %(levelname)s - %(message)s')` em `LoggingConfigurator`; etapas registradas em console e arquivo | `business/order_business.py`, `config/settings.py` | `15_logging_negocio.png`, `16_logging_basicconfig.png`, `23_execucao_pipeline.png` | CONCLUÍDO |
| Tratamento de erros | `try/except` na classe de negócio com `logger.exception` e `raise … from`; exceções de domínio por camada; `finally` encerra a sessão | `business/order_business.py`, `io_utils/data_handler.py`, `src/main.py` | `11_logica_negocio_try_except.png`, `07_main_tratamento_erros.png` | CONCLUÍDO |
| pyproject.toml | Metadados, Python ≥ 3.10 (validado em 3.10 e 3.13), dependências, setuptools (`src/`, só o pacote no wheel), pytest; build sem avisos | `pyproject.toml` | `18_pyproject.png` | CONCLUÍDO |
| requirements.txt | Versões exatas validadas | `requirements.txt` | `19_requirements.png` | CONCLUÍDO |
| README.md | Seções do enunciado e do roteiro, com comandos testados em clone limpo | `README.md` | — | CONCLUÍDO |
| MANIFEST.in | Inclui README, requirements, config, testes e docs; exclui dados e logs | `MANIFEST.in` | `20_manifest.png` | CONCLUÍDO |
| Testes pytest | 67 testes (35 da `OrderBusiness`), todos passando; cobertura 98%; mutação 8/8 detectada | `tests/**` | `21_teste_unitario_negocio.png`, `22_execucao_pytest.png`, `25_teste_aggregation_root.png` | CONCLUÍDO |
| Execução end-to-end | Pipeline real com os datasets oficiais; auditoria 12/12 | — | `23_execucao_pipeline.png` | CONCLUÍDO |
| Saída Parquet | 1 arquivo snappy com 1.473 linhas ordenadas, relido pelo Spark | `data/output/…` | `24_resultado_parquet.png` | CONCLUÍDO |
| Repositório público | Repositório Git preparado (`.gitignore`, sem dados/venv/logs); a publicação depende da conta GitHub do grupo | — | — | AGUARDANDO INFORMAÇÃO |
| PDF final | `Trabalho_Final_Data_Engineering_Programming_FIAP.pdf` gerado com capa, sumário, 19 seções e 24 evidências; faltam nome/RM dos integrantes e o link real do GitHub | — | — | AGUARDANDO INFORMAÇÃO |

## 5. Auditoria técnica com o enunciado como checklist

Auditoria feita do ponto de vista do professor, procurando o que poderia reduzir a nota. Cada não conformidade foi reproduzida, corrigida e comprovada por teste automatizado ou por execução.

### 5.1 Não conformidades encontradas e corrigidas

| # | Severidade | Não conformidade (como reproduzida) | Correção | Comprovação |
|---|---|---|---|---|
| NC-01 | Alta | `master: "local[*]"` no YAML sobrescrevia o `--master` do `spark-submit`: com `--master local[2]` o log mostrava `local[*]`. Em um cluster o job rodaria local, o que fere o critério "agnóstico à plataforma" | `master: null` (padrão do `SparkConfig`); o `SparkSessionManager` só define o master se ele for configurado | Execução com `local[2]` e `local[3]` → log mostra o master informado; teste verifica `config.spark.master is None` |
| NC-02 | Alta | Execução dependia do diretório corrente: `cd src && python main.py` e `cd /tmp && python …/src/main.py` terminavam com código 2 (configuração não encontrada) | `main.py` informa a raiz do projeto ao `ConfigLoader`; caminhos relativos do YAML passam a ser resolvidos pela raiz (URIs e caminhos absolutos são preservados) | Execuções a partir de `src/` e de `/tmp` → 1.473 registros; testes de resolução de caminhos e teste do `main.py` real rodando a partir de outro diretório |
| NC-03 | Média | Integridade silenciosa: com `enforceSchema=true` (padrão), um CSV com colunas fora de ordem gravava `id_pedido='TV'` sem nenhum erro | `enforce_schema: false` → o Spark valida nome e posição de cada coluna do cabeçalho contra o schema | Teste `test_cabecalho_fora_de_ordem_falha_em_vez_de_trocar_colunas` |
| NC-04 | Média | Empacotamento: o wheel instalava um módulo de topo `main` no `site-packages` (colisão de namespace, o problema que o Passo 12 do material manda evitar) | Removidos `py-modules` e o console script; o wheel contém só `data_engineering_pyspark/`, e `src/main.py` é submetido com `--py-files`, como no material | Listagem do wheel; `spark-submit --py-files dist/*.whl src/main.py` → 1.473 |
| NC-05 | Baixa | `python -m build` emitia aviso de depreciação (`project.license` como tabela TOML, remoção prevista para 2027) e avisos do `MANIFEST.in` | Campo de licença removido (a licença é decisão do grupo); regra redundante do `MANIFEST.in` removida | Build com 0 avisos |
| NC-06 | Média | Cobertura de 88%: o caminho feliz do Aggregation Root (`main.py`, 52%), os erros de escrita/releitura do `DataHandler` e os blocos `except` da `OrderBusiness` não tinham teste | 19 testes novos, incluindo o `main.py` real em processo isolado (sucesso e falha), falhas reais do Spark (`raise_error`) nos três `try/except` da classe de negócio, falhas de escrita/releitura e arquivo corrompido | 67 testes, cobertura 98% |
| NC-07 | Baixa | Código morto: `SparkSessionManager.__enter__/__exit__` nunca usados (vulture) | Métodos removidos; o encerramento é feito no `finally` do `main.py` | `vulture`: os métodos não aparecem mais; restam só 4 avisos de 60% de confiança sobre campos do dataclass `PipelineMetrics` (falsos positivos: lidos via `asdict()` e nos testes). O teste em processo isolado confirma `SparkSession encerrada.` |
| NC-08 | Baixa | O README e a evidência 24 liam o Parquet com `spark.read.parquet(...)` **sem schema**, em contradição com "TODOS os dataframes com schema explícito" | Leitura com `spark.read.schema(DataSchemas.RELATORIO)` no README e na evidência | README §24; evidência 24 recapturada |
| NC-09 | Baixa | Funções utilitárias soltas nos testes (`ts`, `_ids`, `_write` e fábricas aninhadas), em conflito literal com "TODOS os componentes em CLASSES" | Classes `DadosDeTeste` e `FabricaDataFrames` em `tests/helpers.py` | Varredura: em `src/`, a única função de módulo é `main()`; nos testes, as 9 funções de módulo são fixtures do pytest |
| NC-10 | Baixa | Se a leitura de pagamentos falhasse, o DataFrame de pedidos já em cache não era liberado (leituras fora do `try`) | Leituras dentro do `try`; o `finally` libera tudo o que foi cacheado | Teste `test_libera_o_cache_quando_a_segunda_leitura_falha` |
| NC-11 | Baixa | `pyproject.toml` suprimia `DeprecationWarning` nos testes, escondendo problemas | Supressão removida | Suíte executada com `-W error::DeprecationWarning` → 67 testes OK |

### 5.2 Pontos verificados e mantidos (com justificativa)

| Ponto | Situação | Justificativa |
|---|---|---|
| `def main()` em `src/main.py` | Mantido | É o padrão do material (Passos 6 a 14). O `main.py` é o Aggregation Root exigido, e toda a lógica está em classes |
| Fixtures do pytest como funções | Mantido | É o mecanismo do pytest (o `conftest.py` do material usa o mesmo padrão); os utilitários de teste estão em classes |
| Logging configurado no `main.py` (via `LoggingConfigurator`), e não dentro da classe de negócio | Mantido | O critério pede `import logging` na classe de negócio (feito) e o logging configurado (feito com `logging.basicConfig` e o formato do exemplo). O material (Passo 8) orienta que módulos não configurem logging |
| Falhas de conteúdo (gzip corrompido, cabeçalho inválido) chegam como `Py4JJavaError` | Mantido | Acontecem só na ação (avaliação preguiçosa); o `main.py` registra com `logger.exception` e sai com código 1, como no "Teste de Arquivo Corrompido" do Passo 9. O material desaconselha capturar `Py4JJavaError` |
| Remoção de linhas 100% idênticas de pedidos | Mantido | Tratada como duplicidade de ingestão, com WARNING no log. Nos dados oficiais a contagem é zero, então não altera o resultado |
| Evidências geradas a partir dos arquivos e das saídas reais (não são prints de tela da máquina do aluno) | Pendência de forma | O conteúdo é real (faixa de linhas e saída de terminal verificáveis). Se o professor exigir prints da própria máquina, o checklist acima indica arquivo e linhas de cada evidência |

### 5.3 Matriz de conformidade final (15 pontos auditados)

| # | Ponto auditado | Resultado | Prova real |
|---|---|---|---|
| 1 | Todos os DataFrames com schema explícito | CONFORME | Varredura: zero `inferSchema`; todas as leituras (`csv`, `json`, `parquet`) usam `.schema(...)`; todo `createDataFrame` usa `DataSchemas`/`StructType`; demais DataFrames derivam de transformações |
| 2 | Todos os componentes em classes | CONFORME | Varredura: em `src/`, a única função de módulo é `main()` (padrão do material); testes com utilitários em classes |
| 3 | `main.py` como Aggregation Root | CONFORME | Evidência 06; teste executa o `src/main.py` real em processo isolado (código 0, Parquet correto, sessão encerrada) |
| 4 | Dependências instanciadas e injetadas no `main.py` | CONFORME | `AppConfig` (via `ConfigLoader`), `SparkSessionManager`, `DataHandler`, `OrderBusiness`, `OrderPipeline` criados no `main.py`; varredura não encontrou instanciação oculta em `src/` |
| 5 | Separação real entre I/O, negócio e orquestração | CONFORME | `OrderBusiness` não lê nem grava; `DataHandler` não aplica regra; `OrderPipeline` só coordena (testado com `MagicMock(spec=DataHandler)`) |
| 6 | Processamento PySpark correto | CONFORME | Auditoria independente 12/12; 8/8 mutações detectadas |
| 7 | Valor total considera todos os itens | CONFORME | `SUM(valor_unitario × quantidade)` por pedido; testes com pedido de 2 itens (4.000,00) e mutação `max` detectada |
| 8 | Filtros de pagamento e fraude | CONFORME | Testes de exclusão de aprovados e fraudes; mutações detectadas; auditoria: 0 aprovados e 0 fraudes no Parquet |
| 9 | Somente pedidos de 2025 | CONFORME | Testes de limite (31/12/2024 23:59:59 fora, 01/01/2025 00:00:00 e 31/12/2025 23:59:59 dentro, 01/01/2026 fora); auditoria em 3 fusos |
| 10 | Ordenação correta | CONFORME | Teste de ordenação; auditoria da ordem física do arquivo (1.473 linhas) |
| 11 | Resultado Parquet correto | CONFORME | 1 arquivo, schema `decimal(18,2)`/`timestamp_ntz`, conteúdo idêntico ao cálculo independente; legível pelo Spark e pelo pyarrow |
| 12 | Logging e tratamento de erros | CONFORME | Evidências 11, 15, 16, 07, 23; testes dos três `try/except` da `OrderBusiness` com falhas reais do Spark e `exc_info` no log |
| 13 | Empacotamento completo | CONFORME | `pyproject.toml`, `requirements.txt`, `README.md`, `MANIFEST.in`; build sem avisos; wheel só com o pacote |
| 14 | Testes validam a lógica de negócio | CONFORME | 35 testes da `OrderBusiness`; 8/8 mutações detectadas; cobertura 98% |
| 15 | Funciona em ambiente independente | CONFORME | Clone limpo com Python 3.10 e 3.13, `make check` e execução a partir de outro diretório |
| — | Capa com nome e RM dos integrantes | PENDENTE (informação do grupo) | Campos marcados no PDF |
| — | Link do repositório público no PDF e no README | PENDENTE (publicação pelo grupo) | Placeholder `SEU-USUARIO` |
