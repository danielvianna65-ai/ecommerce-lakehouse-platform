# 📌 Architectural Decisions — Ecommerce Lakehouse Analytics Platform

Este documento registra as principais decisões arquiteturais adotadas no projeto.

O objetivo é demonstrar os critérios técnicos utilizados na construção da arquitetura.

---

# ADR-001 — Uso da Arquitetura Medallion

## Decisão

Utilizar arquitetura em múltiplas camadas:

```text
Landing → Raw → Trusted → Refined
```

## Motivação

Separar responsabilidades do pipeline para melhorar:

* Governança
* Escalabilidade
* Reprocessamento
* Qualidade de dados
* Organização operacional

---

# ADR-002 — Uso de Apache Spark

## Decisão

Utilizar Apache Spark como engine de processamento distribuído.

## Benefícios

* Escalabilidade horizontal
* Processamento distribuído
* Integração com Delta Lake
* Integração com HDFS

---

# ADR-003 — Spark Standalone Cluster

## Decisão

Utilizar Spark Standalone Cluster via Docker Compose.

## Benefícios

* Ambiente distribuído real
* Simplicidade operacional
* Reprodutibilidade

---

# ADR-004 — Uso de Delta Lake

## Decisão

Utilizar Delta Lake nas camadas Raw, Trusted e Refined.

## Benefícios

* ACID transactions
* Merge incremental
* Time travel

---

# ADR-005 — Estratégia Incremental via Watermark (Landing)

## Decisão

Utilizar estratégia incremental baseada em watermark para controle de ingestão incremental.

## Estratégia

```sql
WHERE data_transacao > watermark
```

## Benefícios

* Redução de I/O
* Melhor performance
* Execuções incrementais eficientes

---

# ADR-006 — Estratégia Híbrida Incremental (Raw e Trusted)

## Decisão

Utilizar estratégia híbrida incremental combinando Unprocessed, Lookback e Delta Merge.

## Estratégia

* Unprocessed
* Lookback
* Delta Merge

## Benefícios

* Idempotência
* Consistência incremental
* Tratamento de late arriving data

---

# ADR-007 — Uso de Apache Airflow

## Decisão

Utilizar Apache Airflow para orquestração dos pipelines de dados.

## Motivação

Necessidade de:

* Agendamento
* Observabilidade
* Retry automático
* Controle operacional

---

# ADR-008 — Separação de DAGs por Camada

## Decisão

Organizar pipelines em DAGs desacopladas por camada da arquitetura Medallion.

## Benefícios

* Responsabilidade clara
* Melhor monitoramento
* Facilidade operacional

---

# ADR-009 — Modelagem Dimensional

## Decisão

Utilizar Star Schema na camada Refined.

## Benefícios

* Performance analítica
* Facilidade para BI
* Organização dimensional

---

# ADR-010 — Uso de SCD Tipo 2

## Decisão

Utilizar Slowly Changing Dimension Tipo 2 (SCD Type 2) nas dimensões históricas da camada Refined.

## Estratégia

Utilização de:

* hash_diff
* dt_inicio
* dt_fim
* is_current

## Benefícios

* Histórico completo
* Auditoria
* Rastreabilidade

---

# ADR-011 — Particionamento Temporal Incremental

## Decisão

Utilizar particionamento temporal baseado na data de transação de negócio nas tabelas incrementais do Data Lake.

## Estratégia

As tabelas são particionadas pela coluna:

```text
dt=YYYY-MM-DD
```

A coluna `dt` é derivada da data de transação (`data_transacao`) utilizada no processamento incremental dos datasets.

## Benefícios

* Partition pruning
* Melhor performance
* Eficiência incremental
* Redução de I/O
* Reprocessamento controlado por partição
* Alinhamento temporal com eventos de negócio

---

# ADR-012 — Docker Compose para Infraestrutura

## Decisão

Utilizar Docker Compose para provisionamento e orquestração da infraestrutura distribuída da plataforma.

## Benefícios

* Reprodutibilidade
* Facilidade de setup
* Ambiente isolado

---

# ADR-013 — Uso do Hive Metastore

## Decisão

Utilizar Hive Metastore como catálogo centralizado.

## Motivação

Necessidade de:

* Governança analítica
* Camada analítica SQL
* Integração com BI
* Metadata centralizada

## Benefícios

* Centralização de schemas
* Compatibilidade SQL
* Integração analítica

---

# ADR-014 — Uso do Spark ThriftServer

## Decisão

Utilizar Spark ThriftServer como camada SQL.

## Motivação

Necessidade de:

* JDBC/ODBC serving
* Integração com ferramentas BI
* Consultas SQL distribuídas

## Benefícios

* SQL serving distribuído
* Integração com Superset
* Analytics sobre Spark SQL

---

# ADR-015 — Uso do Apache Superset

## Decisão

Utilizar Apache Superset como camada de Business Intelligence.

## Motivação

Necessidade de:

* Dashboards executivos
* Consumo analítico
* Visualização de métricas

## Benefícios

* Dashboards interativos
* Semantic analytics
* Visualização executiva

---

# ADR-016 — Camada Semântica Analítica

## Decisão

Utilizar views analíticas como semantic layer.

## Principal View

```sql
refined.vw_fato_vendas_enriquecida
```

## Benefícios

* Reutilização analítica
* Padronização de métricas
* Simplificação para BI

---
# ADR-017 — Organização da Camada Analytics Engineering

## Decisão

Organizar consultas analíticas, KPIs executivos e semantic layer por domínio analítico dentro da estrutura:

```text
superset/sql/
```
## Estrutura

* semantic_layer
* executive_kpis
* sales_analytics
* customer_analytics
* payment_analytics
* product_analytics

## Motivação

Necessidade de:

* Padronizar consultas analíticas
* Reutilizar SQL entre dashboards
* Centralizar regras analíticas
* Reduzir complexidade na camada BI
* Melhorar organização e manutenção das queries

## Benefícios

* Reutilização analítica
* Padronização de métricas
* Organização modular
* Simplificação dos dashboards
* Melhor governança analítica
* Separação entre serving analítico e visualização BI

---
# ADR-018 — Estratégia de Catálogo Desacoplado do Storage

## Decisão

Utilizar Hive Metastore apenas como catálogo centralizado, mantendo datasets Delta Lake persistidos diretamente no HDFS através de LOCATION explícita.

## Estratégia

```sql
CREATE TABLE refined.fato_vendas
USING DELTA
LOCATION '/data/04_refined/ecommerce/fato_vendas'
```

## Motivação

Necessidade de:

* Desacoplamento entre catálogo e armazenamento físico
* Flexibilidade arquitetural
* Rebuild controlado do Data Lake
* Compatibilidade com múltiplas engines SQL
* Governança centralizada de metadata

## Benefícios
* Independência entre metadata e storage
* Facilidade de recuperação operacional
* Arquitetura alinhada a padrões modernos Lakehouse
* Maior controle sobre paths físicos
* Flexibilidade operacional

---

# ADR-019 — Estratégia de Quarantine / Rejected Records

## Decisão

Implementar camada de quarentena para registros inválidos durante o processamento analítico da camada Refined.

## Estratégia

Registros que falham nas validações de conformidade dimensional são direcionados para tabelas de rejeição dedicadas.

Exemplo:

```text
rejected_fato_vendas
```
A validação ocorre após os joins dimensionais da tabela fato.

Registros são rejeitados quando surrogate keys obrigatórias retornam valores nulos:

* sk_cliente
* sk_produto
* sk_pagamento
* sk_data_pedido

## Motivação

Necessidade de:

* Garantir integridade dimensional
* Evitar fatos órfãos
* Preservar consistência analítica
* Melhorar observabilidade operacional
* Permitir rastreabilidade de falhas de qualidade

## Benefícios

* Separação entre registros válidos e inválidos
* Melhor governança de qualidade
* Facilidade de troubleshooting
* Auditoria operacional
* Maior confiabilidade analítica
* Estratégia alinhada a padrões enterprise de Data Quality

---

# 📌 Considerações Finais

As decisões arquiteturais adotadas priorizam:

* Escalabilidade
* Governança
* Analytics distribuído
* Camada analítica SQL distribuída
* Lakehouse architecture
* Business Intelligence
* Reprocessamento controlado
* Consistência analítica