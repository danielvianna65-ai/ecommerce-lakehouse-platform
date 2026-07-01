# 📌 Architectural Decisions — Ecommerce Lakehouse Analytics Platform

Este documento registra as principais decisões arquiteturais adotadas na plataforma Ecommerce Lakehouse Analytics Platform.

O objetivo é documentar os critérios técnicos utilizados na construção da solução, bem como os trade-offs e benefícios das decisões implementadas.

---

# Índice

## Fundação Arquitetural

* ADR-001 — Uso da Arquitetura Medallion
* ADR-002 — Uso de Apache Spark
* ADR-003 — Spark Standalone Cluster
* ADR-004 — Uso de Delta Lake
* ADR-005 — Estratégia de Catálogo Desacoplado do Storage

## Ingestão e Processamento

* ADR-006 — Estratégia Incremental via Watermark
* ADR-007 — Estratégia Híbrida Incremental
* ADR-008 — Particionamento Temporal Incremental

## Orquestração e Operação

* ADR-009 — Uso de Apache Airflow
* ADR-010 — Separação de DAGs por Camada
* ADR-011 — Docker Compose para Infraestrutura

## Modelagem Analítica

* ADR-012 — Modelagem Dimensional
* ADR-013 — Uso de SCD Tipo 2
* ADR-014 — Estratégia de Surrogate Keys Determinísticas
* ADR-015 — Estratégia de Quarantine / Rejected Records

## Serving e Analytics

* ADR-016 — Uso do Hive Metastore
* ADR-017 — Uso do Spark ThriftServer
* ADR-018 — Camada Semântica Analítica
* ADR-019 — Organização da Camada Analytics Engineering
* ADR-020 — Uso do Apache Superset

## Observabilidade

* ADR-021 — Uso do Spark History Server

---

# Fundação Arquitetural

## ADR-001 — Uso da Arquitetura Medallion

### Decisão

Utilizar arquitetura em múltiplas camadas:

```text
Landing → Raw → Trusted → Refined
```

### Motivação

* Governança de dados
* Escalabilidade
* Reprocessamento controlado
* Qualidade de dados
* Organização operacional

---

## ADR-002 — Uso de Apache Spark

### Decisão

Utilizar Apache Spark como engine principal de processamento distribuído.

### Benefícios

* Escalabilidade horizontal
* Processamento distribuído
* Integração com Delta Lake
* Integração com HDFS

---

## ADR-003 — Spark Standalone Cluster

### Decisão

Utilizar Spark Standalone Cluster executando em Docker Compose.

### Benefícios

* Ambiente distribuído real
* Simplicidade operacional
* Reprodutibilidade

---

## ADR-004 — Uso de Delta Lake

### Decisão

Utilizar Delta Lake nas camadas Raw, Trusted e Refined.

### Benefícios

* ACID Transactions
* Merge incremental
* Schema enforcement
* Time Travel

---

## ADR-005 — Estratégia de Catálogo Desacoplado do Storage

### Decisão

Utilizar Hive Metastore apenas como catálogo centralizado, mantendo datasets Delta Lake persistidos diretamente no HDFS através de LOCATION explícita.

### Estratégia

```sql
CREATE TABLE refined.fato_vendas
USING DELTA
LOCATION '/data/04_refined/ecommerce/fato_vendas'
```

### Motivação

* Desacoplamento entre catálogo e armazenamento físico
* Flexibilidade arquitetural
* Rebuild controlado do Data Lake
* Compatibilidade com múltiplas engines SQL
* Governança centralizada de metadata

### Benefícios

* Independência entre metadata e storage
* Recuperação simplificada
* Maior controle operacional
* Arquitetura alinhada a padrões Lakehouse

---

# Ingestão e Processamento

## ADR-006 — Estratégia Incremental via Watermark

### Decisão

Utilizar estratégia incremental baseada em watermark para ingestão dos dados transacionais.

### Estratégia

```sql
WHERE data_transacao > watermark
```

### Benefícios

* Redução de I/O
* Melhor performance
* Eficiência incremental

---

## ADR-007 — Estratégia Híbrida Incremental

### Decisão

Combinar Unprocessed, Lookback e Delta Merge nas camadas Raw e Trusted.

### Estratégia

* Unprocessed
* Lookback
* Delta Merge

### Benefícios

* Idempotência
* Tratamento de late arriving data
* Consistência incremental

---

## ADR-008 — Particionamento Temporal Incremental

### Decisão

Particionar datasets utilizando a coluna dt derivada da data de transação.

### Estratégia

```text
dt=YYYY-MM-DD
```

### Benefícios

* Partition pruning
* Melhor performance
* Eficiência incremental
* Redução de I/O

---

# Orquestração e Operação

## ADR-009 — Uso de Apache Airflow

### Decisão

Utilizar Apache Airflow para orquestração dos pipelines de dados.

### Benefícios

* Agendamento
* Observabilidade
* Retry automático
* Controle operacional

---

## ADR-010 — Separação de DAGs por Camada

### Decisão

Organizar pipelines em DAGs desacopladas por camada da arquitetura Medallion.

### Estratégia

```text
01_landing_ingestion
02_raw_standardization
03_trusted_validation 
04_refined_dimensional_modeling
```

### Benefícios

* Responsabilidade clara
* Melhor monitoramento
* Facilidade operacional

---

## ADR-011 — Docker Compose para Infraestrutura

### Decisão

Utilizar Docker Compose para provisionamento e orquestração da infraestrutura distribuída da plataforma.

### Benefícios

* Reprodutibilidade
* Facilidade de setup
* Ambiente isolado

---

# Modelagem Analítica

## ADR-012 — Modelagem Dimensional

### Decisão

Utilizar Star Schema na camada Refined.

### Benefícios

* Performance analítica
* Facilidade para BI
* Organização dimensional

---

## ADR-013 — Uso de SCD Tipo 2

### Decisão

Implementar Slowly Changing Dimension Type 2 nas dimensões históricas.

### Estratégia

As dimensões históricas utilizam:

* hash_diff para detecção de mudanças de atributos
* dt_inicio para controle de vigência
* dt_fim para encerramento da versão histórica
* is_current para identificação da versão ativa

### Benefícios

* Histórico completo
* Auditoria
* Rastreabilidade temporal

---

## ADR-014 — Estratégia de Surrogate Keys Determinísticas

### Decisão

Utilizar surrogate keys determinísticas baseadas em SHA-256 nas dimensões e tabela fato da camada Refined.

### Estratégia

Dimensões SCD Tipo 2:

* sk_cliente = SHA256(id_cliente + dt_inicio)
* sk_produto = SHA256(id_produto + dt_inicio)

Dimensões Snapshot:

* sk_pagamento = SHA256(id_pagamento)

Dimensão Data:

* sk_data = YYYYMMDD (Date Key)

Tabela Fato:

* sk_venda = SHA256(id_pedido + id_item_pedido + id_produto)

### Motivação

* Garantir unicidade dos registros
* Permitir reprocessamentos completos
* Eliminar dependência de sequências globais
* Garantir reprodutibilidade das chaves
* Simplificar geração de chaves em ambiente distribuído Spark

### Benefícios

* Processamento idempotente
* Reprodutibilidade
* Simplicidade operacional
* Compatibilidade com arquiteturas Lakehouse
* Independência de mecanismos centralizados de geração de IDs

---

## ADR-015 — Estratégia de Quarantine / Rejected Records

### Decisão

Implementar camada de quarentena para registros inválidos durante o processamento analítico da camada Refined.

### Estratégia

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

### Motivação

* Garantir integridade dimensional 
* Evitar fatos órfãos  
* Preservar consistência analítica  
* Melhorar observabilidade operacional  
* Permitir rastreabilidade de falhas de qualidade

### Benefícios
* Separação entre registros válidos e inválidos 
* Melhor governança de qualidade 
* Facilidade de troubleshooting 
* Auditoria operacional 
* Maior confiabilidade analítica 
* Estratégia alinhada a padrões enterprise de Data Quality

---

# Serving e Analytics

## ADR-016 — Uso do Hive Metastore

### Decisão
Utilizar Hive Metastore como catálogo centralizado.

### Motivação

Necessidade de:

* Governança analítica
* Camada analítica SQL
* Integração com BI
* Metadata centralizada

### Benefícios

* Centralização de schemas
* Compatibilidade SQL
* Integração analítica

---

## ADR-017 — Uso do Spark ThriftServer

### Decisão

Utilizar Spark ThriftServer como camada SQL.

### Motivação

Necessidade de:

* JDBC/ODBC serving 
* Integração com ferramentas BI 
* Consultas SQL distribuídas

### Benefícios
* SQL serving distribuído 
* Integração com Superset 
* Analytics sobre Spark SQL

---

## ADR-018 — Camada Semântica Analítica

### Decisão

Utilizar views analíticas como semantic layer.

### Principal View

```sql 
refined.vw_fato_vendas_enriquecida
```

### Benefícios 

* Reutilização analítica 
* Padronização de métricas 
* Simplificação para BI

---
## ADR-019 — Organização da Camada Analytics Engineering

### Decisão

Organizar consultas analíticas, KPIs executivos e semantic layer por domínio analítico dentro da estrutura:

```text 
superset/sql/
```
### Estrutura

* semantic_layer 
* executive_kpis 
* sales_analytics 
* customer_analytics 
* payment_analytics 
* product_analytics

### Motivação

Necessidade de:
* Padronizar consultas analíticas 
* Reutilizar SQL entre dashboards 
* Centralizar regras analíticas 
* Reduzir complexidade na camada BI 
* Melhorar organização e manutenção das queries

### Benefícios

* Reutilização analítica 
* Padronização de métricas 
* Organização modular 
* Simplificação dos dashboards 
* Melhor governança analítica 
* Separação entre serving analítico e visualização BI

---

## ADR-020 — Uso do Apache Superset

### Decisão

Utilizar Apache Superset como camada de Business Intelligence.

### Motivação

Necessidade de:

* Dashboards executivos 
* Consumo analítico 
* Visualização de métricas

### Benefícios

* Dashboards interativos 
* Semantic analytics 
* Visualização executiva

---

## ADR-021 — Uso do Spark History Server

### Decisão

Utilizar Apache Spark History Server para persistência e análise do histórico das aplicações Spark executadas na plataforma.

### Estratégia

Habilitar a geração de Event Logs durante a execução das aplicações Spark e armazená-los no HDFS.

```text
spark.eventLog.enabled=true
spark.eventLog.dir=hdfs://namenode:8020/spark-events

spark.history.fs.logDirectory=hdfs://namenode:8020/spark-events
```

### Motivação

Necessidade de:
* Persistir o histórico das aplicações Spark
* Analisar execuções finalizadas
* Investigar gargalos de desempenho
* Facilitar troubleshooting
* Comparar execuções após otimizações

### Benefícios

* Histórico permanente das aplicações Spark
* Análise de Jobs, Stages e Tasks 
* Visualização do DAG de execução
* Monitoramento de Shuffle, Spill e Skew
* Apoio à otimização de pipelines
* Melhor observabilidade operacional

---

# 📌 Considerações Finais

As decisões arquiteturais adotadas priorizam:

* Escalabilidade
* Governança
* Analytics distribuído
* Lakehouse Architecture
* Business Intelligence
* Reprocessamento controlado
* Consistência analítica
* Eficiência operacional
