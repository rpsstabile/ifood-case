# iFood - Case Técnico Engenharia de Dados

## Objetivo

Construir uma pipeline de dados utilizando arquitetura Medallion (Bronze, Silver e Gold) para processamento dos dados de corridas de táxi da cidade de Nova York, disponibilizando informações analíticas para responder às questões de negócio propostas no desafio.

---

# Arquitetura da Solução

```text
Parquet Files
      │
      ▼
 Bronze Layer
      │
      ▼
 Silver Layer
      │
      ▼
 Gold Layer
```

Camadas implementadas:

* Bronze: armazenamento dos arquivos brutos.
* Silver: limpeza, padronização, validação e enriquecimento dos dados.
* Gold: agregações analíticas para responder às perguntas de negócio.
* Quarantine: armazenamento de registros rejeitados por regras de qualidade.
* Governance: documentação, descrições e tags de negócio.
* Observability: auditoria de pipeline e métricas de qualidade.

---

# Tecnologias Utilizadas

* Databricks Community Edition
* Apache Spark (PySpark)
* Delta Lake
* Unity Catalog
* SQL
* Python

---

# Estrutura do Projeto

```text
ifood-case/

├── src/
│   ├── notebook/
│   └── parquet/
│
├── analysis/
│   └── sql/
│
└── README.md
```

---

# Pré-Requisitos

* Workspace Databricks Community Edition
* Catálogo `main`
* Permissões para criação de Volumes
* Upload dos arquivos Parquet disponibilizados no desafio

---

# Execução da Solução

## 1. Criar estrutura inicial

Executar:

```text
ifood-case/src/notebook/create_layers.py
```

Responsável por criar:

* Catalog: main
* Schema: ifood
* Volumes Bronze
* Volumes Silver
* Volumes Gold
* Volumes Quarantine

---

## 2. Upload dos arquivos Parquet

Realizar upload dos arquivos localizados em:

```text
ifood-case/src/parquet
```

para os respectivos diretórios da camada Bronze:

```text
/Volumes/main/ifood/ifood_case/bronze/
```

Estrutura esperada:

```text
bronze/
├── yellow_taxi/
├── green_taxi/
├── forhire_taxi/
└── highvolume_forhire_taxi/
```

---

## 3. Gerar Camada Silver

Executar os notebooks abaixo:

### Yellow Taxi

```text
ifood-case/src/notebook/yellow_taxi_silver_layer.py
```

### Green Taxi

```text
ifood-case/src/notebook/green_taxi_silver_layer.py
```

Funcionalidades implementadas:

* Leitura incremental
* Controle de arquivos processados
* Padronização de schema
* Data Quality
* Quarantine
* Deduplicação
* Enriquecimento
* Escrita Delta
* Auditoria de execução

---

## 4. Gerar Camada Gold

Executar:

### monthy_revenue

```text
ifood-case/src/notebook/monthy_revenue_gold_layer.py
```

Responsável por gerar:

```text
main.ifood.monthly_revenue
```

---

### passengers_by_hour

```text
ifood-case/src/notebook/passengers_by_hour_gold_layer.py
```

Responsável por gerar:

```text
main.ifood.passengers_by_hour
```

---

## 5. Executar as Respostas do Desafio

### Questão 1

```text
ifood-case/analysis/sql/resposta_1.sql
```

Resposta:

> Qual a média de valor total (total_amount) recebido em um mês considerando todos os yellow táxis da frota?

---

### Questão 2

```text
ifood-case/analysis/sql/resposta_2.sql
```

Resposta:

> Qual a média de passageiros (passenger_count) por cada hora do dia que pegaram táxi no mês de maio considerando todos os táxis da frota?

---

# Componentes Adicionais

## Data Quality

Validações implementadas:

* Campos obrigatórios
* Valores negativos
* Pickup anterior ao Dropoff
* Deduplicação
* Consistência entre data da corrida e partição física
* Quarentena de registros inválidos

---

## Processamento Incremental

Implementado através da tabela:

```text
main.ifood.process_control
```

Evitando reprocessamento de arquivos já carregados.

---

## Observabilidade

Implementado através da tabela:

```text
main.ifood.pipeline_audit
```

Métricas registradas:

* Registros lidos
* Registros válidos
* Registros em quarentena
* Registros gravados
* Quantidade de arquivos processados
* Tempo de execução
* Status da execução

---

# Execuções Opcionais

## 6. Executar testes de validação

```text
ifood-case/src/notebook/validation_test.py
```

Realiza smoke tests sobre as tabelas Gold para validar:

* Existência de dados
* Consistência das agregações
* Métricas não negativas
* Cobertura dos períodos processados

---

## 7. Aplicar Governança

```text
ifood-case/src/notebook/data_governance_tags_descriptions.py
```

Responsável por:

* Comentários de tabelas
* Comentários de colunas
* Tags de negócio
* Data Dictionary

---

## 8. Análise de Observabilidade

```text
ifood-case/analysis/sql/analise_opcional_1.sql
```

Consultas para acompanhamento operacional da pipeline.

---

## 9. Análise de Qualidade de Dados

```text
ifood-case/analysis/sql/analise_opcional_2.sql
```

Consultas para análise das métricas de qualidade dos dados processados.

---

# Autor
Renan Palma Stabile
