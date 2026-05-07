# Koin Data Engineering Case

Pipeline de dados seguindo a arquitetura medalhão (Bronze → Silver → Gold) com orquestração via Apache Airflow 2.9 em Docker. O projeto foi estruturado com visão de escalabilidade: o domínio `sales_operations` é o primeiro de potencialmente vários conjuntos de dados, e toda a arquitetura de pastas, código e DAGs foi pensada para crescer sem reestruturação.

---

## Sumário

1. [Visão Geral da Arquitetura](#1-visão-geral-da-arquitetura)
2. [Decisões de Design](#2-decisões-de-design)
3. [Camadas do Pipeline](#3-camadas-do-pipeline)
4. [Qualidade de Dados](#4-qualidade-de-dados)
5. [LGPD e Anonimização](#5-lgpd-e-anonimização)
6. [Modelagem Gold](#6-modelagem-gold)
7. [Orquestração com Airflow](#7-orquestração-com-airflow)
8. [Escalabilidade e Reprocessamento](#8-escalabilidade-e-reprocessamento)
9. [Estrutura do Projeto](#9-estrutura-do-projeto)
10. [Como Executar](#10-como-executar)

---

## 1. Visão Geral da Arquitetura

```
data/input/     →   data/bronze/    →   data/silver/    →   data/gold/
  CSV bruto          Ingestão              Limpeza +          Tabelas
  da fonte           padronizada           validações         analíticas
                                           LGPD aplicada
                                                ↓
                                         data/rejects/
                                         Registros inválidos
                                         com motivo de rejeição
```

Todos os dados são organizados por **domínio** e **entidade**, seguindo a convenção:

```
data/{camada}/{dominio}/{entidade}/{YYYY-MM-DD}/
```

A separação por domínio (`sales_operations`, e futuros domínios como `financial`, `logistics`, etc.) permite que múltiplos datasets coexistam na mesma infraestrutura sem colisão de caminhos, DAGs ou lógica de processamento.

---

## 2. Decisões de Design

### Por que o domínio `sales_operations`?

O nome não é apenas uma pasta — é um namespace. A decisão de estruturar tudo em torno de domínios antecipa a chegada de dados de outras áreas (financeiro, logística, marketing), permitindo:

- Adicionar um novo domínio sem tocar no código existente
- DAGs independentes por domínio, sem dependências cruzadas indesejadas
- Isolamento de rejeitos, logs e métricas por domínio

### Deduplicação: `keep="last"`

A fonte de dados adota o padrão de **append de correções**: quando um registro precisa ser atualizado, uma nova linha é adicionada ao final do arquivo com os dados corrigidos. Por isso, a estratégia de deduplicação mantém sempre a **última ocorrência** por chave primária (`customer_id` ou `order_id`), garantindo que a versão mais recente do registro prevaleça.

### LGPD na camada Silver, não Bronze

Os dados brutos são ingeridos na Bronze sem qualquer transformação de conteúdo, preservando a fidelidade com a fonte. O mascaramento de PII só ocorre na transição Bronze → Silver, garantindo que:

- A Bronze seja auditável e rastreável até a origem
- A Silver em diante seja segura para uso analítico
- Nunca haja dados pessoais em texto plano nas camadas analíticas

### Gold sem partição de data

As tabelas Gold são arquivos únicos e acumulados por tabela, sem subdivisão por data de execução. A cada pipeline run, o arquivo existente é carregado, mesclado com os novos dados da Silver e deduplicado pela chave primária — o arquivo resultante representa sempre o estado mais atual e completo da tabela.

Essa decisão simplifica o consumo: ferramentas de BI e analistas apontam para um único caminho fixo, sem precisar conhecer a lógica de particionamento.

---

## 3. Camadas do Pipeline

### Input

Dados brutos entregues pela fonte, sem qualquer modificação. Organizados por entidade e data de execução:

```
data/input/sales_operations/customers/YYYY-MM-DD/
data/input/sales_operations/orders/YYYY-MM-DD/
```

### Bronze

**Responsabilidade:** ingestão fiel, padronização estrutural e rastreabilidade.

| Transformação | Descrição |
|---|---|
| Normalização de colunas | `strip + lower + replace(" ", "_")` em todos os headers |
| Metadados de rastreabilidade | `source_file` (nome do arquivo de origem) e `ingestion_timestamp` (UTC) |
| `pipeline_run_date` | Data de execução do pipeline |

A Bronze **não altera conteúdo** — valores, formatos e nulos são preservados exatamente como vieram da fonte.

### Silver (Trusted)

**Responsabilidade:** limpeza, padronização de domínio, validações e anonimização LGPD.

**Customers:**

| Tratamento | Detalhe |
|---|---|
| Deduplicação | `keep="last"` por `customer_id` |
| Normalização de datas | Aceita `%Y-%m-%d`, `%d/%m/%Y`, `%Y/%m/%d` → padroniza para `%Y-%m-%d` |
| Status inválido | Rejeição com `reject_reason = invalid_status` |
| Campos opcionais ausentes | Mantém o registro; adiciona flags `has_email`, `has_phone`, `has_created_at` |
| LGPD | `name` → SHA-256, `email` → `***@domínio`, `phone` → `****1234` |

**Orders:**

| Tratamento | Detalhe |
|---|---|
| Deduplicação | `keep="last"` por `order_id` |
| Normalização de datas | Mesma lógica dos customers; datas inválidas → rejeição |
| Normalização de `amount` | Aceita vírgula como separador decimal (`73,18` → `73.18`) |
| Valor nulo | Rejeição com `missing_amount` |
| Valor negativo | Rejeição com `negative_amount` — reembolsos devem usar `status=refunded` |
| Status inválido | Rejeição com `invalid_status` |
| Método de pagamento inválido | Rejeição com `invalid_payment_method` |
| `customer_id` inexistente | Rejeição com `unknown_customer_id` (validação referencial contra Silver customers) |

### Gold (Refined)

Ver seção [Modelagem Gold](#6-modelagem-gold).

### Rejects

Todos os registros que não passam pelas validações da Silver são gravados em:

```
data/rejects/sales_operations/{entidade}/YYYY-MM-DD/rejects_{entidade}.csv
```

Cada registro rejeitado recebe a coluna `reject_reason` indicando o motivo da rejeição. Múltiplas rejeições no mesmo run são consolidadas em um único arquivo por entidade.

---

## 4. Qualidade de Dados

### Relatório de qualidade (`report_quality`)

Em todas as camadas (Bronze e Silver), após o carregamento dos dados, é gerado um relatório de qualidade via log que inclui:

- Total de registros
- Contagem e percentual de nulos por coluna
- Contagem de linhas completamente duplicadas

### Rejeições (`split_rejects`)

O padrão de rejeição usa uma máscara booleana para separar registros válidos de inválidos sem interromper o pipeline. O fluxo continua com os registros válidos e os rejeitados são acumulados para gravação ao final. Isso garante que um arquivo com problemas parciais não bloqueie o processamento completo.

### Validações aplicadas

| Validação | Camada | Entidade |
|---|---|---|
| Data inválida ou ausente | Silver | Customers, Orders |
| Status fora do domínio | Silver | Customers (`active/inactive/blocked`), Orders (`paid/cancelled/refunded`) |
| `amount` nulo | Silver | Orders |
| `amount` negativo | Silver | Orders |
| Método de pagamento inválido | Silver | Orders (`credit_card/pix/boleto/debit_card`) |
| Integridade referencial `customer_id` | Silver | Orders → valida contra Silver Customers |

---

## 5. LGPD e Anonimização

O mascaramento é aplicado na camada **Silver**, após a deduplicação e antes da gravação do arquivo:

| Campo | Técnica | Resultado |
|---|---|---|
| `name` | SHA-256 com salt configurável via env `ANONYMIZATION_SALT` | `a3f9c2...` (64 chars hex) |
| `email` | Preserva domínio, oculta usuário | `***@gmail.com` |
| `phone` | Preserva últimos 4 dígitos | `******3456` |
| `cpf_hash` | Já fornecido hasheado pela fonte | Não reprocessado |

O domínio do e-mail é preservado intencionalmente para permitir análises de distribuição por provedor sem expor identidades. O salt do hash pode ser rotacionado via variável de ambiente sem alteração de código.

---

## 6. Modelagem Gold

Todas as tabelas Gold são arquivos únicos e acumulados — sem partição por data. A cada execução, o arquivo existente é recarregado, mesclado com novos dados e deduplicado.

### `dim_customers`

Dimensão de clientes com atributos cadastrais e métrica derivada:

| Coluna | Descrição |
|---|---|
| `customer_id` | Chave primária |
| `city`, `state` | Localização |
| `status` | Status atual (`active/inactive/blocked`) |
| `created_at` | Data de cadastro |
| `days_since_registration` | Calculado em relação à data de execução |

### `fact_orders`

Tabela fato de pedidos enriquecida com colunas de tempo:

| Coluna | Descrição |
|---|---|
| `order_id` | Chave primária |
| `customer_id` | Chave estrangeira para `dim_customers` |
| `order_date`, `amount`, `status`, `payment_method` | Atributos do pedido |
| `year`, `month`, `quarter` | Colunas derivadas para análise temporal |

### `agg_customer_metrics`

Agregação por cliente calculada a partir de todos os pedidos históricos:

| Coluna | Descrição |
|---|---|
| `customer_id` | Chave |
| `total_orders` | Total de pedidos |
| `total_amount`, `avg_order_amount` | Volume financeiro |
| `first_order_date`, `last_order_date` | Janela de atividade |

### `agg_orders_monthly`

Agregação mensal recalculada integralmente a partir de todas as partições Silver disponíveis, garantindo consistência histórica:

| Coluna | Descrição |
|---|---|
| `year`, `month` | Chave composta |
| `total_orders`, `total_amount`, `avg_amount` | Volume |
| `unique_customers` | Clientes únicos no período |
| `paid_count`, `cancelled_count`, `refunded_count` | Breakdown por status |

> **Nota sobre recálculo:** `agg_orders_monthly` e `agg_customer_metrics` varrem todas as partições Silver acumuladas (não só a do dia), garantindo que uma correção em dados históricos se propague corretamente para as agregações.

---

## 7. Orquestração com Airflow

### Estrutura de DAGs

As DAGs seguem a mesma lógica de domínio do restante do projeto, separadas em três camadas de responsabilidade:

```
dags/
├── dag_utils/
│   └── file_checks.py          ← Utilitários compartilhados entre DAGs
├── trigger/
│   └── trigger-master.py       ← Orquestrador principal (@daily)
├── trusted/                    ← Bronze + Silver (dados confiáveis)
│   ├── dag-trusted-sales-operations-customers.py
│   └── dag-trusted-sales-operations-orders.py
└── refined/                    ← Gold (dados analíticos refinados)
    ├── dag-refined-sales-operations-dim-customers.py
    ├── dag-refined-sales-operations-fact-orders.py
    ├── dag-refined-sales-operations-agg-customer-metrics.py
    └── dag-refined-sales-operations-agg-orders-monthly.py
```

A nomenclatura `trusted` e `refined` reflete a maturidade dos dados em cada estágio, independente dos nomes das pastas em `src/`.

### `trigger-master`

DAG principal com agendamento configurável via Airflow Variable:

```
SCHEDULE_INTERVAL_DAILY  (padrão: "0 6 * * *" — todo dia às 6h)
```

Estrutura de execução:

```
trigger-master
├── TaskGroup: Trusted
│   ├── dag-trusted-sales-operations-customers  ─┐ sequencial
│   └── dag-trusted-sales-operations-orders     ─┘ (orders após customers)
│
└── TaskGroup: Refined  (aguarda Trusted completar)
    ├── dag-refined-sales-operations-dim-customers → dag-refined-sales-operations-agg-customer-metrics
    └── dag-refined-sales-operations-fact-orders   → dag-refined-sales-operations-agg-orders-monthly
```

Customers roda antes de orders no grupo Trusted para que a validação de integridade referencial (Silver orders verifica `customer_id` contra Silver customers) encontre o arquivo já disponível.

Dentro do grupo Refined, `dim_customers` precede `agg_customer_metrics` e `fact_orders` precede `agg_orders_monthly`, pois as agregações dependem das tabelas base.

### `ShortCircuitOperator`

Cada DAG trusted começa com uma verificação de presença de arquivo de input. Se nenhum CSV for encontrado para a entidade e data do run, todas as tasks downstream são **puladas graciosamente** (status `skipped`, não `failed`), preservando o histórico limpo no Airflow.

### Todas as sub-DAGs têm `schedule_interval=None`

Trusted e Refined não possuem agendamento próprio — elas só executam quando acionadas pelo `trigger-master` via `TriggerDagRunOperator`. Isso evita execuções duplicadas e mantém o controle centralizado no orquestrador.

---

## 8. Escalabilidade e Reprocessamento

### Adicionando um novo domínio

Para incorporar um novo conjunto de dados (ex: `financial_operations`):

1. Criar `src/bronze/financial_operations/`, `src/silver/financial_operations/`, `src/gold/financial_operations/`
2. Criar `dags/trusted/dag-trusted-financial-operations-*.py`
3. Criar `dags/refined/dag-refined-financial-operations-*.py`
4. Adicionar os triggers no `trigger-master.py` em novos TaskGroups

Nenhum código existente precisa ser alterado.

### Reprocessamento de uma data específica

Cada script de processamento recebe `run_date` como parâmetro. Para reprocessar:

```bash
# Localmente
PIPELINE_BASE_DIR=$(pwd) PYTHONPATH=$(pwd)/src \
  python src/silver/sales_operations/orders.py  # usa date.today() por padrão
```

Para reprocessar via Airflow, basta acionar a DAG com uma data específica pelo parâmetro `logical_date` na UI ou via CLI.

### Reprocessamento da Gold

A Gold foi projetada para ser idempotente: ao rodar novamente para qualquer data, ela relê a Silver e reconstrói o arquivo acumulado com deduplicação. Não há risco de duplicação de registros ao reprocessar.

Para `agg_orders_monthly` e `agg_customer_metrics`, o recálculo é feito sempre a partir de **todas** as partições Silver disponíveis, não apenas a do dia corrente — isso garante que correções históricas se propaguem corretamente.

### Ambiente configurável via variáveis de ambiente

| Variável | Uso | Padrão |
|---|---|---|
| `PIPELINE_BASE_DIR` | Raiz do projeto (dados e código) | Diretório raiz do repositório |
| `PIPELINE_SRC_DIR` | Caminho do `src/` para as DAGs | `/opt/pipeline/src` |
| `ANONYMIZATION_SALT` | Salt do SHA-256 para campos PII | `koin_pipeline_2024_salt` |
| `SCHEDULE_INTERVAL_DAILY` | Cron do `trigger-master` | `0 6 * * *` |

---

## 9. Estrutura do Projeto

```
koin_Case/
├── dags/
│   ├── dag_utils/
│   │   └── file_checks.py
│   ├── trigger/
│   │   └── trigger-master.py
│   ├── trusted/
│   │   ├── dag-trusted-sales-operations-customers.py
│   │   └── dag-trusted-sales-operations-orders.py
│   └── refined/
│       ├── dag-refined-sales-operations-dim-customers.py
│       ├── dag-refined-sales-operations-fact-orders.py
│       ├── dag-refined-sales-operations-agg-customer-metrics.py
│       └── dag-refined-sales-operations-agg-orders-monthly.py
│
├── data/
│   ├── input/sales_operations/{customers,orders}/YYYY-MM-DD/
│   ├── bronze/sales_operations/{customers,orders}/YYYY-MM-DD/
│   ├── silver/sales_operations/{customers,orders}/YYYY-MM-DD/
│   ├── gold/sales_operations/
│   │   ├── dim_customers/dim_customers.csv
│   │   ├── fact_orders/fact_orders.csv
│   │   ├── agg_customer_metrics/agg_customer_metrics.csv
│   │   └── agg_orders_monthly/agg_orders_monthly.csv
│   └── rejects/sales_operations/{customers,orders}/YYYY-MM-DD/
│
├── src/
│   ├── bronze/sales_operations/
│   │   ├── customers.py
│   │   └── orders.py
│   ├── silver/sales_operations/
│   │   ├── customers.py
│   │   └── orders.py
│   ├── gold/sales_operations/
│   │   ├── dim_customers.py
│   │   ├── fact_orders.py
│   │   ├── agg_customer_metrics.py
│   │   └── agg_orders_monthly.py
│   └── utils/
│       ├── anonymization.py    ← LGPD: hash, mask_email, mask_phone
│       ├── data_quality.py     ← report_quality, split_rejects
│       ├── logger.py           ← logger configurado por módulo
│       └── paths.py            ← resolução de caminhos por camada
│
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## 10. Como Executar

### Com Docker (recomendado)

```bash
# 1. Inicializar o banco e criar usuário admin
docker compose up airflow-init

# 2. Subir os serviços
docker compose up -d

# 3. Acessar a UI
# http://localhost:8080  |  usuário: admin  |  senha: admin
```

Na UI do Airflow:

1. Habilitar a DAG `trigger-master`
2. Ela acionará automaticamente todas as sub-DAGs na ordem correta

> Para testar com uma data específica, acione a DAG manualmente com o parâmetro `logical_date` na UI.

### Localmente (sem Docker)

```bash
pip install -r requirements.txt

export PIPELINE_BASE_DIR=$(pwd)
export PYTHONPATH=$(pwd)/src

# Bronze
python src/bronze/sales_operations/customers.py
python src/bronze/sales_operations/orders.py

# Silver
python src/silver/sales_operations/customers.py
python src/silver/sales_operations/orders.py

# Gold
python src/gold/sales_operations/dim_customers.py
python src/gold/sales_operations/fact_orders.py
python src/gold/sales_operations/agg_customer_metrics.py
python src/gold/sales_operations/agg_orders_monthly.py
```

Por padrão, os scripts usam `date.today()` como `run_date`. Para testar outra data, modifique a chamada no bloco `if __name__ == "__main__"` de cada script.

### Estrutura esperada do input

```
data/input/sales_operations/customers/YYYY-MM-DD/arquivo.csv
data/input/sales_operations/orders/YYYY-MM-DD/arquivo.csv
```

Se o arquivo não existir para a data de execução, o `ShortCircuitOperator` da DAG correspondente irá pular todas as tasks sem falhar o pipeline.
