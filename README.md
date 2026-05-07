# Koin Data Engineering

Pipeline de dados baseado na arquitetura medalhão (**Bronze → Silver → Gold**), com orquestração pelo **Apache Airflow** rodando em **Docker**. O projeto foi organizado pensando em crescimento e manutenção. O domínio `sales_operations` foi usado como primeiro conjunto de dados, mas a estrutura de pastas, códigos e DAGs já foi pensada para permitir a inclusão de novos domínios no futuro sem precisar refazer a arquitetura.


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

### LGPD na camada Silver, não Bronze

Os dados brutos são ingeridos na Bronze sem qualquer transformação de conteúdo, preservando a fidelidade com a fonte. O mascaramento de PII só ocorre na transição Bronze → Silver, garantindo que:

- A Bronze seja auditável e rastreável até a origem
- A Silver em diante seja segura para uso analítico
- Nunca haja dados pessoais nas camadas analíticas

### Gold sem partição de data

As tabelas Gold são arquivos únicos e acumulados por tabela, sem subdivisão por data de execução. A cada pipeline run, o arquivo existente é carregado, mesclado com os novos dados da Silver e deduplicado pela chave primária — o arquivo resultante representa sempre o estado mais atual e completo da tabela.

---

## 3. Camadas do Pipeline

### Input

Dados brutos entregues pela fonte, sem qualquer modificação. Em um cenário real, essa origem poderia ser substituída por integrações com Google Drive, OneDrive, etc.

### Bronze

- **Normalização de colunas:** aplicação de `strip`, `lower` e `replace(" ", "_")` em todos os cabeçalhos.

- **Metadados de rastreabilidade:** inclusão das colunas `source_file`, com o nome do arquivo de origem, e `ingestion_timestamp`, com o horário de ingestão em UTC.

- **Data de execução do pipeline:** inclusão da coluna `pipeline_run_date`, representando a data em que o pipeline foi executado.

A Bronze **não altera conteúdo** 

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

### `trigger-master`

DAG principal com agendamento configurável via Airflow Variable:

```
SCHEDULE_INTERVAL_DAILY  (padrão: "0 6 * * *" — todo dia às 6h)
```

Estrutura de execução:

Customers roda antes de orders no grupo Trusted para que a validação de integridade referencial (Silver orders verifica `customer_id` contra Silver customers) encontre o arquivo já disponível.

Dentro do grupo Refined, `dim_customers` precede `agg_customer_metrics` e `fact_orders` precede `agg_orders_monthly`, pois as agregações dependem das tabelas base.

### `ShortCircuitOperator`

Cada DAG trusted começa com uma verificação de presença de arquivo de input. Se nenhum CSV for encontrado para a entidade e data do run, todas as tasks downstream são **puladas**.

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

Para reprocessar via Airflow, basta acionar a DAG com uma data específica pelo parâmetro `logical_date` na UI ou via CLI.

---

## 9. Como Executar

### Com Docker (recomendado)

```bash
# 1. Inicializar o banco e criar usuário admin
docker compose up airflow-init

# 2. Subir os serviços
docker compose up -d

# 3. Acessar a UI
http://localhost:8080  |  usuário: admin  |  senha: admin
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

