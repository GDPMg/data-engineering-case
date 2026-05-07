# Koin Data Engineering

Pipeline baseado na arquitetura medalhão (**Bronze → Silver → Gold**), orquestrado com **Apache Airflow** em Docker.

```
data/input/  →  data/bronze/  →  data/silver/  →  data/gold/
  CSV bruto      Ingestão          Limpeza +         Tabelas
  da fonte       padronizada       validações         analíticas
                                   LGPD aplicada
                                        ↓
                                   data/rejects/
```

Dados organizados por camada, domínio e entidade: `data/{camada}/{dominio}/{entidade}/{YYYY-MM-DD}/`

---

## Decisões de Design

**LGPD na Silver, não na Bronze** — A Bronze preserva os dados brutos sem alteração para rastreabilidade e auditoria. O mascaramento de PII ocorre apenas na transição Bronze → Silver, garantindo que nenhum dado pessoal chegue às camadas analíticas. O salt do hash é configurável via `ANONYMIZATION_SALT`.

**Gold acumulada sem partição de data** — As tabelas Gold são arquivos únicos por tabela. A cada run, o arquivo existente é carregado, mesclado com novos dados da Silver e deduplicado pela chave primária (`keep="last"`). O arquivo sempre representa o estado mais atual.

**Validação referencial no Silver** — Orders rejeitam registros cujo `customer_id` não existe na Silver de customers do mesmo `run_date`. Por isso, customers é sempre processado antes de orders — tanto localmente quanto na ordenação do `trigger-master`.

**ShortCircuit nas DAGs** — Todas as DAGs começam com um `ShortCircuitOperator` que verifica a existência do arquivo de input (trusted) ou silver (refined) para o `run_date`. Se não encontrar, encerra sem erro e pula as tasks downstream.

**`run_date` configurável via params** — Todas as DAGs expõem `run_date` como parâmetro com default em `datetime.now()`. Ao disparar manualmente no Airflow, o campo aparece no formulário "Trigger DAG w/ config" e pode ser sobrescrito para qualquer data.

**Estrutura extensível por domínio** — Pastas, DAGs e scripts seguem a convenção `{camada}/{dominio}/{entidade}`. Adicionar um novo domínio não exige alteração no código existente.

---

## Camadas

**Bronze** — Normaliza cabeçalhos (`strip`, `lower`, `replace(" ", "_")`) e adiciona metadados de rastreabilidade (`source_file`, `ingestion_timestamp`, `pipeline_run_date`). Não altera conteúdo.

**Silver** — Deduplicação (`keep="last"`), normalização de datas (aceita `%Y-%m-%d`, `%d/%m/%Y`, `%Y/%m/%d`), normalização de `amount` (vírgula → ponto), validação de domínio e integridade referencial. Registros inválidos vão para `data/rejects/` com coluna `reject_reason`.

Motivos de rejeição possíveis:

| `reject_reason` | Entidade |
|---|---|
| `invalid_status` | Customers, Orders |
| `invalid_or_missing_date` | Customers, Orders |
| `missing_amount` / `negative_amount` | Orders |
| `invalid_payment_method` | Orders |
| `unknown_customer_id` | Orders |

**Gold** — Quatro tabelas acumuladas reconstruídas a cada run:

`dim_customers` — Dimensão de clientes. Chave: `customer_id`. Contém `city`, `state`, `status` e `days_since_registration` (calculado em relação ao `run_date`). Deduplicada por `customer_id` com `keep="last"`.

`fact_orders` — Tabela fato de pedidos. Chave: `order_id`. Contém `customer_id`, `order_date`, `amount`, `status`, `payment_method` e as colunas derivadas `year`, `month`, `quarter` para facilitar análises temporais sem transformação no consumo.

`agg_customer_metrics` — Agregação por cliente calculada sobre todos os pedidos históricos da Silver. Colunas: `total_orders`, `total_amount`, `avg_order_amount`, `first_order_date`, `last_order_date`. Recalculada integralmente a cada run para garantir consistência.

`agg_orders_monthly` — Agregação mensal de pedidos. Chave composta: `year` + `month`. Colunas: `total_orders`, `total_amount`, `avg_amount`, `unique_customers`, `paid_count`, `cancelled_count`, `refunded_count`. Também recalculada integralmente a partir de todas as partições Silver disponíveis.

**LGPD (aplicada na Silver):**

| Campo | Técnica |
|---|---|
| `name` | SHA-256 com salt |
| `email` | `***@domínio` (preserva domínio para análise de distribuição) |
| `phone` | `******XXXX` (preserva últimos 4 dígitos) |
| `cpf_hash` | Já hasheado pela fonte — não reprocessado |

---

## Orquestração

```
trigger-master  (diário às 6h, configurável via Airflow Variable SCHEDULE_INTERVAL_DAILY)
├── trusted/customers  →  trusted/orders
└── refined/dim_customers  →  refined/agg_customer_metrics
    refined/fact_orders    →  refined/agg_orders_monthly
```

Em todas as camadas, após o processamento é gerado um relatório de qualidade via log com total de registros, percentual de nulos por coluna e contagem de duplicatas.

---

## Escalabilidade e Reprocessamento

**Novo domínio** — Criar as pastas `src/{bronze,silver,gold}/novo_dominio/`, as DAGs correspondentes em `dags/trusted/` e `dags/refined/`, e adicionar os triggers no `trigger-master.py`. Nenhum código existente precisa ser alterado.

**Reprocessar uma data específica** — Acionar a DAG manualmente via "Trigger DAG w/ config" no Airflow e informar o `run_date` desejado. Localmente, basta passar a data como argumento no bloco `if __name__ == "__main__"` de cada script.

---

## Como Executar

### Docker (recomendado)

```bash
docker compose up -d
http://localhost:8080  |  usuário: admin  |  senha: admin
```

Habilitar a DAG `trigger-master` — ela dispara todas as sub-DAGs na ordem correta. Para rodar uma data específica, use "Trigger DAG w/ config" e edite o campo `run_date`.

### Local (sem Docker)

```bash
pip install -r requirements.txt

# Linux/macOS
export PIPELINE_BASE_DIR=$(pwd) && export PYTHONPATH=$(pwd)/src

# Windows (PowerShell)
$env:PIPELINE_BASE_DIR = (Get-Location).Path; $env:PYTHONPATH = (Get-Location).Path + "\src"

python src/bronze/sales_operations/customers.py
python src/bronze/sales_operations/orders.py
python src/silver/sales_operations/customers.py
python src/silver/sales_operations/orders.py
python src/gold/sales_operations/dim_customers.py
python src/gold/sales_operations/fact_orders.py
python src/gold/sales_operations/agg_customer_metrics.py
python src/gold/sales_operations/agg_orders_monthly.py
```
