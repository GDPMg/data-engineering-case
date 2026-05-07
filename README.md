# Koin Data Engineering Case

Pipeline medalhão (Bronze → Silver → Gold) com orquestração em Apache Airflow 2.9 + Docker.

---

## Arquitetura

```
input  →  bronze  →  silver  →  gold
                         ↓
                      rejects
```

Os dados são organizados por dataset e tabela:

```
data/{camada}/sales_operations/{tabela}/{YYYY-MM-DD}/
```

O namespace `sales_operations` foi uma escolha deliberada pensando em escala: quando chegarem dados de outras áreas (financeiro, logística...), cada uma ganha seu próprio domínio sem mexer no que já existe. Novas DAGs, novos scripts em `src/`, mesmo padrão.

---

## Decisões de design

**LGPD na Silver, não na Bronze**
A Bronze preserva os dados exatamente como vieram da fonte — isso é importante para auditoria e rastreabilidade. O mascaramento de PII só acontece na transição Bronze → Silver, então as camadas analíticas nunca têm dados pessoais em texto plano.

**Deduplicação com `keep="last"`**
A fonte adota o padrão de corrigir registros adicionando uma nova linha ao final do arquivo. Por isso mantemos sempre a última ocorrência por chave primária — ela representa a versão mais atualizada.

**Gold sem partição de data**
Cada tabela Gold é um único arquivo acumulado. A cada execução, o arquivo existente é carregado, mesclado com os dados novos e deduplicado pela chave primária. Isso simplifica muito o consumo: ferramentas de BI e analistas apontam para um caminho fixo, sem saber nada sobre partições.

**Agregações recalculadas do zero**
`agg_orders_monthly` e `agg_customer_metrics` varrem todas as partições Silver disponíveis a cada run, não só a do dia. Isso garante que uma correção histórica se propague automaticamente para os agregados.

---

## Camadas

### Bronze
Ingestão fiel: normaliza headers (`lower + strip`), adiciona `source_file`, `ingestion_timestamp` e `pipeline_run_date`. Nenhum valor é alterado.

### Silver (Trusted)

**Customers:** deduplicação por `customer_id`, normalização de datas (aceita `dd/mm/yyyy` e `yyyy/mm/dd`), rejeição de status inválido, flags para campos opcionais ausentes (`has_email`, `has_phone`, `has_created_at`), LGPD aplicada ao final.

**Orders:** deduplicação por `order_id`, normalização de datas e de `amount` (aceita vírgula como separador decimal). Registros rejeitados e seus motivos:

| Motivo de rejeição | Condição |
|---|---|
| `invalid_or_missing_date` | Data ausente ou não parseável |
| `missing_amount` | Valor nulo |
| `negative_amount` | Valor < 0 (reembolsos usam `status=refunded`) |
| `invalid_status` | Fora de `paid / cancelled / refunded` |
| `invalid_payment_method` | Fora de `credit_card / pix / boleto / debit_card` |
| `unknown_customer_id` | `customer_id` não encontrado na Silver customers |

Todos os rejeitos vão para `data/rejects/sales_operations/{entidade}/YYYY-MM-DD/` com a coluna `reject_reason`. Um arquivo com problemas parciais não trava o processamento — os registros válidos seguem normalmente.

### Gold (Refined)

| Tabela | O que contém |
|---|---|
| `dim_customers` | Atributos cadastrais + `days_since_registration` calculado |
| `fact_orders` | Pedidos com `year`, `month`, `quarter` derivados |
| `agg_customer_metrics` | Total de pedidos, volume financeiro e janela de atividade por cliente |
| `agg_orders_monthly` | Volume mensal com breakdown por status de pagamento |

---

## LGPD

Mascaramento aplicado na Silver, após deduplicação:

| Campo | Resultado |
|---|---|
| `name` | SHA-256 com salt (`ANONYMIZATION_SALT`) |
| `email` | `***@gmail.com` — domínio preservado para análise de provider |
| `phone` | `******3456` — últimos 4 dígitos |
| `cpf_hash` | Já vem hasheado da fonte, não reprocessado |

---

## Orquestração

```
dags/
├── trigger/trigger-master.py         ← agenda e orquestra tudo
├── trusted/                          ← bronze + silver
│   ├── dag-trusted-sales-operations-customers.py
│   └── dag-trusted-sales-operations-orders.py
└── refined/                          ← gold
    ├── dag-refined-sales-operations-dim-customers.py
    ├── dag-refined-sales-operations-fact-orders.py
    ├── dag-refined-sales-operations-agg-customer-metrics.py
    └── dag-refined-sales-operations-agg-orders-monthly.py
```

O `trigger-master` roda com cron configurável via Airflow Variable (`SCHEDULE_INTERVAL_DAILY`, padrão `0 6 * * *`). Ele aciona as DAGs filhas em ordem:

```
Trusted: customers → orders  (sequencial — orders valida customer_id contra silver customers)
Refined: dim_customers → agg_customer_metrics
         fact_orders   → agg_orders_monthly
```

As DAGs trusted e refined têm `schedule_interval=None` — só rodam quando acionadas pelo master.

Cada DAG trusted começa com um `ShortCircuitOperator` que verifica se o arquivo de input existe para a data do run. Se não existir, as tasks seguintes são puladas sem falhar o pipeline.

---

## Como executar

### Docker

```bash
docker compose up airflow-init
docker compose up -d
# UI: http://localhost:8080  |  admin / admin
```

Habilite a DAG `trigger-master` na UI — ela cuida do resto.

### Localmente

```bash
pip install -r requirements.txt

export PIPELINE_BASE_DIR=$(pwd)
export PYTHONPATH=$(pwd)/src

python src/bronze/sales_operations/customers.py
python src/bronze/sales_operations/orders.py
python src/silver/sales_operations/customers.py
python src/silver/sales_operations/orders.py
python src/gold/sales_operations/dim_customers.py
python src/gold/sales_operations/fact_orders.py
python src/gold/sales_operations/agg_customer_metrics.py
python src/gold/sales_operations/agg_orders_monthly.py
```

Os scripts usam `date.today()` por padrão. Para testar outra data, edite o bloco `if __name__ == "__main__"` de cada script.

O input esperado é:
```
data/input/sales_operations/{customers,orders}/YYYY-MM-DD/arquivo.csv
```
