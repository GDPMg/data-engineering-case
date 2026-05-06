# Koin Data Engineering Case

Pipeline de dados seguindo arquitetura medalhão (Bronze → Silver → Gold) com orquestração via Apache Airflow no Docker.

---

## Arquitetura

```
data/input/          ← CSVs brutos da fonte
data/bronze/         ← Ingestão + mascaramento LGPD
data/silver/         ← Dados limpos e padronizados
data/gold/           ← Tabelas analíticas (dim/fact/agg)
data/rejects/        ← Registros inválidos com motivo de rejeição
```

### Camadas

| Camada | Responsabilidade |
|--------|-----------------|
| **Bronze** | Leitura do CSV bruto, normalização de colunas, aplicação de LGPD, adição de metadados (`ingestion_timestamp`, `source_file`) |
| **Silver** | Deduplicação, normalização de datas, tratamento de valores inválidos, validação referencial, separação de rejeitos |
| **Gold** | Modelagem analítica: `dim_customers`, `fact_orders`, `agg_customer_metrics`, `agg_orders_monthly` |

---

## Tratamento de Dados

### Customers
| Problema | Decisão |
|----------|---------|
| Duplicatas (C0005, C0015, C0030, C0045) | Mantém a última ocorrência (registro mais atualizado) |
| Datas em formatos variados (`dd/MM/yyyy`, `yyyy/MM/dd`) | Normaliza para `yyyy-MM-dd` |
| Campos nulos (email, phone, created_at) | Mantém o registro com flags `has_email`, `has_phone`, `has_created_at` |
| Status inválido | Rejeitado para `rejects/` |

### Orders
| Problema | Decisão |
|----------|---------|
| Duplicatas (O00011, O00026, O00041, O00076) | Mantém a última ocorrência |
| Datas inválidas (mês 13, formato inválido) | Rejeitado |
| Valor negativo | Rejeitado (reembolso deve usar `status=refunded`) |
| Separador decimal por vírgula (`73,18`) | Normalizado para ponto flutuante |
| Valor nulo | Rejeitado |
| `customer_id` inexistente (C9999) | Rejeitado (falha referencial) |

---

## LGPD

Mascaramento aplicado já na camada **bronze** — dados PII nunca são persistidos em texto plano:

| Campo | Tratamento |
|-------|-----------|
| `name` | Hash SHA-256 com salt |
| `email` | `***@domínio.com` (domínio visível para análise de provider) |
| `phone` | `*******1234` (últimos 4 dígitos) |
| `cpf_hash` | Já fornecido hasheado pela fonte |

---

## Modelagem Gold

```
dim_customers.csv         → customer_id, city, state, status, created_at, days_since_registration
agg_customer_metrics.csv  → customer_id, total_orders, total_amount, avg_order_amount, first/last_order_date
fact_orders.csv           → order_id, customer_id, order_date, amount, payment_method, status, year, month, quarter
agg_orders_monthly.csv    → year, month, total_orders, total_amount, avg_amount, unique_customers, paid/cancelled/refunded_count
```

---

## Como Executar

### Com Docker (Airflow)

```bash
# Subir o ambiente
docker compose up airflow-init
docker compose up -d

# Acessar UI: http://localhost:8080
# Usuário: admin | Senha: admin
```

Ativar as DAGs na UI na ordem:
1. `customers_pipeline`
2. `orders_pipeline`
3. `gold_pipeline` (aguarda as duas anteriores via ExternalTaskSensor)

### Localmente (sem Docker)

```bash
pip install -r requirements.txt

export PIPELINE_BASE_DIR=$(pwd)
export PYTHONPATH=$(pwd)/src

# Rodar por camada
python src/bronze/sales_operations/customers.py
python src/bronze/sales_operations/orders.py
python src/silver/sales_operations/customers.py
python src/silver/sales_operations/orders.py
python src/gold/sales_operations/customers.py
python src/gold/sales_operations/orders.py
```

---

## Estrutura do Projeto

```
koin_Case/
├── dags/
│   ├── customers_pipeline_dag.py
│   ├── orders_pipeline_dag.py
│   └── gold_pipeline_dag.py
├── data/
│   ├── input/sales_operations/{customers,orders}/YYYY-MM-DD/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── rejects/
├── src/
│   ├── utils/         ← paths, logger, anonymization, data_quality
│   ├── bronze/sales_operations/
│   ├── silver/sales_operations/
│   └── gold/sales_operations/
├── docker-compose.yml
├── requirements.txt
└── README.md
```
