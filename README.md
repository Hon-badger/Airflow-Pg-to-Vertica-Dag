
### Project Structure:

```text
├── README.md
├── creds
│   └── vertica_creds.json
├── docker-compose.yml
└── src
    ├── csv_data
    ├── dags
    │   ├── dwh_vertica_global_metrics.py
    │   ├── py
    │   │   ├── postgres_currencies_extract.py
    │   │   ├── postgres_transactions_extract.py
    │   │   ├── vertica_currencies_insert.py
    │   │   ├── vertica_global_metrics_insert.py
    │   │   └── vertica_transactions_insert.py
    │   ├── stg_vertica_currencies.py
    │   └── stg_vertica_transactions_put.py
    └── sql
        ├── vertica_currencies.sql
        ├── vertica_global_metrics.sql
        └── vertica_transactions.sql
```

### How to work with project:

1) Run command docker compose up -d
2) connect to Vertica and run .sql scripts from folder `/src/sql`
3) Add Vertica and Postgres credentials in airflow
4) Run dag in order:
    a) stg_vertica_currencies.py
    b) stg_vertica_transactions_put.py
    c) dwh_vertica_global_metrics.py
5) you need to set start_date as current date
