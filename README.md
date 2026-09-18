# Binance BTC/USDT — Batch, REST API & Streaming Data Pipeline

A local data engineering platform that ingests Binance **BTCUSDT aggregated trades**,
processes them with **Apache Spark** following a **medallion architecture**
(bronze → silver → gold), and orchestrates everything with **Apache Airflow**.
The whole stack runs in **Docker Compose**.

---

## Table of contents

- [Overview](#overview)
- [Tech stack](#tech-stack)
- [Architecture](#architecture)
- [Medallion layers](#medallion-layers)
- [Project structure](#project-structure)
- [Data sources](#data-sources)
- [Pipelines (DAGs)](#pipelines-dags)
- [How the containers communicate](#how-the-containers-communicate)
- [Getting started](#getting-started)
- [Useful commands](#useful-commands)
- [Services & ports](#services--ports)
- [Notes & possible improvements](#notes--possible-improvements)

---

## Overview

The project is designed around **three ingestion modes** for the `BTCUSDT` pair, all feeding the
same Spark transformation logic and the same medallion layers:

1. **Batch** — downloads the official daily `aggTrades` archive from `data.binance.vision`
   (a `.zip` containing a `.csv`).  implemented
2. **REST API** — calls the Binance REST endpoint `/api/v3/aggTrades` and stores the JSON response.
    implemented
3. **Streaming (Kafka)** — real-time ingestion of the trade stream through Apache Kafka,
   consumed by Spark Structured Streaming. 🚧 **planned / not started yet** (the `streaming` branch
   is scaffolded in `orchestrator.py` but the Kafka producer/consumer is not implemented).

All feeds are cleaned and transformed by Spark and written to the **gold** layer as Parquet files.
Airflow schedules and orchestrates the ingestion and transformation steps.

---

## Tech stack

| Component        | Version           | Role                                              |
|------------------|-------------------|---------------------------------------------------|
| Apache Spark     | `4.0.0`           | Distributed processing (1 master + 2 workers)     |
| Apache Airflow   | `3.3.1`           | Orchestration / scheduling                        |
| PostgreSQL       | `16`              | Airflow metadata database                         |
| Docker Compose   | —                 | Container orchestration & networking              |
| Python           | 3.x               | Ingestion + transformation logic (`src/`)         |

---

## Architecture

```mermaid
flowchart TB
    subgraph Host["Docker host (single Compose network)"]

        subgraph Orchestration["Orchestration — Apache Airflow"]
            AI[airflow-init<br/>db migrate]
            AA[airflow-apiserver<br/>:8082]
            AS[airflow-scheduler<br/>LocalExecutor]
            ADP[airflow-dag-processor]
            PG[(postgres<br/>:5432)]
        end

        subgraph Compute["Compute — Apache Spark"]
            SM[spark-master<br/>:7077 / UI :8081]
            SW1[spark-worker-1<br/>2 cores / 2g]
            SW2[spark-worker-2<br/>2 cores / 2g]
        end

        DATA[["./data (shared host volume)<br/>bronze / silver / gold"]]
    end

    AI --> PG
    AA --> PG
    AS --> PG
    ADP --> PG

    SW1 -->|register| SM
    SW2 -->|register| SM

    AS -.->|docker exec spark-submit| SM
    AS -.->|Spark client mode| SM

    AS <-->|read / write files| DATA
    SM <-->|read / write files| DATA
```

The **key idea**: Airflow and Spark are decoupled. They never share application memory —
they exchange data through the **shared `./data` directory** and are triggered through
either the **Docker socket** or **Spark client mode** (details below).

### Why a master + 2 workers?

Spark runs as a **standalone cluster with 1 master and 2 worker containers** on purpose. The goal is
to **simulate a real, distributed multi-node environment** locally instead of running Spark in a
single `local[*]` process:

- The **master** (`spark-master`) handles cluster coordination and resource scheduling — it does
  **not** run the computation itself.
- Each **worker** (`spark-worker-1`, `spark-worker-2`) is configured with `2 cores` and `2g` of RAM
  and stands in for a **separate physical machine** in the cluster.
- When a job runs, its **executors are spread across the two workers**, so the work is genuinely
  **partitioned and processed in parallel across "nodes"** — exactly what happens on a production
  cluster (e.g. YARN / Kubernetes / EMR), just reproduced on a single host with Docker.

This makes the setup much closer to a professional deployment than a single-machine Spark session,
and lets you observe real distribution, parallelism and shuffle behaviour in the Spark UI
(`http://localhost:8081`).

---

## Medallion layers

| Layer      | Location             | Content                                                                 | Format          |
|------------|----------------------|-------------------------------------------------------------------------|-----------------|
| **Bronze** | `data/bronze/`       | Raw ingested data, untouched (`.zip`, `.csv`, `binance_api.json`)        | zip / csv / json|
| **Silver** | in-memory            | Standardized schema, formatted timestamps, nulls/outliers/duplicates dropped | Spark DataFrame |
| **Gold**   | `data/gold/`         | Business-ready aggregated dataset (`btcusdt_batch`, `btcusdt_api`)       | Parquet         |

> In the current code, the **silver** cleaning steps are applied in-memory during the Spark job,
> and only the **gold** output is persisted to disk. The `data/silver/` folder is reserved for
> future materialization of the intermediate layer.

The transformation chain in `src/batch/`:

- `standardize_*_agg_trades` — enforce a consistent, typed schema
- `formate_timestamps_*` — convert epoch millis to proper timestamps
- `drop_null_agg_trades` — remove rows with missing critical fields
- `drop_outlier_agg_trades` — filter out anomalous prices/quantities
- `drop_duplicate` — deduplicate trades
- `gold1` — final gold-layer aggregation

---

## Project structure

```
.
├── artifacts/                 # (reserved)
├── dags/                      # Airflow DAG definitions
│   ├── hello_airflow.py       #   smoke-test DAG
│   ├── binance_api.py         #   API ingestion + transform (every 15 min)
│   └── binance_batch.py       #   batch ingestion + Spark transform (daily)
├── data/                      # SHARED volume between Airflow & Spark
│   ├── bronze/                #   raw data
│   ├── silver/                #   (reserved)
│   └── gold/                  #   Parquet outputs
├── logs/                      # Airflow task logs
├── sql/                       # (reserved)
├── src/                       # Application code
│   ├── orchestrator.py        #   entry point: download_*/transform_* + CLI (batch|api)
│   ├── batch/
│   │   ├── bronze_to_silver.py#   cleaning / standardization
│   │   └── silver_to_gold.py  #   gold aggregation (gold1)
│   ├── download/
│   │   ├── download_binance.py#   download_bronze() + unzip()
│   │   └── api_binance.py     #   get_binance_api()
│   ├── session/
│   │   └── spark_session.py   #   init_spark_session()
│   ├── common/                #   (reserved)
│   └── parsing/               #   (reserved)
├── tests/                     # (reserved)
└── docker-compose.yml
```

---

## Data sources

**Batch** (`data.binance.vision`):
```
https://data.binance.vision/data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-08-06.zip
```
> The date is currently hardcoded in `src/orchestrator.py`. Parameterize it to backfill other days.

**API** (`api.binance.com`):
```
GET https://api.binance.com/api/v3/aggTrades?symbol=BTCUSDT&limit=1000
```

---

## Pipelines (DAGs)

| DAG id           | Schedule           | Steps                                                                 | Spark execution                     |
|------------------|--------------------|----------------------------------------------------------------------|-------------------------------------|
| `hello_airflow`  | every 60 min       | `hello` (BashOperator) — smoke test                                   | —                                   |
| `binance_api`    | `*/15 * * * *`     | `get_binance_api` → `transform_binance_api` (both PythonOperator)     | **Client mode** from the Airflow container |
| `binance_batch`  | `@daily`           | `download_binance_batch` (PythonOperator) → `transform_binance_batch` (BashOperator) | **`docker exec` → spark-submit** on `spark-master` |

The two `binance_*` DAGs deliberately demonstrate **two different ways of running a Spark job**
from Airflow (see next section).

---

## How the containers communicate

All services declared in `docker-compose.yml` join the **same default Compose network**, so they
can reach each other by **service name** (Docker's built-in DNS). There are **four distinct
communication mechanisms** at play in this project.

### 1. Service-name DNS over the Compose network

Every container resolves the others by their service name — no IP addresses hardcoded.

- **Spark workers → master**: workers start with
  `start-worker.sh spark://spark-master:7077`, so they resolve `spark-master` and register with it.
- **Airflow → Postgres**: all Airflow components use
  `postgresql+psycopg2://airflow:airflow@postgres/airflow` — the host `postgres` is the DB container.
- **Scheduler → API server**: the scheduler uses
  `AIRFLOW__CORE__EXECUTION_API_SERVER_URL: http://airflow-apiserver:8080/execution/`.

### 2. Postgres as Airflow's shared state

`airflow-init`, `airflow-apiserver`, `airflow-scheduler` and `airflow-dag-processor` are separate
containers, but they coordinate **entirely through the Postgres metadata database**. The scheduler
writes task states, the API server serves the UI/API from the same DB, and the DAG processor parses
DAG files and records them there. This is the backbone of Airflow's internal communication.

### 3. Shared host volume `./data` — the data exchange "bus"

This is how **Airflow and Spark exchange data without talking directly to each other**:

- Spark services mount the whole repo: `./:/opt/spark/project` (workdir `/opt/spark/project`),
  so they see data under `/opt/spark/project/data/...`.
- Airflow services mount `./data:/opt/airflow/data` (workdir `/opt/airflow`),
  so they see the **same** files under `/opt/airflow/data/...`.

Because both paths point to the **same host folder `./data`**, a relative path like
`Path("data/bronze")` resolves to the identical physical directory in both stacks:

```
host ./data/bronze  ==  /opt/airflow/data/bronze  ==  /opt/spark/project/data/bronze
```

So the flow is: **Airflow downloads → writes to bronze → Spark reads bronze → writes gold**, all via
this shared folder. `./dags`, `./logs` and `./src` are shared the same way among Airflow services.

### 4. Docker socket — Docker-out-of-Docker (batch DAG)

The **batch** transform is a `BashOperator` that runs:

```bash
docker exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/project/src/orchestrator.py batch
```

For this to work, `airflow-scheduler` mounts the host Docker daemon socket:

```yaml
volumes:
  - /var/run/docker.sock:/var/run/docker.sock
```

This gives the scheduler container the ability to drive the **host's Docker engine** and run a
command inside its **sibling** `spark-master` container. Here the **Spark driver runs inside
`spark-master`**, and the master distributes work to the two workers.

### Two Spark execution patterns compared

| Pipeline        | Trigger from Airflow                          | Where the Spark **driver** runs | Executors        |
|-----------------|-----------------------------------------------|---------------------------------|------------------|
| `binance_batch` | `docker exec ... spark-submit` (Docker socket)| Inside `spark-master`           | `spark-worker-*` |
| `binance_api`   | `PythonOperator` → `init_spark_session()` with `master=spark://spark-master:7077` | Inside the **Airflow** container (client mode) | `spark-worker-*` |

**Batch data flow:**

```mermaid
sequenceDiagram
    participant AF as airflow-scheduler
    participant FS as ./data (shared volume)
    participant SM as spark-master
    participant SW as spark-workers

    Note over AF: DAG binance_batch (@daily)
    AF->>FS: download_batch — write .zip + .csv to data/bronze
    AF->>SM: docker exec spark-submit ... batch
    SM->>FS: read data/bronze/*.csv
    SM->>SW: distribute tasks
    SW-->>SM: partial results
    SM->>FS: write data/gold/btcusdt_batch (parquet)
```

**API data flow:**

```mermaid
sequenceDiagram
    participant API as Binance REST API
    participant AF as airflow-scheduler (Spark driver)
    participant FS as ./data (shared volume)
    participant SM as spark-master
    participant SW as spark-workers

    Note over AF: DAG binance_api (every 15 min)
    AF->>API: GET /api/v3/aggTrades?symbol=BTCUSDT
    API-->>AF: aggTrades JSON
    AF->>FS: write data/bronze/binance_api.json
    AF->>SM: open SparkSession (client mode)
    SM->>SW: distribute tasks (read json / transform)
    SW-->>SM: partial results
    AF->>FS: write data/gold/btcusdt_api (parquet)
```

> Note: the API flow uses **Spark client mode**, so the Airflow container acts as the driver and
> must have `pyspark` installed and be reachable by the executors on the network.

---

## Getting started

### Prerequisites

- Docker and Docker Compose
- Ports `8081`, `8082`, `7077` free on the host

### 1. Initialize the Airflow metadata database

```bash
docker compose up airflow-init
```

### 2. Start the whole stack

```bash
docker compose up -d
```

### 3. Check that everything is running

```bash
docker compose ps
```

### 4. Open the UIs

- Airflow: <http://localhost:8082>
- Spark master: <http://localhost:8081>

> **Credentials:** the Postgres and JWT values in `docker-compose.yml` are **local dev defaults**
> and should be changed for anything beyond local use. The Airflow admin password is generated at
> init — retrieve it from the API server logs (or configure your own). Do **not** commit real
> credentials to the repository.

### 5. Enable and trigger a DAG

```bash
docker exec -it airflow-scheduler airflow dags unpause binance_batch
docker exec -it airflow-scheduler airflow dags trigger binance_batch
```

---

## Useful commands

```bash
# Lifecycle
docker compose up airflow-init            # initialize the Airflow DB (db migrate)
docker compose up -d                      # start all containers (detached)
docker compose ps                         # list running containers

# Logs
docker logs airflow-apiserver                         # api server logs
docker compose logs -f airflow-dag-processor          # follow dag-processor logs
docker compose logs --tail=100 airflow-scheduler      # last 100 scheduler log lines

# DAG management
docker exec -it airflow-scheduler airflow dags list                 # list DAGs
docker exec -it airflow-scheduler airflow dags unpause <dag_id>     # unpause a DAG
docker exec -it airflow-scheduler airflow dags trigger <dag_id>     # trigger a run
docker exec -it airflow-scheduler airflow dags list-runs <dag_id>   # run history

# Run the Spark batch job manually
docker exec -it spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/project/src/orchestrator.py batch

# The orchestrator CLI supports: batch | api
```

---

## Services & ports

| Service                 | Container name          | Host port | Purpose                          |
|-------------------------|-------------------------|-----------|----------------------------------|
| `spark-master`          | `spark-master`          | `8081` (UI), `7077` (protocol) | Spark master + Web UI |
| `spark-worker-1`        | `spark-worker-1`        | —         | Spark worker (2 cores / 2g)      |
| `spark-worker-2`        | `spark-worker-2`        | —         | Spark worker (2 cores / 2g)      |
| `postgres`              | `airflow-postgres`      | —         | Airflow metadata DB              |
| `airflow-init`          | `airflow-init`          | —         | One-shot DB migration            |
| `airflow-apiserver`     | `airflow-apiserver`     | `8082`    | Airflow API + Web UI             |
| `airflow-scheduler`     | `airflow-scheduler`     | —         | Scheduling + LocalExecutor       |
| `airflow-dag-processor` | `airflow-dag-processor` | —         | DAG parsing                      |

---

## Notes & possible improvements

- **Parameterize the batch date** instead of hardcoding `2026-08-06` (e.g. use the Airflow logical/execution date to backfill).
- **Materialize the silver layer** to `data/silver/` for reproducibility and debugging.
- **Idempotency**: `download_bronze` / `unzip` already skip existing files; extend the same guarantees to gold writes with dated partitions.
- **Secrets management**: move Postgres, JWT and admin credentials out of the Compose file into a `.env` / secrets backend.
- **Streaming (next milestone)**: implement the third ingestion mode — a Kafka producer for the live
  Binance trade stream and a Spark Structured Streaming consumer feeding the same bronze → silver → gold flow.
- **Tests**: the `tests/` folder is reserved — add unit tests for the transformation functions.
