# Binance BTC/USDT Data Pipeline

Data engineering project that ingests Binance BTCUSDT trades, processes them with Spark,
and orchestrates everything with Airflow. It all runs locally with Docker Compose.

The data is ingested in three ways:

- **Batch** — daily `aggTrades` archives from `data.binance.vision` (done)
- **REST API** — the `/api/v3/aggTrades` endpoint (done)
- **Streaming** — Kafka + Spark Structured Streaming (done)

Processing follows a bronze / silver / gold layout, and the gold layer is written as Parquet.

## Stack

- Apache Spark 4.0.0 — 1 master + 2 workers
- Apache Airflow 3.3.1 — LocalExecutor, Postgres backend
- PostgreSQL 16
- Apache Kafka 4.0.0 (KRaft mode)
- Python 3.12 producer (`confluent-kafka`, `websocket-client`)
- Spark Kafka connector `spark-sql-kafka-0-10_2.13:4.0.0`
- Docker Compose

## Architecture

```
Docker Compose network
|
+- Airflow (scheduler / apiserver / dag-processor)
|     |
|     +- metadata  ------------------>  Postgres
|     |
|     +- triggers a Spark job  ------>  Spark master --+--> worker-1
|                                                      +--> worker-2
|
+- Shared volume ./data  (bronze / silver / gold)
        ^                              ^
        |  Airflow writes bronze       |  Spark reads bronze / writes gold
```

Airflow and Spark are kept separate: they never share memory, they only exchange files through
the shared `./data` folder, and Airflow triggers Spark jobs from outside (see below).

### Streaming path

```
Binance WebSocket --> binance-producer --> Kafka --> Spark Structured Streaming --> data/gold/btcusdt_stream
                      (Python 3.12)        topic        driver in spark-master,
                                           binance.     executors on the workers
                                           aggtrades
                                           (3 partitions)
```

Airflow is not part of the streaming path: a streaming job never terminates, whereas an Airflow
task must.

### Why a master + 2 workers

Spark runs as a small standalone cluster (1 master, 2 workers) to reproduce a real distributed
setup instead of a single `local[*]` process. The master only schedules the work; each worker gets
2 cores / 2g and plays the role of a separate machine. When a job runs, its executors are spread
across both workers, so the data is actually partitioned and processed in parallel across "nodes",
like on a production cluster but on a single host.

## Medallion layers

- **bronze** (`data/bronze/`) — raw data as downloaded (`.zip`, `.csv`, `binance_api.json`)
- **silver** — cleaning done in Spark: schema, timestamp formatting, dropping nulls / outliers /
  duplicates. Currently applied in memory, not persisted.
- **gold** (`data/gold/`) — final aggregated dataset, saved as Parquet

## Project structure

```
.
├── dags/                   Airflow DAGs
├── data/                   shared with Spark (bronze / silver / gold)
│   └── checkpoints/        Spark streaming checkpoint (offsets, state)
├── logs/                   Airflow task logs
├── secrets/                Airflow password (not committed)
├── src/
│   ├── orchestrator.py     entry point: download_* / transform_* + CLI (batch | api)
│   ├── batch/              bronze_to_silver.py, silver_to_gold.py
│   ├── download/           download_binance.py, api_binance.py
│   ├── session/            spark_session.py
│   └── streaming/          producer.py, requirements.txt, stream_to_gold.py
├── tests/
├── .env                    versions and credentials (not committed)
├── Dockerfile.airflow      custom Airflow image (not enabled yet)
└── docker-compose.yml
```

## Data sources

- Batch: `https://data.binance.vision/data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-08-06.zip`
  (the date is hardcoded in `orchestrator.py` for now)
- API: `GET https://api.binance.com/api/v3/aggTrades?symbol=BTCUSDT&limit=1000`

## Pipelines (DAGs)

- **hello_airflow** — simple smoke test (runs `date && echo`).
- **binance_api** — every 15 min. Downloads from the REST API then transforms it, both as
  PythonOperators. The transform opens a SparkSession pointing at the cluster (client mode).
- **binance_batch** — daily. Downloads the archive (PythonOperator), then runs the Spark job with a
  BashOperator that does `docker exec spark-master spark-submit ... batch`.


### Streaming

- **Topic** `binance.aggtrades`, 3 partitions, created once by hand.
- **Producer** — `src/streaming/producer.py` runs in the `binance-producer` container (restarted
  automatically). It receives trades over the Binance WebSocket and writes each one to Kafka.
- **Consumer** — `src/streaming/stream_to_gold.py` reads the topic with Spark Structured Streaming:
  micro-batch every 30 s, JSON decoding, silver cleaning, watermark, deduplication on
  `agg_trade_id`, then aggregation by minute (same `gold1` function as batch and API).
- **Checkpoint** — `data/checkpoints/btcusdt_stream/` stores the Kafka offsets and the aggregation
  state. After a stop, the job resumes from the saved offsets and catches up on the trades that
  arrived meanwhile, with no loss.
- **Resources** — the driver runs in `spark-master` and is limited to 2 cores
  (`--total-executor-cores 2`), which leaves the 2 other cores free for a batch job running at the
  same time.
- **Launch** — by hand with `spark-submit` (see Useful commands). Gold files appear about 2 to 3
  minutes behind real time, because a minute is only written once the watermark passes it.

## How the containers talk to each other

- **Same Compose network.** Every container reaches the others by service name: workers connect to
  `spark-master:7077`, Airflow connects to `postgres`, the scheduler to `airflow-apiserver`.
- **Postgres is Airflow's shared state.** The scheduler, apiserver and dag-processor are separate
  containers that coordinate through the metadata database.
- **The `./data` folder is how Airflow and Spark exchange data.** Spark mounts the repo at
  `/opt/spark/project` and Airflow mounts `./data` at `/opt/airflow/data`, both pointing at the same
  host folder. So a path like `data/bronze` is the same physical directory on both sides: Airflow
  writes the bronze files, Spark reads them and writes the gold Parquet.
- **The Docker socket runs the batch Spark job.** The scheduler mounts `/var/run/docker.sock`, which
  lets the BashOperator run `docker exec spark-master spark-submit`. Here the Spark driver runs
  inside `spark-master`. In the API pipeline it's different: the driver runs inside the Airflow
  container (client mode) and only the executors run on the workers.

- **Kafka has two listeners.** `kafka:9092` for the other containers, `localhost:29092` for the host.
- **The producer's code is bind-mounted** from `./src` into `binance-producer`, and its dependencies
  are installed at container start from `src/streaming/requirements.txt`.


## Getting started

```bash
docker compose up airflow-init   # initialize the Airflow DB
docker compose up -d             # start everything
docker compose ps                # check the containers
```

- Airflow UI: http://localhost:8082
- Spark UI: http://localhost:8081

Credentials in `docker-compose.yml` are local dev defaults — change them before using this anywhere
real, and don't commit real secrets.

## Useful commands

```bash
# logs
docker compose logs -f airflow-dag-processor
docker compose logs --tail=100 airflow-scheduler

# DAGs
docker exec -it airflow-scheduler airflow dags list
docker exec -it airflow-scheduler airflow dags unpause <dag_id>
docker exec -it airflow-scheduler airflow dags trigger <dag_id>
docker exec -it airflow-scheduler airflow dags list-runs <dag_id>

# run the Spark batch job manually
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/project/src/orchestrator.py batch
```

## TODO

- Streaming mode: Kafka producer for the live trade stream + Spark Structured Streaming consumer.
- Parameterize the batch date instead of hardcoding it.
- Persist the silver layer to `data/silver/`.
- Add tests for the transformation functions.
