from pathlib import Path
import json
import sys

from src.download.download_binance import download_bronze, unzip
from src.download.api_binance import get_binance_api


BRONZE_DIR = Path("data/bronze")
GOLD_DIR = Path("data/gold")


def download_batch():
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    url = (
        "https://data.binance.vision/data/spot/daily/"
        "aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-08-06.zip"
    )

    destination = (
        BRONZE_DIR
        / "BTCUSDT-aggTrades-2026-08-06.zip"
    )

    download_bronze(
        url,
        destination
    )

    unzip(
        destination,
        BRONZE_DIR
    )


def transform_batch():
    from src.session.spark_session import init_spark_session

    from src.batch.bronze_to_silver import (
        standardize_batch_agg_trades,
        drop_null_agg_trades,
        drop_outlier_agg_trades,
        drop_duplicate,
        formate_timestamps_batch,
    )

    from src.batch.silver_to_gold import gold1

    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    spark = init_spark_session("batch_transform")

    spark.conf.set(
        "spark.sql.caseSensitive",
        "true"
    )

    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.csv(
        str(
            BRONZE_DIR
            / "BTCUSDT-aggTrades-2026-08-06.csv"
        )
    )

    df = standardize_batch_agg_trades(df)
    df = formate_timestamps_batch(df)

    df = drop_null_agg_trades(df)
    df = drop_outlier_agg_trades(df)
    df = drop_duplicate(df)

    gold_df = gold1(df)

    gold_df.write.mode("overwrite").parquet(
        str(
            GOLD_DIR
            / "btcusdt_batch"
        )
    )

    spark.stop()


def download_api():
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    json_binance = get_binance_api()

    with open(
        BRONZE_DIR / "binance_api.json",
        "w",
        encoding="utf-8"
    ) as f:
        json.dump(json_binance, f)


def transform_api():
    from src.session.spark_session import init_spark_session

    from src.batch.bronze_to_silver import (
        standardize_api_agg_trades,
        drop_null_agg_trades,
        drop_outlier_agg_trades,
        drop_duplicate,
        formate_timestamps_api,
    )

    from src.batch.silver_to_gold import gold1

    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    spark = init_spark_session("api_transform")

    spark.conf.set(
        "spark.sql.caseSensitive",
        "true"
    )

    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.read
        .option("multiLine", "true")
        .json(
            str(
                BRONZE_DIR
                / "binance_api.json"
            )
        )
    )

    df = standardize_api_agg_trades(df)
    df = formate_timestamps_api(df)

    df = drop_null_agg_trades(df)
    df = drop_outlier_agg_trades(df)
    df = drop_duplicate(df)

    gold_df = gold1(df)

    gold_df.write.mode("overwrite").parquet(
        str(
            GOLD_DIR
            / "btcusdt_api"
        )
    )

    spark.stop()


if __name__ == "__main__":

    if len(sys.argv) < 2:
        raise ValueError(
            "Tu dois préciser le mode : batch ou api"
        )

    mode = sys.argv[1]

    if mode == "batch":
        transform_batch()

    elif mode == "api":
        transform_api()

    else:
        raise ValueError(
            f"Mode inconnu : {mode}"
        )

# import os
# import sys
# from pathlib import Path

# # ..\sql\venv\Scripts\Activate.ps1

# os.environ["PYSPARK_PYTHON"] = sys.executable
# os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


# import sys

# def main():
#     spark_session = init_spark_session("spark_session")
#     spark_session.conf.set("spark.sql.caseSensitive", "true")
#     spark_session.sparkContext.setLogLevel("WARN")
#     mode = sys.argv[1]

#     print("Mode choisi :", mode, flush=True)

#     if mode == "batch":
#         url = "https://data.binance.vision/data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-08-06.zip"
#         destination = Path("data/bronze/BTCUSDT-aggTrades-2026-08-06.zip")
#         directory_to_extract = Path("data/bronze")
#         download_bronze(url, destination)
#         unzip(destination, directory_to_extract)  
#         df = spark_session.read.csv(
#             "data/bronze/BTCUSDT-aggTrades-2026-08-06.csv"
#         )
#         df = standardize_batch_agg_trades(df)
#         df = formate_timestamps_batch(df)
#         df.show()
#         print("=== SCHEMA BATCH ===")
#         df.printSchema()

#     elif mode == "api":
#         print("api binance", flush=True)
#         json_binance = get_binance_api()
#         df = spark_session.createDataFrame(json_binance)
#         df = standardize_api_agg_trades(df)
#         df = formate_timestamps_api(df)
#         df.show()

#     elif mode == "streaming":
#         print("streaming with kafka", flush=True)
#     before = df.count()

#     df = drop_null_agg_trades(df)
#     df = drop_outlier_agg_trades(df)
#     df = drop_duplicate(df)
    
    
#     after = df.count()

#     print("Avant :", before)
#     print("Après :", after)
#     print("Supprimées :", before - after)
#     df.show(truncate=False)
#     gold_1 = gold1(df)
#     gold_1.show(truncate=False)
# if __name__ == "__main__":
#     main()



#v1


# from pathlib import Path

# from download.download_binance import download_bronze,unzip
# from download.api_binance import get_binance_api
# from session.spark_session import init_spark_session
# from batch.bronze_to_silver import standardize_batch_agg_trades, standardize_api_agg_trades, drop_null_agg_trades, drop_outlier_agg_trades, drop_duplicate, formate_timestamps_batch, formate_timestamps_api
# from batch.silver_to_gold import gold1



# import json

# def download_batch():
#     url = (
#         "https://data.binance.vision/data/spot/daily/"
#         "aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-08-06.zip"
#     )

#     destination = Path(
#         "data/bronze/BTCUSDT-aggTrades-2026-08-06.zip"
#     )

#     directory_to_extract = Path("data/bronze")

#     download_bronze(url, destination)
#     unzip(destination, directory_to_extract)

# def transform_batch():

#     spark = init_spark_session("batch_transform")
#     spark.conf.set("spark.sql.caseSensitive", "true")
#     spark.sparkContext.setLogLevel("WARN")

#     df = spark.read.csv(
#         "data/bronze/BTCUSDT-aggTrades-2026-08-06.csv"
#     )

#     df = standardize_batch_agg_trades(df)
#     df = formate_timestamps_batch(df)

#     df = drop_null_agg_trades(df)
#     df = drop_outlier_agg_trades(df)
#     df = drop_duplicate(df)

#     gold_df = gold1(df)

#     gold_df.write.mode("overwrite").parquet(
#         "data/gold/btcusdt_batch"
#     )

#     spark.stop()



# def download_api():

#     json_binance = get_binance_api()
#     BRONZE_DIR = Path("data/bronze")
#     GOLD_DIR = Path("data/gold")

#     BRONZE_DIR.mkdir(parents=True, exist_ok=True)
#     GOLD_DIR.mkdir(parents=True, exist_ok=True)

#     with open(
#         "data/bronze/binance_api.json",
#         "w",
#         encoding="utf-8"
#     ) as f:
#         json.dump(json_binance, f)



# def transform_api():

#     spark = init_spark_session("api_transform")

#     spark.conf.set("spark.sql.caseSensitive", "true")
#     spark.sparkContext.setLogLevel("WARN")

#     df = (
#         spark.read
#         .option("multiLine", "true")
#         .json("data/bronze/binance_api.json")
#     )

#     df = standardize_api_agg_trades(df)
#     df = formate_timestamps_api(df)

#     df = drop_null_agg_trades(df)
#     df = drop_outlier_agg_trades(df)
#     df = drop_duplicate(df)

#     gold_df = gold1(df)

#     gold_df.write.mode("overwrite").parquet(
#         "data/gold/btcusdt_api"
#     )

#     spark.stop()