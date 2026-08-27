import os
import sys
from pathlib import Path

# ..\sql\venv\Scripts\Activate.ps1

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from download.download_binance import download_bronze,unzip
from download.api_binance import get_binance_api
from session.spark_session import init_spark_session
from batch.bronze_to_silver import standardize_batch_agg_trades, standardize_api_agg_trades, drop_null_agg_trades, drop_outlier_agg_trades, drop_duplicate, formate_timestamps_batch, formate_timestamps_api
from batch.silver_to_gold import gold1
import sys

def main():
    spark_session = init_spark_session("spark_session")
    spark_session.conf.set("spark.sql.caseSensitive", "true")
    spark_session.sparkContext.setLogLevel("WARN")
    mode = sys.argv[1]

    print("Mode choisi :", mode, flush=True)

    if mode == "batch":
        url = "https://data.binance.vision/data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-08-06.zip"
        destination = Path("data/bronze/BTCUSDT-aggTrades-2026-08-06.zip")
        directory_to_extract = Path("data/bronze")
        download_bronze(url, destination)
        unzip(destination, directory_to_extract)  
        df = spark_session.read.csv(
            "data/bronze/BTCUSDT-aggTrades-2026-08-06.csv"
        )
        df = standardize_batch_agg_trades(df)
        df = formate_timestamps_batch(df)
        df.show()
        print("=== SCHEMA BATCH ===")
        df.printSchema()

    elif mode == "api":
        print("api binance", flush=True)
        json_binance = get_binance_api()
        df = spark_session.createDataFrame(json_binance)
        df = standardize_api_agg_trades(df)
        df = formate_timestamps_api(df)
        df.show()

    elif mode == "streaming":
        print("streaming with kafka", flush=True)
    before = df.count()

    df = drop_null_agg_trades(df)
    df = drop_outlier_agg_trades(df)
    df = drop_duplicate(df)
    
    
    after = df.count()

    print("Avant :", before)
    print("Après :", after)
    print("Supprimées :", before - after)
    df.show(truncate=False)
    gold_1 = gold1(df)
    gold_1.show(truncate=False)
if __name__ == "__main__":
    main()