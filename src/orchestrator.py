import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable



from download.download_binance import download_bronze,unzip
from download.api_binance import get_binance_api
from session.spark_session import init_spark_session
from pathlib import Path

def main():
    answer = 0
    print("Python utilisé :", sys.executable)
    print("PYSPARK_PYTHON :", os.environ["PYSPARK_PYTHON"])
    print("we are gonna initiate the spark session")
    spark_session = init_spark_session("spark_session")
    
    print("From batch or from API REST")

    while answer != "1" and answer != "2" and answer != "3":
        print("tap 1 for batch tap 2 for API Rest tap 3 for Streaming")
        answer = input()

    if answer == "1":
        url = "https://data.binance.vision/data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-08-06.zip"
        destination = Path("data/bronze/BTCUSDT-aggTrades-2026-08-06.zip")
        directory_to_extract = Path("data/bronze")
        download_bronze(url, destination)
        unzip(destination, directory_to_extract)
        df = spark_session.read.csv("data/bronze/BTCUSDT-aggTrades-2026-08-06.csv")
        df.show()
    if answer == "2":
        print("api binance")
        json_binance = get_binance_api()
        df = spark_session.createDataFrame(json_binance)
        df.show()

    if answer == "3":
        print("streaming with kafka")
    
        
    

if __name__ == "__main__":
    main()