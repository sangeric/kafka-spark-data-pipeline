
from src.batch.bronze_to_silver import unzip
from src.download.download_binance import download_bronze
from pathlib import Path
def main():
    url = "https://data.binance.vision/data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-08-06.zip"
    destination = Path("data/bronze/BTCUSDT-aggTrades-2026-08-06.zip")
    directory_to_extract = Path("data/silver")
    download_bronze(url, destination)
    unzip(destination, directory_to_extract)
if __name__ == "__main__":
    main()