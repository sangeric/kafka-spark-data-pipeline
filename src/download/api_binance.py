import requests



def get_binance_api(symbol: str = "BTCUSDT", limit : int = 1000):
    url = "https://api.binance.com/api/v3/aggTrades"

    resp = requests.get(
        url,
        params={
            "symbol" : symbol,
            "limit" : limit,
        },
        timeout = 30,
    )

    resp.raise_for_status()

    return resp.json()