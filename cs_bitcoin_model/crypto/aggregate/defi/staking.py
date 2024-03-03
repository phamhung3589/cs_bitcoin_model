from cs_bitcoin_model.crypto.aggregate.btc_prediction.binance_client import BinanceClient


def get_all_staking_pair():
    client = BinanceClient()
    binance_result = client.get_staking_data()
    client.close()

    result = {"binance": binance_result}

    return result

