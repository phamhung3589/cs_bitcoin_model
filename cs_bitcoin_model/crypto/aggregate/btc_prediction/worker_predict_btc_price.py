import pandas as pd
import numpy as np
import json
from catboost import CatBoostClassifier, Pool
from datetime import datetime, timedelta
from cs_bitcoin_model.crypto.aggregate.btc_prediction.binance_client import BinanceClient
from cs_bitcoin_model.crypto.utils.cs_config import CSConfig


def get_current_price(coin_pair: str, date: str):
    client = BinanceClient()
    df = None
    if client.get_support_coin_pair(coin_pair):
        df = client.get_historical_data(1, coin_pair)
    client.close()

    return df[df.index == date]


def load_model(mdir):
    with open(f"{mdir}/desc.json") as f:
        desc = json.load(f)

    clf = None
    if desc['type'] == 'CATBOOST_BINOMIAL':
        clf = CatBoostClassifier()
        clf.load_model(f"{mdir}/parameter.dat")

    return desc, clf


class BtcPredict:
    def __init__(self):
        parser = CSConfig("production", self.__class__.__name__, "config")
        self.coin_pair = parser.read_parameter("coin_pair")
        self.model_path = parser.read_parameter("model_path")
        self.desc, self.clf = load_model(self.model_path)
        self.features = self.clf.feature_names_

    def predict(self, date):
        df = get_current_price(self.coin_pair, date)
        to_time = datetime.strptime(date, "%Y_%m_%d %H:%M:%S") + timedelta(hours=1)
        to_time = to_time.strftime("%Y_%m_%d %H:%M:%S")
        X = df[self.features]
        y_predict = self.clf.predict(X)[0][0]

        if y_predict == 1:
            price_up_down = "neutral"
        elif y_predict == 0:
            price_up_down = "down"
        else:
            price_up_down = "up"

        return {"value": y_predict,
                "prediction": price_up_down,
                "current_price": df["close"].values[0],
                "from_time": str(df.index[0]),
                "to_time": to_time,
                "reference_price": df["close"].values[0],
                "accuracy": self.desc["accuracy"]}


if __name__ == "__main__":
    current_time = datetime.utcnow().replace(minute=0, second=0)
    current_time = current_time.strftime("%Y_%m_%d %H:%M:%S")
    btc_client = BtcPredict()
    price = btc_client.predict(current_time)
    print(price)
