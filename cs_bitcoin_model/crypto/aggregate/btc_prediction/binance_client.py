from binance.client import Client
import pandas as pd
import datetime
pd.options.display.width = 0
from cs_bitcoin_model.crypto.utils.cs_config import CSConfig


class BinanceClient:
    def __init__(self):
        parser = CSConfig("production", self.__class__.__name__, "config")
        self.api_key = parser.read_parameter("api_key")
        self.api_secret = parser.read_parameter("api_secret")
        self.client = Client(self.api_key, self.api_secret)
        self.support_coin = {"BTCUSDT"}

    def get_support_coin_pair(self, coin_pair):
        return coin_pair in self.support_coin

    def get_historical_data(self, delay_dates: int, coin_pair="BTCUSDT", time_range=Client.KLINE_INTERVAL_1HOUR):
        # Calculate the timestamps for the binance api function
        untilThisDate = datetime.datetime.now()
        sinceThisDate = untilThisDate - datetime.timedelta(days=delay_dates)

        # Execute the query from binance - timestamps must be converted to strings !
        candle = self.client.get_historical_klines(coin_pair, time_range, str(sinceThisDate), str(untilThisDate))

        # Create a dataframe to label all the columns returned by binance so we work with them later.
        df = pd.DataFrame(candle, columns=['dateTime', 'open', 'high', 'low', 'close', 'volume', 'closeTime', 'quoteAssetVolume', 'tradecount', 'volume_btc', 'volume_usdt', 'ignore'])

        # as timestamp is returned in ms, let us convert this back to proper timestamps.
        df.dateTime = pd.to_datetime(df.dateTime, unit='ms').dt.strftime("%Y_%m_%d %H:%M:%S")
        df.set_index('dateTime', inplace=True)

        # Get rid of columns we do not need
        df = df.drop(['quoteAssetVolume', 'closeTime', 'ignore', 'volume'], axis=1)

        return df

    def get_staking_data(self):
        request = self.client._request_margin_api('get', 'staking/productList', True, data={'product': 'STAKING'})
        all_token = [{"token": data["projectId"].split("*")[0], "staking":[{"time":data["projectId"].split("*")[1],  "apy": data["detail"]["apy"]}]} for data in request]
        all_token = sorted(all_token, key=lambda x: x["token"], reverse=True)
        if len(all_token) == 0:
            return []

        result = [all_token[0]]

        for i in range(len(all_token)-1):
            if all_token[i]["token"] != all_token[i+1]["token"]:
                result.append(all_token[i+1].copy())
            else:
                result[-1]["staking"].append(all_token[i+1]["staking"][0])

        return result

    def close(self):
        self.client.close_connection()

    def save_to_db(self):
        pass


if __name__ == "__main__":
    client = BinanceClient()
    a = datetime.datetime.strptime('2022/01/01', "%Y/%m/%d")
    b = datetime.datetime.utcnow().replace(minute=0, second=0)
    delta = (b - a).days + 1
    df = client.get_historical_data(delta, time_range=Client.KLINE_INTERVAL_1DAY)["close"].reset_index()
    df["dateTime"] = pd.to_datetime(df["dateTime"], format="%Y_%m_%d %H:%M:%S")
    df["dateTime"] = df["dateTime"].dt.strftime("%Y%m%d")
    df = df.set_index("dateTime")
    print(df.to_dict()["close"])
