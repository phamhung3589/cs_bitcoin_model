import json
import requests
import collections
import pandas as pd
from typing import Union, Optional
from binance.client import Client

import uvicorn

from fastapi import FastAPI, Depends, Query, Header, Security, Response, Cookie
from fastapi.security import OAuth2PasswordBearer, HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel
from datetime import datetime

# from cs_bitcoin_model.crypto.aggregate.btc_prediction.binance_client import BinanceClient
# from cs_bitcoin_model.crypto.aggregate.btc_prediction.worker_predict_btc_price import BtcPredict
# from cs_bitcoin_model.crypto.aggregate.defi.staking import get_all_staking_pair
from cs_bitcoin_model.crypto.importdb.access_token.sql_access_token import SqlAccessToken
from cs_bitcoin_model.crypto.importdb.btc_prediction.sql_btc_prediction import SqlBtcPrediction
from cs_bitcoin_model.crypto.importdb.crypto_quant.sql_cryptoquant_indicator import SqlCryptoQuantRawValue
from cs_bitcoin_model.crypto.importdb.sql_connection import SqlConnector
from datetime import datetime, date, timedelta
from fastapi.middleware.httpsredirect import HTTPSRedirectMiddleware
from fastapi.middleware.cors import CORSMiddleware

from cs_bitcoin_model.crypto.importdb.sql_twitter_connection import SqlTwitterConnection
from cs_bitcoin_model.crypto.importdb.twitter.sql_twiiter_save_tweets import SqlTwitterSaveTweetsConnection
from cs_bitcoin_model.crypto.importdb.twitter.sql_twitter_advanced_filter_daily import SqlTwitterAdvanceFilterDailyConnection
from cs_bitcoin_model.crypto.importdb.twitter.sql_twitter_advanced_filter_weekly import SqlTwitterAdvanceFilterWeeklyConnection
from cs_bitcoin_model.crypto.importdb.twitter.sql_twitter_recent_search import SqlTwitterRecentSearchConnection
from cs_bitcoin_model.crypto.importdb.twitter.sql_twitter_search_week import SqlTwitterSearchWeekConnection
from cs_bitcoin_model.crypto.utils import constants
from cs_bitcoin_model.crypto.utils.db_utils import decode_token, calculate_sloth_index

app = FastAPI(reload=True)

origins = [
    "https://data.cryptosloth.io",
    "http://data.cryptosloth.io",
    "https://data.cryptosloth.io:2096",
    "https://cryptosloth.io/on-chain",
    "https://cryptosloth.io",
    "https://localhost:3000",
    "https://localhost"
]

app.add_middleware(HTTPSRedirectMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

cryptoquant_indicator_daily = {}
cryptoquant_indicator_hist = {}
sloth_index = {}
fear_and_greed = {}
twitter_search = {}
twitter_advanced_filter = {}
twitter_recent_count = {}
twitter_trending_tag = {}
btc_prediction_result = {}
btc_prediction_history = {}
quant_indicator_raw_values = {}
staking = {}

# Load model for btc client
# btc_client = BtcPredict()

# oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


class User(BaseModel):
    user_id: int
    datetime: Union[int, None] = None
    list_save_tweets: Union[list, None] = None


def get_authorization_token(auth: HTTPAuthorizationCredentials = Security(HTTPBearer())):
    return auth.credentials


# @app.get("/crypto_indicators/daily", tags=["crypto indicator"])
# async def get_indicators():
#     global cryptoquant_indicator_daily
#     running_date = date.today() - timedelta(days=1)
#     if running_date in cryptoquant_indicator_daily:
#         return cryptoquant_indicator_daily[running_date]
#
#     sqlConnector = SqlConnector()
#     result = sqlConnector.get_data_from_db(running_date.strftime("%Y-%m-%d"))
#
#     sql_client_quant = SqlCryptoQuantRawValue()
#     result_raw = sql_client_quant.get_all_data("20220101")
#     sql_client_quant.close()
#
#     for i in range(len(result["data"])):
#         result["data"][i]["chart"] = result_raw[result["data"][i]["name"]]
#
#     # Get history data of btc
#     client = BinanceClient()
#     start_date_btc_price = datetime.strptime('2022/01/01', "%Y/%m/%d")
#     current_date_btc_price = datetime.utcnow().replace(minute=0, second=0)
#     delta = (current_date_btc_price - start_date_btc_price).days + 1
#     df = client.get_historical_data(delta, time_range=Client.KLINE_INTERVAL_1DAY)["close"].reset_index()
#     df["dateTime"] = pd.to_datetime(df["dateTime"], format="%Y_%m_%d %H:%M:%S")
#     df["dateTime"] = df["dateTime"].dt.strftime("%Y%m%d")
#     df = df.set_index("dateTime")
#     result["btc_price_hist"] = df.to_dict()["close"]
#     client.close()
#
#     cryptoquant_indicator_daily = {running_date: result}
#
#     return result


@app.get("/crypto_indicators/{running_date}", tags=["crypto indicator"])
async def get_indicators(running_date: str):
    global cryptoquant_indicator_hist
    running_date = datetime.strptime(running_date, "%Y-%m-%d")
    if running_date in cryptoquant_indicator_hist:
        return cryptoquant_indicator_hist[running_date]

    sqlConnector = SqlConnector()
    try:
        result = sqlConnector.get_data_from_db(running_date)
        cryptoquant_indicator_hist[running_date] = result
        return result
    except ValueError:
        return {"Incorrect data format, should be YYYY-MM-DD"}


@app.get("/indicator_raw_value/{indicator_name}", tags=["crypto indicator"], description="indicator name must be in this list: ind_mpi, ind_exchange_netflow, ind_stablecoins_ratio_usd, ind_leverage_ratio, ind_sopr, ind_sopr_holders, ind_mvrv,ind_nupl, ind_nvt_golden_cross, ind_puell_multiple, ind_utxo,ind_long_liquidation, ind_short_liquidation")
async def get_indicator_name(indicator_name: str):
    if indicator_name not in constants.list_indicators_name:
        return {"error": "indicator has been not supported"}

    current_time = datetime.fromtimestamp(datetime.utcnow().timestamp() - 24*60*60).replace(minute=0, second=0)
    current_time = current_time.strftime("%Y%m%d")

    if indicator_name in quant_indicator_raw_values and current_time in quant_indicator_raw_values[indicator_name]:
        return quant_indicator_raw_values[indicator_name]

    sql_client = SqlCryptoQuantRawValue()

    if indicator_name not in quant_indicator_raw_values:
        result = sql_client.get_multi_data_from_db(current_time, indicator_name)
        quant_indicator_raw_values[indicator_name] = {}
        quant_indicator_raw_values[indicator_name] = result
    else:
        result = sql_client.get_data_from_db(current_time, indicator_name)
        if result != {}:
            quant_indicator_raw_values[indicator_name][current_time] = result["indicator_value"]
    sql_client.close()

    return quant_indicator_raw_values[indicator_name]


@app.get("/onchain/sloth_index", tags=["crypto indicator"], description="sloth index equal sum of all onchain indicators that defined in api /crypto_indicators/daily, with type = 1-5 (Risk - Excellent) and coeff = 1-1.5-2(M-H-VH), after that divide to number of indicators. The values of sloth index running from 19/13(1.46) -> 95/13(~7.31)")
async def get_sloth_index():
    global sloth_index
    running_date = date.today() - timedelta(days=1)
    if running_date in sloth_index:
        return sloth_index[running_date]

    sqlConnector = SqlConnector()
    indicator_values = sqlConnector.get_data_from_db(running_date.strftime("%Y-%m-%d"))["data"]

    sloth_tmp_value = 0
    for indicator in indicator_values:
        sloth_tmp_value += calculate_sloth_index(indicator["type"], indicator["coeff"])
    sloth_tmp_value = sloth_tmp_value / len(indicator_values)

    sloth_index_value = {"sloth_index": sloth_tmp_value}

    sloth_index = {running_date: sloth_index_value}
    return sloth_index_value


@app.get("/onchain/fear_and_greed_index", tags=["crypto indicator"], description="must assign source from https://alternative.me/crypto/fear-and-greed-index/")
async def fear_and_greed_index():
    global fear_and_greed
    running_time = datetime.utcnow().replace(minute=0, second=0, hour=0).strftime("%Y_%m_%d")
    if running_time in fear_and_greed:
        return fear_and_greed[running_time]

    url = "https://api.alternative.me/fng/?limit=1"
    response = requests.get(url)
    if response.status_code == 200:
        data = json.loads(response.text)["data"][0]

        fear_and_greed = {running_time: data}
        return data
    else:
        raise ValueError('cannot get data from https://alternative.me/')


# @app.get("/token/prediction/{token_pair}", tags=["token_prediction"], description="token_pair default = btcusdt")
# async def predict_btc_price(token_pair: str):
#     if token_pair == "btcusdt":
#         global btc_prediction_result
#         current_time = datetime.utcnow().replace(minute=0, second=0)
#         current_time = current_time.strftime("%Y_%m_%d %H:%M:%S")
#
#         if current_time in btc_prediction_result:
#             return btc_prediction_result[current_time]
#
#         result = btc_client.predict(current_time)
#         btc_prediction_result = {current_time: result}
#
#         return result
#     else:
#         raise ValueError("error value token pair")


@app.get("/token/prediction_history/{token_pair}", tags=["token_prediction"], description="token_pair default = btcusdt")
async def predict_btc_price_history(token_pair: str):
    if token_pair == "btcusdt":
        global btc_prediction_history

        current_time = datetime.fromtimestamp(datetime.utcnow().timestamp() - 60 * 60).replace(minute=0, second=0)

        if current_time in btc_prediction_history:
            return btc_prediction_history[current_time]

        sql_client = SqlBtcPrediction()
        result = sql_client.get_all_data(current_time)

        btc_prediction_history = {current_time: result}

        return result
    else:
        raise ValueError("error value token pair")


@app.get("/tweets/search/aggregate_twitter_search", tags=["twitter"])
async def get_twitter_search():
    global twitter_search

    current_time = datetime.fromtimestamp(datetime.utcnow().timestamp() - 1 * 60).replace(minute=0, second=0).strftime("%Y-%m-%dT%H:%M:%S")
    if current_time in twitter_search:
        return twitter_search[current_time]

    # Search daily
    sqlTwitter = SqlTwitterRecentSearchConnection()
    search_daily = sqlTwitter.get_data_from_db(current_time)

    # search weekly
    sqlTwitter = SqlTwitterSearchWeekConnection()
    search_weekly = sqlTwitter.get_data_from_db(current_time)

    result = {"daily_search": search_daily, "weekly_search": search_weekly}
    twitter_search = {current_time: result}

    return result


@app.get("/aggregate_twitter_chain_count", tags=["twitter"])
async def get_twitter_chain_count():
    global twitter_recent_count
    current_time = datetime.fromtimestamp(datetime.utcnow().timestamp() - 60 * 60).replace(minute=0, second=0).strftime("%Y-%m-%dT%H:%M:%S")
    if current_time in twitter_recent_count:
        return twitter_recent_count[current_time]

    sqlTwitter = SqlTwitterConnection()
    result = sqlTwitter.get_multi_data_from_db(current_time, 25)
    twitter_recent_count = {current_time: result}
    sqlTwitter.close()

    return result


@app.get("/tweets/advanced_filter/all_tweets", tags=["twitter"])
async def get_twitter_advanced_filter():
    global twitter_advanced_filter
    current_time = datetime.fromtimestamp(datetime.utcnow().timestamp() - 60 * 60).replace(minute=0, second=0).strftime("%Y-%m-%dT%H:%M:%S")
    if current_time in twitter_advanced_filter:
        return twitter_advanced_filter[current_time]

    # daily search
    sqlTwitter = SqlTwitterAdvanceFilterDailyConnection()
    daily_search = sqlTwitter.get_data_from_db(current_time)
    sqlTwitter.close()

    # weekly search
    sqlTwitter = SqlTwitterAdvanceFilterWeeklyConnection()
    weekly_search = sqlTwitter.get_data_from_db(current_time)
    sqlTwitter.close()

    result = {"daily_filter": daily_search, "weekly_filter": weekly_search}
    twitter_advanced_filter = {current_time: result}

    return result


@app.get("/tweets/trending_tag", tags=["twitter"])
async def get_twitter_trending_tag():
    global twitter_trending_tag
    current_time = datetime.fromtimestamp(datetime.utcnow().timestamp() - 60 * 60).replace(minute=0, second=0).strftime("%Y-%m-%dT%H:%M:%S")
    if current_time in twitter_trending_tag:
        return twitter_trending_tag[current_time]

    all_tags = []
    sqlTwitter_week = SqlTwitterSearchWeekConnection()
    sqlTwitter_day = SqlTwitterRecentSearchConnection()
    result_week = sqlTwitter_week.get_data_from_db(current_time)
    result_day = sqlTwitter_day.get_data_from_db(current_time)
    sqlTwitter_week.close()
    sqlTwitter_day.close()

    for result in (result_day, result_week):
        for chain in (result["eth_tag"], result["sol_tag"], result["bnb_tag"]):
            for list_tag in chain:
                if list_tag != [""]:
                    all_tags += list_tag

    tag_requency = dict(collections.Counter(all_tags))
    tag_requency = dict(sorted(tag_requency.items(), key=lambda x: x[1], reverse=True))
    result = {"list_tag": tag_requency}
    twitter_trending_tag = {current_time: result}

    return result


def check_access_token(token: str):
    sqlAccessToken = SqlAccessToken()
    check_token = sqlAccessToken.check_data_from_db(token)
    sqlAccessToken.close()

    return check_token


def check_list_tweets(tweets, check_delete=True):
    global twitter_search
    if "daily_search" in twitter_search:
        twitter_recent_search = twitter_search["daily_search"]
    else:
        twitter_recent_search = {}

    if "weekly_search" in twitter_search:
        twitter_weekly_search = twitter_search["weekly_search"]
    else:
        twitter_weekly_search = {}

    if tweets == "":
        return False

    # Get all current tweets on website
    if check_delete:
        all_current_tweets = tweets
    else:
        all_current_tweets = set()
        current_time = datetime.fromtimestamp(datetime.utcnow().timestamp() - 1 * 60).replace(minute=0, second=0).strftime(
            "%Y-%m-%dT%H:%M:%S")
        if current_time in twitter_weekly_search:
            all_current_tweets = all_current_tweets.union(set(twitter_weekly_search[current_time]["eth_chain"] +
                                     twitter_weekly_search[current_time]["sol_chain"] +
                                     twitter_weekly_search[current_time]["bnb_chain"]))

        if current_time in twitter_recent_search:
            all_current_tweets = all_current_tweets.union(set(twitter_recent_search[current_time]["eth_chain"] +
                                     twitter_recent_search[current_time]["sol_chain"] +
                                     twitter_recent_search[current_time]["bnb_chain"]))

    # Check if list_tweets only have 1 tweets
    if tweets.isnumeric():
        if tweets in all_current_tweets:
            return True
        else:
            return False

    # Check case list tweets have many tweets
    if "," in tweets:
        all_tweets = tweets.split(",")
        # If only 1 tweet is not numeric -> return False
        for tweet in all_tweets:
            if (not tweet.isnumeric()) or (tweet not in all_current_tweets):
                return False
        # if all tweet is numeric -> return True
        return True

    # if tweet is not numeric and dont have comma in string -> return False
    return False


def get_current_user(token: str):
    check_token = check_access_token(token)

    if check_token:
        user_id = decode_token(token)
        sqlSaveTweets = SqlTwitterSaveTweetsConnection()
        user_data = sqlSaveTweets.get_data_from_db(user_id)
        sqlSaveTweets.close()

        return user_data

    return {"error": "token's not exist in db"}


@app.get("/tweets/bookmark/get_all_save_tweets", tags=["twitter"])
async def get_save_tweets(response: Response,
                          authen_token: Optional[str] = Cookie(None),
                          access_token=Depends(get_authorization_token)):
    if authen_token:
        return json.loads(authen_token.replace("'", "\""))

    result = get_current_user(access_token)
    if "error" not in result:
        result["datetime"] = result["datetime"].strftime("%Y-%m-%dT%H:%M:%S")
        response.set_cookie(key='authen_token', value=result, httponly=True)

    return result


@app.post("/tweets/bookmark/save_tweets", tags=["twitter"])
async def save_tweets(list_tweets, response: Response, access_token=Depends(get_authorization_token)):
    check_token = check_access_token(access_token)
    check_tweet = check_list_tweets(list_tweets)

    if not check_tweet:
        raise ValueError('list tweet is not in correct format, please update list_tweets in format: tweet_id_1,tweet_id_2,... or ... Do you wanna hack in my server ?')

    if not check_token:
        raise ValueError('token is not in database... spam user')

    user_id = decode_token(access_token)
    sqlSaveTweets = SqlTwitterSaveTweetsConnection()
    data = {"datetime": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
            "user_id": user_id,
            "list_save_tweets": list_tweets}

    data_cookies = sqlSaveTweets.insert_data(data)
    # Save to cookies
    response.set_cookie(key='authen_token', value=data_cookies, httponly=True)
    sqlSaveTweets.close()

    return {"status": "insert done"}


@app.delete("/tweets/bookmark/{delete_tweets}", tags=["twitter"])
async def delete_tweets(delete_tweets, response: Response, access_token=Depends(get_authorization_token)):
    check_token = check_access_token(access_token)
    check_tweet = check_list_tweets(delete_tweets)

    if not check_tweet:
        raise ValueError('list tweet is not in correct format, please update list_tweets in format: tweet_id_1,tweet_id_2,...')

    if not check_token:
        raise ValueError('token is not in database... spam user')

    user_id = decode_token(access_token)
    sqlSaveTweets = SqlTwitterSaveTweetsConnection()

    delete_tweets = [x for x in delete_tweets.split(",")]
    # Save to cookies
    data_cookies = sqlSaveTweets.delete_tweets(user_id, delete_tweets)
    if data_cookies != "":
        data_cookies["datetime"] = data_cookies["datetime"].strftime("%Y-%m-%dT%H:%M:%S")
    response.set_cookie(key='authen_token', value=data_cookies, httponly=True)
    sqlSaveTweets.close()

    return {"status": "delete done"}


# @app.get("/staking/all_pairs", tags=["staking-farming"])
# async def get_staking_apy():
#     global staking
#     current_time = datetime.utcnow().replace(minute=0, second=0).strftime("%Y-%m-%dT%H:%M:%S")
#     if current_time in staking:
#         return staking[current_time]
#
#     result = get_all_staking_pair()
#     staking = {current_time: result}
#
#     return result


if __name__ == '__main__':
    uvicorn.run(app=app,
                host="127.0.0.1",
                port=2096,
                log_level="info",
                reload=True,
                ssl_keyfile="./key.pem",
                ssl_certfile="./cert.pem")
