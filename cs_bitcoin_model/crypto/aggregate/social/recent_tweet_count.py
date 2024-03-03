import requests
import json
from datetime import datetime

# To set your environment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
from cs_bitcoin_model.crypto.utils.cs_config import CSConfig


class TwitterSearchCount:
    def __init__(self):
        parser = CSConfig("private_product", self.__class__.__name__, "config")
        self.bearer_token = parser.read_parameter("bearer_token")
        self.search_url = parser.read_parameter("search_count_url")

    def bearer_oauth(self, r):
        """
        Method required by bearer token authentication.
        """

        r.headers["Authorization"] = f"Bearer {self.bearer_token}"
        r.headers["User-Agent"] = "v2RecentTweetCountsPython"
        return r

    def connect_to_endpoint(self, url, params):
        response = requests.request("GET", url, auth=self.bearer_oauth, params=params)
        if response.status_code != 200:
            raise Exception(response.status_code, response.text)
        return response.json()

    def search_count(self, query_params):
        json_response = self.connect_to_endpoint(self.search_url, query_params)
        data = json.loads(json.dumps(json_response, indent=4, sort_keys=True))["meta"]

        return data["total_tweet_count"]


def get_query_count_sol(start_time: str, end_time: str):
    # Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
    query_sol = {'query': '(#solana OR #sol OR solana) lang:en',
                    "start_time": start_time,
                    "end_time": end_time}

    return query_sol


def get_query_count_eth(start_time: str, end_time: str):
    # Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
    query_eth = {'query': '(#ethereum OR #eth OR ethereum) lang:en',
                    "start_time": start_time,
                    "end_time": end_time}

    return query_eth


def get_query_count_bnb(start_time: str, end_time: str):
    # Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
    query_bnb = {'query': '(#bnb OR #bnbchain OR bnbchain) lang:en',
                    "start_time": start_time,
                    "end_time": end_time}

    return query_bnb


def run(date: datetime):
    recent_search = TwitterSearchCount()
    current_time = datetime.fromtimestamp(date.timestamp() - 1 * 60).strftime("%Y-%m-%dT%H:%M:%SZ")
    base_time = datetime.fromtimestamp(date.timestamp() - 60 * 60).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Create query
    query_eth = get_query_count_eth(base_time, current_time)
    query_sol = get_query_count_sol(base_time, current_time)
    query_bnb = get_query_count_bnb(base_time, current_time)

    # get from twitter api
    total_tweet_count_eth = recent_search.search_count(query_eth)
    total_tweet_count_sol = recent_search.search_count(query_sol)
    total_tweet_count_bnb = recent_search.search_count(query_bnb)

    # print("total_tweet_count_eth: ", total_tweet_count_eth)
    # print("total_tweet_count_sol: ", total_tweet_count_sol)
    # print("total_tweet_count_bnb: ", total_tweet_count_bnb)

    return total_tweet_count_eth, total_tweet_count_sol, total_tweet_count_bnb


if __name__ == "__main__":
    # run(datetime.utcnow())
    pass
