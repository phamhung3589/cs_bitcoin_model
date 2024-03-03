import requests
import json
from datetime import datetime, date

from cs_bitcoin_model.crypto.utils.cs_config import CSConfig


class TwitterRecentSearch:
    def __init__(self):
        parser = CSConfig("private_product", self.__class__.__name__, "config")
        self.bearer_token = parser.read_parameter("bearer_token")
        self.search_url = parser.read_parameter("recent_search_url")

    def bearer_oauth(self, r):
        """
        Method required by bearer token authentication.
        """

        r.headers["Authorization"] = f"Bearer {self.bearer_token}"
        r.headers["User-Agent"] = "v2RecentSearchPython"
        return r

    def connect_to_endpoint(self, url, params):
        response = requests.get(url, auth=self.bearer_oauth, params=params)
        if response.status_code != 200:
            raise Exception(response.status_code, response.text)
        return response.json()

    def search(self, query_params, num_action=20):
        json_response = self.connect_to_endpoint(self.search_url, query_params)
        data_content = json.loads(json.dumps(json_response, indent=4, sort_keys=True))["data"]
        includes = json.loads(json.dumps(json_response, indent=4, sort_keys=True))["includes"]["users"]

        result = []
        for data, user_info in zip(data_content, includes):
            data["username"] = user_info["username"]
            result.append(data)

        result = sorted(result, key=lambda x: sum(x["public_metrics"].values()), reverse=True)
        result = [data for data in result if sum(data["public_metrics"].values()) > num_action]
        username = [x["username"] for x in result]
        times = [x["created_at"] for x in result]
        contents = [x["text"] for x in result]
        all_tags = []
        for content in contents:
            content = content.replace("\n", " ").replace(u'\xa0', u' ').replace(",", " ").replace(".", " ")
            content = content.split(" ")
            content = list(set(x for x in content if "#" in x or "@" in x))
            all_tags.append(content)

        result = ["https://twitter.com/" + data["username"] + "/status/" + data["id"] for data in result]

        return result, username, all_tags, times


def get_query_sol(start_time: str, end_time: str):
    # Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
    # expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields
    query = {'query': '(#solana OR #sol OR solana) -is:retweet lang:en',
                    "user.fields": "username",
                    "expansions": "author_id",
                    "tweet.fields": 'author_id,public_metrics,created_at,withheld',
                    "start_time": start_time,
                    "end_time": end_time,
                    "sort_order": "relevancy",
                    "max_results": "100"}

    return query


def get_query_eth(start_time: str, end_time: str):
    # Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
    # expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields
    query = {'query': '(#ethereum OR #eth OR ethereum) -is:retweet lang:en',
                    "user.fields": "username",
                    "expansions": "author_id",
                    "tweet.fields": 'author_id,public_metrics,created_at,withheld',
                    "start_time": start_time,
                    "end_time": end_time,
                    "sort_order": "relevancy",
                    "max_results": "100"}

    return query


def get_query_bnb(start_time: str, end_time: str):
    # Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
    # expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields
    query = {'query': '(#bnb OR bnb) -is:retweet lang:en',
                    "user.fields": "username",
                    "expansions": "author_id",
                    "tweet.fields": 'author_id,public_metrics,created_at,withheld',
                    "start_time": start_time,
                    "end_time": end_time,
                    "sort_order": "relevancy",
                    "max_results": "100"}

    return query


def get_query_nft(start_time: str, end_time: str):
    # Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
    # expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields
    query = {'query': '(#nft OR #NFTGiveaway OR #NFTdrop OR #NFTGame OR #NFTart OR NFTMarketplaceOR #Metaverse OR #NFTs) -is:retweet lang:en',
                    "user.fields": "username",
                    "expansions": "author_id",
                    "tweet.fields": 'author_id,public_metrics,created_at,withheld',
                    "start_time": start_time,
                    "end_time": end_time,
                    "sort_order": "relevancy",
                    "max_results": "100"}

    return query


def get_query_defi(start_time: str, end_time: str):
    # Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
    # expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields
    query = {'query': '(#DeFi OR DeFi) -is:retweet lang:en',
                    "user.fields": "username",
                    "expansions": "author_id",
                    "tweet.fields": 'author_id,public_metrics,created_at,withheld',
                    "start_time": start_time,
                    "end_time": end_time,
                    "sort_order": "relevancy",
                    "max_results": "100"}

    return query


def get_query_web3(start_time: str, end_time: str):
    # Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
    # expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields
    query = {'query': '(#Web3 OR Web3) -is:retweet lang:en',
                    "user.fields": "username",
                    "expansions": "author_id",
                    "tweet.fields": 'author_id,public_metrics,created_at,withheld',
                    "start_time": start_time,
                    "end_time": end_time,
                    "sort_order": "relevancy",
                    "max_results": "100"}

    return query


def get_query_ico(start_time: str, end_time: str):
    # Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
    # expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields
    query = {'query': '(#IDO OR #ICO OR #IEO OR IDO OR ICO OR IEO) -is:retweet lang:en',
                    "user.fields": "username",
                    "expansions": "author_id",
                    "tweet.fields": 'author_id,public_metrics,created_at,withheld',
                    "start_time": start_time,
                    "end_time": end_time,
                    "sort_order": "relevancy",
                    "max_results": "100"}

    return query


def get_tweets_recent_search(date: datetime):
    recent_search = TwitterRecentSearch()
    current_time = datetime.fromtimestamp(date.timestamp() - 1 * 60).strftime("%Y-%m-%dT%H:%M:%SZ")
    base_time = datetime.fromtimestamp(date.timestamp() - 6 * 60 * 60).strftime("%Y-%m-%dT%H:%M:%SZ")

    query_sol = get_query_sol(base_time, current_time)
    query_eth = get_query_eth(base_time, current_time)
    query_bnb = get_query_bnb(base_time, current_time)

    link_sol, username_sol, all_tags_sol, times_sol = recent_search.search(query_sol, num_action=20)
    link_eth, username_eth, all_tags_eth, times_eth = recent_search.search(query_eth, num_action=20)
    link_bnb, username_bnb, all_tags_bnb, times_bnb = recent_search.search(query_bnb, num_action=20)

    link_sol = ",".join(link_sol)
    link_eth = ",".join(link_eth)
    link_bnb = ",".join(link_bnb)

    username_sol = ",".join(username_sol)
    username_eth = ",".join(username_eth)
    username_bnb = ",".join(username_bnb)

    all_tags_sol = "|".join([",".join(x) for x in all_tags_sol])
    all_tags_eth = "|".join([",".join(x) for x in all_tags_eth])
    all_tags_bnb = "|".join([",".join(x) for x in all_tags_bnb])

    times_sol = ",".join(times_sol)
    times_eth = ",".join(times_eth)
    times_bnb = ",".join(times_bnb)

    data_sol = {"link": link_sol, "username": username_sol, "tag": all_tags_sol, "times": times_sol}
    data_eth = {"link": link_eth, "username": username_eth, "tag": all_tags_eth, "times": times_eth}
    data_bnb = {"link": link_bnb, "username": username_bnb, "tag": all_tags_bnb, "times": times_bnb}

    return data_eth, data_sol, data_bnb


def get_tweets_search_7_days(date: datetime):
    recent_search = TwitterRecentSearch()
    current_time = datetime.fromtimestamp(date.timestamp() - 1 * 60).strftime("%Y-%m-%dT%H:%M:%SZ")
    base_time = datetime.fromtimestamp(date.timestamp() - 6 * 24 * 60 * 60).strftime("%Y-%m-%dT%H:%M:%SZ")

    query_sol = get_query_sol(base_time, current_time)
    query_eth = get_query_eth(base_time, current_time)
    query_bnb = get_query_bnb(base_time, current_time)

    link_sol, username_sol, all_tags_sol, times_sol = recent_search.search(query_sol, num_action=200)
    link_eth, username_eth, all_tags_eth, times_eth = recent_search.search(query_eth, num_action=200)
    link_bnb, username_bnb, all_tags_bnb, times_bnb = recent_search.search(query_bnb, num_action=200)

    link_sol = ",".join(link_sol)
    link_eth = ",".join(link_eth)
    link_bnb = ",".join(link_bnb)

    username_sol = ",".join(username_sol)
    username_eth = ",".join(username_eth)
    username_bnb = ",".join(username_bnb)

    all_tags_sol = "|".join([",".join(x) for x in all_tags_sol])
    all_tags_eth = "|".join([",".join(x) for x in all_tags_eth])
    all_tags_bnb = "|".join([",".join(x) for x in all_tags_bnb])

    times_sol = ",".join(times_sol)
    times_eth = ",".join(times_eth)
    times_bnb = ",".join(times_bnb)

    data_sol = {"link": link_sol, "username": username_sol, "tag": all_tags_sol, "times": times_sol}
    data_eth = {"link": link_eth, "username": username_eth, "tag": all_tags_eth, "times": times_eth}
    data_bnb = {"link": link_bnb, "username": username_bnb, "tag": all_tags_bnb, "times": times_bnb}

    return data_eth, data_sol, data_bnb


def get_advanced_filter_tweets_daily(date: datetime):
    recent_search = TwitterRecentSearch()
    current_time = datetime.fromtimestamp(date.timestamp() - 1 * 60).strftime("%Y-%m-%dT%H:%M:%SZ")
    base_time = datetime.fromtimestamp(date.timestamp() - 24 * 60 * 60).strftime("%Y-%m-%dT%H:%M:%SZ")

    query_nft = get_query_nft(base_time, current_time)
    query_defi = get_query_defi(base_time, current_time)
    query_web3 = get_query_web3(base_time, current_time)
    query_ico = get_query_ico(base_time, current_time)

    link_nft, username_nft, all_tags_nft, times_nft = recent_search.search(query_nft, num_action=20)
    link_defi, username_defi, all_tags_defi, times_defi = recent_search.search(query_defi, num_action=20)
    link_web3, username_web3, all_tags_web3, times_web3 = recent_search.search(query_web3, num_action=20)
    link_ico, username_ico, all_tags_ico, times_ico = recent_search.search(query_ico, num_action=20)

    link_nft = ",".join(link_nft)
    link_defi = ",".join(link_defi)
    link_web3 = ",".join(link_web3)
    link_ico = ",".join(link_ico)

    username_nft = ",".join(username_nft)
    username_defi = ",".join(username_defi)
    username_web3 = ",".join(username_web3)
    username_ico = ",".join(username_ico)

    all_tags_nft = "|".join([",".join(x) for x in all_tags_nft])
    all_tags_defi = "|".join([",".join(x) for x in all_tags_defi])
    all_tags_web3 = "|".join([",".join(x) for x in all_tags_web3])
    all_tags_ico = "|".join([",".join(x) for x in all_tags_ico])

    times_nft = ",".join(times_nft)
    times_defi = ",".join(times_defi)
    times_web3 = ",".join(times_web3)
    times_ico = ",".join(times_ico)

    data_nft = {"link": link_nft, "username": username_nft, "tag": all_tags_nft, "times": times_nft}
    data_defi = {"link": link_defi, "username": username_defi, "tag": all_tags_defi, "times": times_defi}
    data_web3 = {"link": link_web3, "username": username_web3, "tag": all_tags_web3, "times": times_web3}
    data_ico = {"link": link_ico, "username": username_ico, "tag": all_tags_ico, "times": times_ico}

    return data_nft, data_defi, data_web3, data_ico


def get_advanced_filter_tweets_weekly(date: datetime):
    recent_search = TwitterRecentSearch()
    current_time = datetime.fromtimestamp(date.timestamp() - 1 * 60).strftime("%Y-%m-%dT%H:%M:%SZ")
    base_time = datetime.fromtimestamp(date.timestamp() - 6 * 26 * 60 * 60).strftime("%Y-%m-%dT%H:%M:%SZ")

    query_nft = get_query_nft(base_time, current_time)
    query_defi = get_query_defi(base_time, current_time)
    query_web3 = get_query_web3(base_time, current_time)
    query_ico = get_query_ico(base_time, current_time)

    link_nft, username_nft, all_tags_nft, times_nft = recent_search.search(query_nft, num_action=200)
    link_defi, username_defi, all_tags_defi, times_defi = recent_search.search(query_defi, num_action=200)
    link_web3, username_web3, all_tags_web3, times_web3 = recent_search.search(query_web3, num_action=200)
    link_ico, username_ico, all_tags_ico, times_ico = recent_search.search(query_ico, num_action=200)

    link_nft = ",".join(link_nft)
    link_defi = ",".join(link_defi)
    link_web3 = ",".join(link_web3)
    link_ico = ",".join(link_ico)

    username_nft = ",".join(username_nft)
    username_defi = ",".join(username_defi)
    username_web3 = ",".join(username_web3)
    username_ico = ",".join(username_ico)

    all_tags_nft = "|".join([",".join(x) for x in all_tags_nft])
    all_tags_defi = "|".join([",".join(x) for x in all_tags_defi])
    all_tags_web3 = "|".join([",".join(x) for x in all_tags_web3])
    all_tags_ico = "|".join([",".join(x) for x in all_tags_ico])

    times_nft = ",".join(times_nft)
    times_defi = ",".join(times_defi)
    times_web3 = ",".join(times_web3)
    times_ico = ",".join(times_ico)

    data_nft = {"link": link_nft, "username": username_nft, "tag": all_tags_nft, "times": times_nft}
    data_defi = {"link": link_defi, "username": username_defi, "tag": all_tags_defi, "times": times_defi}
    data_web3 = {"link": link_web3, "username": username_web3, "tag": all_tags_web3, "times": times_web3}
    data_ico = {"link": link_ico, "username": username_ico, "tag": all_tags_ico, "times": times_ico}

    return data_nft, data_defi, data_web3, data_ico


if __name__ == "__main__":
    recent_search = TwitterRecentSearch()

    now = datetime.utcnow()
    current_time = datetime.fromtimestamp(now.timestamp() - 1 * 60).strftime("%Y-%m-%dT%H:%M:%SZ")
    base_time = datetime.fromtimestamp(now.timestamp() - 6 * 60 * 60).strftime("%Y-%m-%dT%H:%M:%SZ")

    query = get_query_bnb(base_time, current_time)
    data = recent_search.search(query)

    # data_eth, data_sol, data_bnb = get_tweets_search_7_days(now)

    # print(data_bnb["link"])
    # print(data_bnb["username"])
    # print(data_bnb["tag"].split("|"))

    # import collections
    # all_tag = []
    # for chain in (data_eth, data_sol, data_bnb):
    #     for list_tag in chain["tag"].split("|"):
    #         if list_tag != "":
    #             all_tag += list_tag.split(",")
    #
    # count = dict(collections.Counter(all_tag))
    # print(dict(sorted(count.items(), key=lambda x: x[1], reverse=True)))
    # print(all_tag)

    # data_nft, data_defi, data_web3, data_ico = get_advanced_filter_tweets_daily(now)
    # print(data_nft)
    # print(data_defi)
    # print(data_web3)
    # print(data_ico)
