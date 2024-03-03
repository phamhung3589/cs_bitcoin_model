import click

from datetime import datetime, timedelta

from cs_bitcoin_model.crypto.aggregate.social.twitter_recent_search import get_tweets_recent_search
from cs_bitcoin_model.crypto.importdb.twitter.sql_twitter_recent_search import SqlTwitterRecentSearchConnection


@click.command()
@click.option('--date', default='2022_03_05T07:00:00', required=True)
def run(date):
    """
    :return: import data to sql server
    """
    date = datetime.strptime(date, "%Y_%m_%dT%H:%M:%S")
    data_eth, data_sol, data_bnb = get_tweets_recent_search(date)
    sqlTwitter = SqlTwitterRecentSearchConnection()
    data = {"datetime": date,
            "eth_chain": data_eth["link"],
            "sol_chain": data_sol["link"],
            "bnb_chain": data_bnb["link"],
            "eth_username": data_eth["username"],
            "sol_username": data_sol["username"],
            "bnb_username": data_bnb["username"],
            "eth_created": data_eth["times"],
            "sol_created": data_sol["times"],
            "bnb_created": data_bnb["times"],
            "eth_tag": data_eth["tag"],
            "sol_tag": data_sol["tag"],
            "bnb_tag": data_bnb["tag"]
            }

    sqlTwitter.insert_data(data)
    sqlTwitter.delete_many(date - timedelta(days=2))
    sqlTwitter.close()


if __name__ == "__main__":
    run()
