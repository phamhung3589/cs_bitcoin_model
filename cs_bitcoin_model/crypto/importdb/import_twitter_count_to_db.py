import click

from cs_bitcoin_model.crypto.aggregate.social import recent_tweet_count
from datetime import datetime, timedelta

from cs_bitcoin_model.crypto.importdb.sql_twitter_connection import SqlTwitterConnection


@click.command()
@click.option('--date', default='2022_03_05T07:00:00', required=True)
def run(date):
    """
    :return: import data to sql server
    """
    date = datetime.strptime(date, "%Y_%m_%dT%H:%M:%S")
    total_tweet_count_eth, total_tweet_count_sol, total_tweet_count_bnb = recent_tweet_count.run(date)
    sqlTwitter = SqlTwitterConnection()
    data = {"datetime": date,
            "eth_count": total_tweet_count_eth,
            "sol_count": total_tweet_count_sol,
            "bnb_count": total_tweet_count_bnb}

    sqlTwitter.insert_data(data)
    # sqlTwitter.delete_many(date - timedelta(days=2))
    sqlTwitter.close()


if __name__ == "__main__":
    run()
