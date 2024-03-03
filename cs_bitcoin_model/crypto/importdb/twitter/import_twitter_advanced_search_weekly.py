import click

from datetime import datetime, timedelta

from cs_bitcoin_model.crypto.aggregate.social.twitter_recent_search import get_advanced_filter_tweets_weekly
from cs_bitcoin_model.crypto.importdb.twitter.sql_twitter_advanced_filter_weekly import SqlTwitterAdvanceFilterWeeklyConnection


@click.command()
@click.option('--date', default='2022_03_05T07:00:00', required=True)
def run(date):
    """
    :return: import data to sql server
    """
    date = datetime.strptime(date, "%Y_%m_%dT%H:%M:%S")
    data_nft, data_defi, data_web3, data_ico = get_advanced_filter_tweets_weekly(date)
    sqlTwitter = SqlTwitterAdvanceFilterWeeklyConnection()
    data = {"datetime": date,
            "nft_link": data_nft["link"],
            "defi_link": data_defi["link"],
            "web3_link": data_web3["link"],
            "ico_link": data_ico["link"],
            "nft_username": data_nft["username"],
            "defi_username": data_defi["username"],
            "web3_username": data_web3["username"],
            "ico_username": data_ico["username"],
            "nft_created": data_nft["times"],
            "defi_created": data_defi["times"],
            "web3_created": data_web3["times"],
            "ico_created": data_ico["times"],
            "nft_tag": data_nft["tag"],
            "defi_tag": data_defi["tag"],
            "web3_tag": data_web3["tag"],
            "ico_tag": data_ico["tag"]
            }

    sqlTwitter.insert_data(data)
    sqlTwitter.delete_many(date - timedelta(days=20))
    sqlTwitter.close()


if __name__ == "__main__":
    run()
