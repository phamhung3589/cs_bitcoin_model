import click

from cs_bitcoin_model.crypto.aggregate.cryoto_quant.crypto_quant_api import AggregateCryptoQuanIndicator
from cs_bitcoin_model.crypto.importdb.sql_connection import SqlConnector
from datetime import datetime


@click.command()
@click.option('--date', default='2022_03_05', required=True)
def run(date):
    """
    :return: import data to sql server
    """
    date = str(datetime.strptime(date, "%Y_%m_%d").strftime("%Y%m%d"))
    quant_api = AggregateCryptoQuanIndicator()
    result = quant_api.aggregate_indicator(date)
    sqlConnector = SqlConnector()
    sqlConnector.insert_data(result)


if __name__ == "__main__":
    run()
