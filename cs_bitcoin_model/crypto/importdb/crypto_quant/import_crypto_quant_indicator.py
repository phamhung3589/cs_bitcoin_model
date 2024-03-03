import click

from datetime import datetime, timedelta

from cs_bitcoin_model.crypto.aggregate.cryoto_quant.crypto_quant_api import AggregateCryptoQuanIndicator
from cs_bitcoin_model.crypto.importdb.crypto_quant.sql_cryptoquant_indicator import SqlCryptoQuantRawValue


@click.command()
@click.option('--date', default='20220305', required=True)
def run(date):
    """
    :return: import data to sql server
    """
    quant_api = AggregateCryptoQuanIndicator(raw_value=True)
    data = quant_api.aggregate_indicator(date)

    sql_client = SqlCryptoQuantRawValue()
    sql_client.insert_data(data)
    date = datetime.strptime(date, "%Y%m%d")

    sql_client.delete_many(date - timedelta(days=1800))
    sql_client.close()


if __name__ == "__main__":
    run()
