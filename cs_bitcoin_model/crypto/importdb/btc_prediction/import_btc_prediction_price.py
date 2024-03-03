import click

from datetime import datetime, timedelta

from cs_bitcoin_model.crypto.aggregate.btc_prediction.worker_predict_btc_price import BtcPredict, get_current_price
from cs_bitcoin_model.crypto.importdb.btc_prediction.sql_btc_prediction import SqlBtcPrediction


@click.command()
@click.option('--date', default='2022_03_05T07:00:00', required=True)
def run(date):
    """
    :return: import data to sql server
    """
    # Convert date to datetime
    date = datetime.strptime(date, "%Y_%m_%dT%H:%M:%S")

    # Predict for previous hours
    previous_time = datetime.fromtimestamp(date.timestamp() - 60 * 60).replace(minute=0, second=0)
    btc_client = BtcPredict()
    result = btc_client.predict(previous_time.strftime("%Y_%m_%d %H:%M:%S"))
    price, current_close = result["value"], float(result["current_price"])

    # Get current close value
    next_close = float(get_current_price("BTCUSDT", date.replace(minute=0, second=0).strftime("%Y_%m_%d %H:%M:%S"))["close"].values[0])

    # our prediction is true or false
    value = (next_close - current_close)/current_close*100

    if value > 0.5:
        value = 2
    elif value < -0.5:
        value = 0
    else:
        value = 1

    true_or_false = int(value) == int(price)

    # Create data
    data = {"datetime": previous_time,
            "prediction": price,
            "current_close": current_close,
            "next_close": next_close,
            "true_or_false": true_or_false}

    # Insert into db
    sql_client = SqlBtcPrediction()
    sql_client.insert_data(data)

    # Close all connection
    sql_client.close()


if __name__ == "__main__":
    run()
