import mysql.connector
import numpy as np
import pandas as pd

from mysql.connector import Error

# from cs_bitcoin_model.crypto.aggregate.btc_prediction.worker_predict_btc_price import BtcPredict, get_current_price
from cs_bitcoin_model.crypto.utils.cs_config import CSConfig
from datetime import datetime


class SqlBtcPrediction:
    """
    - Create connection to mysql server - reading from config file
    - Adding function for inserting data, select from db, create table
    """
    def __init__(self):
        """
        - Create connection to cursor to mysql server
        """
        parser = CSConfig("production", "SqlConnector", "config")
        host = parser.read_parameter("host")
        database = parser.read_parameter("database")
        user = parser.read_parameter("user")
        password = parser.read_parameter("password")
        self.connector = None
        try:
            self.connector = mysql.connector.connect(host=host, database=database, user=user, password=password)

            if self.connector.is_connected():
                # print(f"Connected to server with host: {host} and db: {database}")
                self.cursor = self.connector.cursor()
        except Error as e:
            print("Error while connecting to MySQL", e)

    def close(self):
        """
        :return: Close all connection
        """
        self.cursor.close()
        self.connector.close()
        print("Disconnected to sql server")

    def create_table(self):
        """
        :return: Running query to create table if not exists
        """
        tables = {'btc_prediction':
                      "CREATE TABLE IF NOT EXISTS `btc_prediction_price` ("
                      "  `datetime` DATETIME NOT NULL PRIMARY KEY,"
                      "  `prediction` INT NOT NULL,"
                      "  `current_close` FLOAT NOT NULL,"
                      "  `next_close` FLOAT NOT NULL,"
                      "  `true_or_false` BOOLEAN NOT NULL"
                      ")"}

        self.cursor.execute(tables['btc_prediction'])

    def insert_data(self, data: dict):
        """
        :param data: data with key as the same in insert query and value as a String of Type-Coefficient
        :return: insert one row to db
        """
        # Create table if not exist in db
        self.create_table()

        # insert data in to table
        add_data = ("REPLACE INTO btc_prediction_price "
                          "(datetime, prediction, current_close, next_close, true_or_false) "
                          "VALUES "
                          "(%(datetime)s, %(prediction)s, %(current_close)s, %(next_close)s, %(true_or_false)s)")

        self.cursor.execute(add_data, data)
        self.connector.commit()
        print("Insert to db done")

    def delete_data(self, key: datetime):
        sql_query = f"DELETE FROM btc_prediction_price where datetime = '%s'"

        try:
            # Execute the SQL command
            self.cursor.execute(sql_query % key)
            print(f"delete data with datetime = {key}")
            # Commit your changes in the database
            self.connector.commit()

        except Exception as e:
            print(e)

    def delete_many(self, key: datetime):
        sql_query = f"DELETE FROM btc_prediction_price where datetime < '%s'"

        try:
            # Execute the SQL command
            self.cursor.execute(sql_query % key)
            print(f"delete data with datetime < {key}")
            # Commit your changes in the database
            self.connector.commit()

        except Exception as e:
            print(e)

    def get_all_data(self, remove_date=None):
        sql_query = f"SELECT * FROM btc_prediction_price"
        self.cursor.execute(sql_query)
        all_data = self.cursor.fetchall()

        if all_data is None:
            return {}

        data_btc_prediction_name = [i[0] for i in self.cursor.description]
        list_all_prediction = []
        result = {}
        for data in all_data:
            current_prediction = dict(zip(data_btc_prediction_name, list(data)))
            # current_prediction.pop("current_close")
            # current_prediction.pop("next_close")
            list_all_prediction.append(current_prediction)

        recent_prediction = {}
        for pred in list_all_prediction[-24:]:
            recent_prediction[pred["datetime"].strftime("%Y-%m-%d %H:%M:%S")] = \
                {"prediction": pred["prediction"],
                 "true_or_false": pred["true_or_false"],
                 "close_price": pred["next_close"]}

        result["recent_prediction"] = recent_prediction

        historical_data = {}
        for pred in list_all_prediction:
            tmp_date = pred["datetime"].strftime("%Y-%m-%d")
            if tmp_date not in historical_data:
                historical_data[tmp_date] = {"prediction": [], "true_or_false": []}

            historical_data[tmp_date]["prediction"].append(pred["prediction"])
            historical_data[tmp_date]["true_or_false"].append(pred["true_or_false"])

        if remove_date is not None:
            remove_date = remove_date.strftime("%Y-%m-%d")
            historical_data.pop(remove_date)

        historical_acc = []
        for date in historical_data:
            y_pred = np.array(historical_data[date]["prediction"])
            true_or_not = np.array(historical_data[date]["true_or_false"])
            acc = true_or_not.sum() / true_or_not.shape[0]

            historical_acc.append({"date": date,
                                   "acc": acc,
                                   "label_0_pre": y_pred[y_pred == 0].shape[0],
                                   "label_1_pre": y_pred[y_pred == 1].shape[0],
                                   "label_2_pre": y_pred[y_pred == 2].shape[0]})

        result["historical_accuracy"] = {}
        result["historical_accuracy"]["1W"] = historical_acc[-7:]
        result["historical_accuracy"]["2W"] = historical_acc[-14:]
        result["historical_accuracy"]["1M"] = historical_acc[-30:]
        result["historical_accuracy"]["3M"] = historical_acc[-90:]
        result["historical_accuracy"]["1Y"] = historical_acc[-365:]

        return result

    def get_data_from_db(self, filter_date):
        """
        :param filter_date: date for filter from sql tables
        :return: Row corresponding to date need to get
        """
        sql_query = f"SELECT * FROM btc_prediction_price where datetime = '%s'"
        self.cursor.execute(sql_query % filter_date)
        data_btc_prediction = self.cursor.fetchone()

        if data_btc_prediction is None:
            return {}

        data_btc_prediction_name = [i[0] for i in self.cursor.description]
        data_btc_prediction_value = list(data_btc_prediction)
        data = dict(zip(data_btc_prediction_name, data_btc_prediction_value))

        result = {"datetime": filter_date,
                  "prediction": data["prediction"],
                  "true_or_false": data["true_or_false"]}

        return result

    def get_multi_data_from_db(self, current_date):
        list_dates = [current_date]
        current_datetime = datetime.strptime(current_date, "%Y-%m-%d %H:%M:%S")
        for i in range(1, 24):
            list_dates.append(
                datetime.fromtimestamp(current_datetime.timestamp() - 60 * 60 * i).replace(minute=0, second=0).strftime(
                    "%Y-%m-%d %H:%M:%S"))

        result = {}
        for date in list_dates:
            data = self.get_data_from_db(date)
            if data != {}:
                data.pop("datetime")
                result[date] = data

        return result


if __name__ == "__main__":
    previous_time = datetime.fromtimestamp(datetime.utcnow().timestamp() - 60 * 60).replace(minute=0, second=0)
    # btc_client = BtcPredict()
    # result = btc_client.predict(previous_time.strftime("%Y_%m_%d %H:%M:%S"))
    # price, current_close = result["value"], float(result["current_price"])
    #
    # # Get current close value
    # next_close = float(get_current_price("BTCUSDT", datetime.utcnow().replace(minute=0, second=0).strftime("%Y_%m_%d %H:%M:%S"))["close"].values[0])
    # print(next_close)
    # # our prediction is true or false
    # value = (next_close - current_close)/current_close*100
    # print(value)
    #
    # if value > 0.5:
    #     value = 2
    # elif value < -0.5:
    #     value = 0
    # else:
    #     value = 1
    #
    # true_or_false = int(value) == int(price)
    #
    # print(value, price, true_or_false)

    # Get data
    sql_client = SqlBtcPrediction()
    # sql_client.insert_data(data)

    # data_get = sql_client.get_multi_data_from_db(previous_time.strftime("%Y-%m-%d %H:%M:%S"))
    data_get = sql_client.get_all_data()
    # print(data_get)
