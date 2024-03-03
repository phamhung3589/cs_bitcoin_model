import mysql.connector

from mysql.connector import Error
from cs_bitcoin_model.crypto.utils.cs_config import CSConfig
from datetime import datetime


class SqlCryptoQuantRawValue:
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
        tables = {'cypto_quant':
                      "CREATE TABLE IF NOT EXISTS `crypto_quant_indicator_value` ("
                      "  `datetime` DATE NOT NULL PRIMARY KEY,"
                      "  `ind_mpi` FLOAT NOT NULL,"
                      "  `ind_exchange_netflow` FLOAT NOT NULL,"
                      "  `ind_stablecoins_ratio_usd` FLOAT NOT NULL,"
                      "  `ind_leverage_ratio` FLOAT NOT NULL,"
                      "  `ind_sopr` FLOAT NOT NULL,"
                      "  `ind_sopr_holders` FLOAT NOT NULL,"
                      "  `ind_mvrv` FLOAT NOT NULL,"
                      "  `ind_nupl` FLOAT NOT NULL,"
                      "  `ind_nvt_golden_cross` FLOAT NOT NULL,"
                      "  `ind_puell_multiple` FLOAT NOT NULL,"
                      "  `ind_utxo` FLOAT NOT NULL,"
                      "  `ind_long_liquidation` FLOAT NOT NULL,"
                      "  `ind_short_liquidation` FLOAT NOT NULL"
                      ")"}

        self.cursor.execute(tables['cypto_quant'])

    def insert_data(self, data: dict):
        """
        :param data: data with key as the same in insert query and value as a String of Type-Coefficient
        :return: insert one row to db
        """
        # Create table if not exist in db
        self.create_table()

        # insert data in to table
        add_data = ("REPLACE INTO crypto_quant_indicator_value "
                          "(datetime, ind_mpi, ind_exchange_netflow, ind_stablecoins_ratio_usd, ind_leverage_ratio, ind_sopr, ind_sopr_holders, ind_mvrv, ind_nupl, ind_nvt_golden_cross, ind_puell_multiple, ind_utxo, ind_long_liquidation, ind_short_liquidation) "
                          "VALUES "
                          "(%(datetime)s, %(ind_mpi)s, %(ind_exchange_netflow)s, %(ind_stablecoins_ratio_usd)s, %(ind_leverage_ratio)s, %(ind_sopr)s, %(ind_sopr_holders)s, %(ind_mvrv)s, %(ind_nupl)s, %(ind_nvt_golden_cross)s, %(ind_puell_multiple)s, %(ind_utxo)s, %(ind_long_liquidation)s, %(ind_short_liquidation)s)")

        self.cursor.execute(add_data, data)
        self.connector.commit()
        print("Insert to db done")

    def delete_data(self, key: datetime):
        sql_query = f"DELETE FROM crypto_quant_indicator_value where datetime = '%s'"

        try:
            # Execute the SQL command
            self.cursor.execute(sql_query % key)
            print(f"delete data with datetime = {key}")
            # Commit your changes in the database
            self.connector.commit()

        except Exception as e:
            print(e)

    def delete_many(self, key: datetime):
        sql_query = f"DELETE FROM crypto_quant_indicator_value where datetime < '%s'"

        try:
            # Execute the SQL command
            self.cursor.execute(sql_query % key)
            print(f"delete data with datetime < {key}")
            # Commit your changes in the database
            self.connector.commit()

        except Exception as e:
            print(e)

    def get_data_from_db(self, filter_date, column):
        """
        :param filter_date: date for filter from sql tables
        :return: Row corresponding to date need to get
        """
        sql_query = f"SELECT {column} FROM crypto_quant_indicator_value where datetime = '%s'"
        self.cursor.execute(sql_query % filter_date)
        data_quant_indicators = self.cursor.fetchone()

        if data_quant_indicators is None:
            return {}

        data_quant_indicators_name = [i[0] for i in self.cursor.description]
        data_quant_indicators_value = list(data_quant_indicators)
        data = dict(zip(data_quant_indicators_name, data_quant_indicators_value))

        result = {"datetime": filter_date,
                  "indicator_value": data[column]}

        return result

    def get_multi_data_from_db(self, current_date, column):
        list_dates = [current_date]
        current_datetime = datetime.strptime(current_date, "%Y%m%d")
        for i in range(1, 90):
            list_dates.append(
                datetime.fromtimestamp(current_datetime.timestamp() - 24 * 60 * 60 * i).replace(minute=0, second=0).strftime(
                    "%Y%m%d"))

        list_dates.reverse()
        recent_data = {}
        for date in list_dates:
            data = self.get_data_from_db(date, column)
            if data != {}:
                data.pop("datetime")
                recent_data[date] = data["indicator_value"]

        return recent_data

    def get_all_data(self, from_date):
        """
        :param filter_date: start date to current date for filter from sql tables
        :return: Row corresponding to date need to get
        """
        sql_query = f"SELECT * FROM crypto_quant_indicator_value where datetime >= '%s'"
        self.cursor.execute(sql_query % from_date)
        data_quant_indicators = self.cursor.fetchall()

        if data_quant_indicators is None:
            return {}

        data_quant_indicators_name = [i[0] for i in self.cursor.description]
        result = {}

        for i in range(1, len(data_quant_indicators_name)):
            data_hist = {x[0].strftime("%Y%m%d"): x[i] for x in data_quant_indicators}
            result[data_quant_indicators_name[i][4:]] = data_hist

        return result


if __name__ == "__main__":
    current_time = datetime.fromtimestamp(datetime.utcnow().timestamp() - 100*24*60*60).replace(minute=0, second=0)
    current_time = current_time.strftime("%Y%m%d")

    sql_client = SqlCryptoQuantRawValue()
    result = sql_client.get_all_data("20220101")
    print(result)
