import mysql.connector

from mysql.connector import Error

from cs_bitcoin_model.crypto.utils.constants import list_indicators
from cs_bitcoin_model.crypto.utils.cs_config import CSConfig
from datetime import datetime

from cs_bitcoin_model.crypto.utils.db_utils import parse_string


class SqlConnector:
    """
    - Create connection to mysql server - reading from config file
    - Adding function for inserting data, select from db, create table
    """
    def __init__(self):
        """
        - Create connection to cursor to mysql server
        """
        parser = CSConfig("production", self.__class__.__name__, "config")
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
        tables = {'quant_indicator':
                      "CREATE TABLE IF NOT EXISTS `cryptoquant` ("
                      "  `datetime` DATE NOT NULL PRIMARY KEY,"
                      "  `ind_mpi` nvarchar(15) NOT NULL,"
                      "  `ind_exchange_netflow` nvarchar(15) NOT NULL,"
                      "  `ind_stablecoins_ratio_usd` nvarchar(15) NOT NULL,"
                      "  `ind_leverage_ratio` nvarchar(15) NOT NULL,"
                      "  `ind_sopr` nvarchar(15) NOT NULL,"
                      "  `ind_sopr_holders` nvarchar(15) NOT NULL,"
                      "  `ind_mvrv` nvarchar(15) NOT NULL,"
                      "  `ind_nupl` nvarchar(15) NOT NULL,"
                      "  `ind_nvt_golden_cross` nvarchar(15) NOT NULL,"
                      "  `ind_puell_multiple` nvarchar(15) NOT NULL,"
                      "  `ind_utxo` nvarchar(15) NOT NULL,"
                      "  `ind_long_liquidation` nvarchar(15) NOT NULL,"
                      "  `ind_short_liquidation` nvarchar(15) NOT NULL"
                      ")"}

        self.cursor.execute(tables['quant_indicator'])

    def insert_data(self, data: dict):
        """
        :param data: data with key as the same in insert query and value as a String of Type-Coefficient
        :return: insert one row to db
        """
        # Create table if not exist in db
        self.create_table()

        # insert data in to table
        add_data_quant = ("REPLACE INTO cryptoquant "
                          "(datetime, ind_mpi, ind_exchange_netflow, ind_stablecoins_ratio_usd, ind_leverage_ratio, "
                          "ind_sopr, ind_sopr_holders, ind_mvrv, ind_nupl, ind_nvt_golden_cross, ind_puell_multiple, "
                          "ind_utxo, ind_long_liquidation, ind_short_liquidation) "
                          "VALUES "
                          "(%(datetime)s, %(ind_mpi)s, %(ind_exchange_netflow)s, %(ind_stablecoins_ratio_usd)s, "
                          "%(ind_leverage_ratio)s, %(ind_sopr)s, %(ind_sopr_holders)s, %(ind_mvrv)s, "
                          "%(ind_nupl)s, %(ind_nvt_golden_cross)s, %(ind_puell_multiple)s, %(ind_utxo)s, "
                          "%(ind_long_liquidation)s, %(ind_short_liquidation)s)")

        self.cursor.execute(add_data_quant, data)
        self.connector.commit()
        print("Insert to db done")
        self.close()

    def get_data_from_db(self, filter_date):
        """
        :param filter_date: date for filter from sql tables
        :return: Row corresponding to date need to get
        """
        sql_query = f"SELECT " + ",".join(list_indicators) + " FROM cryptoquant where datetime = '%s'"
        self.cursor.execute(sql_query % filter_date)
        data_indicators = self.cursor.fetchone()

        if data_indicators is None:
            return {}

        data_indicators = list(data_indicators)

        result = {}
        data = []
        for idx, value in enumerate(data_indicators):
            if idx == 0:
                result[list_indicators[idx]] = str(data_indicators[idx])
                continue
            data_indicators[idx] = parse_string(value)
            indicator = {"name": list_indicators[idx][4:], "type": data_indicators[idx][0], "coeff": data_indicators[idx][1]}
            data.append(indicator)

            # result[list_indicators[idx] + "_type"] = data_indicators[idx][0]
            # result[list_indicators[idx] + "_coeff"] = data_indicators[idx][1]

        result["data"] = data

        self.close()
        return result


if __name__ == "__main__":
    date = datetime.strptime("2022-03-04", "%Y-%m-%d").date()
    sqlConnector = SqlConnector()
    print(sqlConnector.get_data_from_db(date))
