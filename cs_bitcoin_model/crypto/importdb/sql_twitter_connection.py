import mysql.connector

from mysql.connector import Error
from cs_bitcoin_model.crypto.utils.cs_config import CSConfig
from datetime import datetime


class SqlTwitterConnection:
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
        tables = {'twitter_search_count':
                      "CREATE TABLE IF NOT EXISTS `twitter_search_count` ("
                      "  `datetime` DATETIME NOT NULL PRIMARY KEY,"
                      "  `eth_count` int NOT NULL,"
                      "  `sol_count` int NOT NULL,"
                      "  `bnb_count` int NOT NULL"
                      ")"}

        self.cursor.execute(tables['twitter_search_count'])

    def insert_data(self, data: dict):
        """
        :param data: data with key as the same in insert query and value as a String of Type-Coefficient
        :return: insert one row to db
        """
        # Create table if not exist in db
        self.create_table()

        # insert data in to table
        add_data = ("REPLACE INTO twitter_search_count "
                          "(datetime, eth_count, sol_count, bnb_count) "
                          "VALUES "
                          "(%(datetime)s, %(eth_count)s, %(sol_count)s, %(bnb_count)s)")

        self.cursor.execute(add_data, data)
        self.connector.commit()
        print("Insert to db done")

    def delete_data(self, key: datetime):
        sql_query = f"DELETE FROM twitter_search_count where datetime = '%s'"

        try:
            # Execute the SQL command
            self.cursor.execute(sql_query % key)
            print(f"delete data with datetime = {key}")
            # Commit your changes in the database
            self.connector.commit()

        except Exception as e:
            print(e)

    def delete_many(self, key: datetime):
        sql_query = f"DELETE FROM twitter_search_count where datetime < '%s'"

        try:
            # Execute the SQL command
            self.cursor.execute(sql_query % key)
            print(f"delete data with datetime < {key}")
            # Commit your changes in the database
            self.connector.commit()

        except Exception as e:
            print(e)

    def get_data_from_db(self, filter_date):
        """
        :param filter_date: date for filter from sql tables
        :return: Row corresponding to date need to get
        """
        sql_query = f"SELECT * FROM twitter_search_count where datetime = '%s'"
        self.cursor.execute(sql_query % filter_date)
        data_twitter_count = self.cursor.fetchone()

        if data_twitter_count is None:
            return {}

        data_twitter_count_name = [i[0] for i in self.cursor.description]
        data_twitter_count_value = list(data_twitter_count)
        data = dict(zip(data_twitter_count_name, data_twitter_count_value))

        result = {"datetime": filter_date, "eth_count": data["eth_count"], "sol_count": data["sol_count"],
                  "bnb_count": data["bnb_count"]}

        return result

    def get_multi_data_from_db(self, current_date, num_sample=11):
        list_dates = [current_date]
        current_datetime = datetime.strptime(current_date, "%Y-%m-%dT%H:%M:%S")
        for i in range(1, num_sample):
            list_dates.append(
                datetime.fromtimestamp(current_datetime.timestamp() - 60 * 60 * i).replace(minute=0, second=0).strftime(
                    "%Y-%m-%dT%H:%M:%S"))

        result = {}
        for date in list_dates:
            data = self.get_data_from_db(date)
            if data != {}:
                data.pop("datetime")
                result[date] = data

        return result


if __name__ == "__main__":
    date = datetime.fromtimestamp(datetime.utcnow().timestamp() - 60 * 60).replace(minute=0, second=0)
    # date = date.strftime("%Y-%m-%dT%H:%M:%S")
    # sqlTwitter = SqlTwitterConnection()
    # data = {"datetime": date, "eth_count": 2, "sol_count": 5, "bnb_count": 7}
    # sqlTwitter.insert_data(data)
    # data = sqlTwitter.get_data_from_db(date)
    # print(data)
    # sqlTwitter.delete_data(date)
