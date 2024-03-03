import mysql.connector

from mysql.connector import Error
from cs_bitcoin_model.crypto.utils.cs_config import CSConfig


class SqlAccessToken:
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

    def check_data_from_db(self, token_access):
        """
        :param filter_date: date for filter from sql tables
        :return: Row corresponding to date need to get
        """
        sql_query = f"SELECT * FROM access_token where token = '%s'"
        self.cursor.execute(sql_query % token_access)
        data_access_token = self.cursor.fetchone()

        if data_access_token is None:
            return False

        return True


if __name__ == "__main__":
    token_check = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxNSIsImlhdCI6MTY1NjY4NDA1OCwiZXhwIjoxNzQzMDg0MDU4fQ.3o1IrAfRQIVFcartLSyeteoP0yIOTtz9sCgqpaYvKiM"
    sqlAccessToken = SqlAccessToken()
    print(sqlAccessToken.check_data_from_db(token_check))
