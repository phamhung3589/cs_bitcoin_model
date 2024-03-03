import mysql.connector
import requests

from mysql.connector import Error

from cs_bitcoin_model.crypto.aggregate.social.twitter_recent_search import TwitterRecentSearch, get_query_sol, get_query_eth, \
    get_query_bnb, get_tweets_recent_search
from cs_bitcoin_model.crypto.utils.cs_config import CSConfig
from datetime import datetime


class SqlTwitterRecentSearchConnection:
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
        tables = {'twitter_recent_search_trending':
                      "CREATE TABLE IF NOT EXISTS `twitter_recent_search_trending` ("
                      "  `datetime` DATETIME NOT NULL PRIMARY KEY,"
                      "  `eth_chain` TEXT NOT NULL,"
                      "  `sol_chain` TEXT NOT NULL,"
                      "  `bnb_chain` TEXT NOT NULL,"
                      "  `eth_username` TEXT NOT NULL,"
                      "  `sol_username` TEXT NOT NULL,"
                      "  `bnb_username` TEXT NOT NULL,"
                      "  `eth_created` TEXT NOT NULL,"
                      "  `sol_created` TEXT NOT NULL,"
                      "  `bnb_created` TEXT NOT NULL,"
                      "  `eth_tag` TEXT NOT NULL,"
                      "  `sol_tag` TEXT NOT NULL,"
                      "  `bnb_tag` TEXT NOT NULL"
                      ")"}

        self.cursor.execute(tables['twitter_recent_search_trending'])

    def insert_data(self, data: dict):
        """
        :param data: data with key as the same in insert query and value as a String of Type-Coefficient
        :return: insert one row to db
        """
        # Create table if not exist in db
        self.create_table()

        # insert data in to table
        add_data = ("REPLACE INTO twitter_recent_search_trending "
                    "(datetime, eth_chain, sol_chain, bnb_chain, eth_username, sol_username, bnb_username, eth_created, sol_created, bnb_created, eth_tag, sol_tag, bnb_tag) "
                    "VALUES "
                    "(%(datetime)s, %(eth_chain)s, %(sol_chain)s, %(bnb_chain)s, %(eth_username)s, %(sol_username)s, %(bnb_username)s, %(eth_created)s, %(sol_created)s, %(bnb_created)s, %(eth_tag)s, %(sol_tag)s, %(bnb_tag)s)")

        self.cursor.execute(add_data, data)
        self.connector.commit()
        print("Insert to db done")

    def delete_data(self, key: datetime):
        sql_query = f"DELETE FROM twitter_recent_search_trending where datetime = '%s'"

        try:
            # Execute the SQL command
            self.cursor.execute(sql_query % key)
            print(f"delete data with datetime = {key}")
            # Commit your changes in the database
            self.connector.commit()

        except Exception as e:
            print(e)

    def delete_many(self, key: datetime):
        sql_query = f"DELETE FROM twitter_recent_search_trending where datetime < '%s'"

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
        sql_query = f"SELECT * FROM twitter_recent_search_trending where datetime = '%s'"
        self.cursor.execute(sql_query % filter_date)
        data_twitter_search = self.cursor.fetchone()

        if data_twitter_search is None:
            return {}

        data_twitter_search_name = [i[0] for i in self.cursor.description]
        data_twitter_search_value = list(data_twitter_search)
        data = dict(zip(data_twitter_search_name, data_twitter_search_value))

        # data["eth_chain"] = [requests.get("https://publish.twitter.com/oembed?url=" + x).json()["html"] for x in data["eth_chain"].split(",")]
        # data["sol_chain"] = [requests.get("https://publish.twitter.com/oembed?url=" + x).json()["html"] for x in data["sol_chain"].split(",")]
        # data["bnb_chain"] = [requests.get("https://publish.twitter.com/oembed?url=" + x).json()["html"] for x in data["bnb_chain"].split(",")]

        data["eth_chain"] = [x.split("/")[-1] for x in data["eth_chain"].split(",")]
        data["sol_chain"] = [x.split("/")[-1] for x in data["sol_chain"].split(",")]
        data["bnb_chain"] = [x.split("/")[-1] for x in data["bnb_chain"].split(",")]

        data["eth_username"] = data["eth_username"].split(",")
        data["sol_username"] = data["sol_username"].split(",")
        data["bnb_username"] = data["bnb_username"].split(",")

        data["eth_created"] = data["eth_created"].split(",")
        data["sol_created"] = data["sol_created"].split(",")
        data["bnb_created"] = data["bnb_created"].split(",")

        data["eth_tag"] = [x.split(",") for x in data["eth_tag"].split("|")]
        data["sol_tag"] = [x.split(",") for x in data["sol_tag"].split("|")]
        data["bnb_tag"] = [x.split(",") for x in data["bnb_tag"].split("|")]

        result = {"datetime": filter_date,
                  "eth_chain": data["eth_chain"],
                  "sol_chain": data["sol_chain"],
                  "bnb_chain": data["bnb_chain"],
                  "eth_username": data["eth_username"],
                  "sol_username": data["sol_username"],
                  "bnb_username": data["bnb_username"],
                  "eth_created": data["eth_created"],
                  "sol_created": data["sol_created"],
                  "bnb_created": data["bnb_created"],
                  "eth_tag": data["eth_tag"],
                  "sol_tag": data["sol_tag"],
                  "bnb_tag": data["bnb_tag"]}

        self.close()
        return result


if __name__ == "__main__":
    date = datetime.fromtimestamp(datetime.utcnow().timestamp() - 10 * 60).replace(minute=0, second=0)
    date = date.strftime("%Y-%m-%dT%H:%M:%S")
    print(date)
    now = datetime.utcnow()
    data_eth, data_sol, data_bnb = get_tweets_recent_search(now)
    data = {"datetime": date,
            "eth_chain": data_eth["link"],
            "sol_chain": data_sol["link"],
            "bnb_chain": data_bnb["link"],
            "eth_username": data_eth["username"],
            "sol_username": data_sol["username"],
            "bnb_username": data_bnb["username"],
            "eth_created": data_eth["times"],
            "sol_created": data_sol["times"],
            "bnb_created": data_bnb["times"],
            "eth_tag": data_eth["tag"],
            "sol_tag": data_sol["tag"],
            "bnb_tag": data_bnb["tag"]
            }

    sqlTwitter = SqlTwitterRecentSearchConnection()
    # sqlTwitter.insert_data(data)
    # sqlTwitter.close()

    data_get = sqlTwitter.get_data_from_db(date)
    print(data_get)

    # # sqlTwitter.delete_many(now)

    # r = requests.get("https://publish.twitter.com/oembed?url=https://twitter.com/Interior/status/463440424141459456")
    # print(r.json()["html"])
