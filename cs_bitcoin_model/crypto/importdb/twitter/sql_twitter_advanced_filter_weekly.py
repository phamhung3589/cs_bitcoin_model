import mysql.connector

from mysql.connector import Error

from cs_bitcoin_model.crypto.aggregate.social.twitter_recent_search import *
from cs_bitcoin_model.crypto.utils.cs_config import CSConfig
from datetime import datetime


class SqlTwitterAdvanceFilterWeeklyConnection:
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
        tables = {'twitter_advanced_filter_weekly':
                      "CREATE TABLE IF NOT EXISTS `twitter_advanced_filter_weekly` ("
                      "  `datetime` DATETIME NOT NULL PRIMARY KEY,"
                      "  `nft_link` TEXT NOT NULL,"
                      "  `defi_link` TEXT NOT NULL,"
                      "  `web3_link` TEXT NOT NULL,"
                      "  `ico_link` TEXT NOT NULL,"
                      "  `nft_username` TEXT NOT NULL,"
                      "  `defi_username` TEXT NOT NULL,"
                      "  `web3_username` TEXT NOT NULL,"
                      "  `ico_username` TEXT NOT NULL,"
                      "  `nft_created` TEXT NOT NULL,"
                      "  `defi_created` TEXT NOT NULL,"
                      "  `web3_created` TEXT NOT NULL,"
                      "  `ico_created` TEXT NOT NULL,"
                      "  `nft_tag` TEXT NOT NULL,"
                      "  `defi_tag` TEXT NOT NULL,"
                      "  `web3_tag` TEXT NOT NULL,"
                      "  `ico_tag` TEXT NOT NULL"
                      ")"}

        self.cursor.execute(tables['twitter_advanced_filter_weekly'])

    def insert_data(self, data: dict):
        """
        :param data: data with key as the same in insert query and value as a String of Type-Coefficient
        :return: insert one row to db
        """
        # Create table if not exist in db
        self.create_table()

        # insert data in to table
        add_data = ("REPLACE INTO twitter_advanced_filter_weekly "
                    "(datetime, nft_link, defi_link, web3_link, ico_link, nft_username, defi_username, web3_username, ico_username, nft_created, defi_created, web3_created, ico_created, nft_tag, defi_tag, web3_tag, ico_tag) "
                    "VALUES "
                    "(%(datetime)s, %(nft_link)s, %(defi_link)s, %(web3_link)s, %(ico_link)s, %(nft_username)s, %(defi_username)s, %(web3_username)s, %(ico_username)s, %(nft_created)s, %(defi_created)s, %(web3_created)s, %(ico_created)s, %(nft_tag)s, %(defi_tag)s, %(web3_tag)s, %(ico_tag)s)")

        self.cursor.execute(add_data, data)
        self.connector.commit()
        print("Insert to db done")

    def delete_data(self, key: datetime):
        sql_query = f"DELETE FROM twitter_advanced_filter_weekly where datetime = '%s'"

        try:
            # Execute the SQL command
            self.cursor.execute(sql_query % key)
            print(f"delete data with datetime = {key}")
            # Commit your changes in the database
            self.connector.commit()

        except Exception as e:
            print(e)

    def delete_many(self, key: datetime):
        sql_query = f"DELETE FROM twitter_advanced_filter_weekly where datetime < '%s'"

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
        sql_query = f"SELECT * FROM twitter_advanced_filter_weekly where datetime = '%s'"
        self.cursor.execute(sql_query % filter_date)
        data_twitter_advanced_search = self.cursor.fetchone()

        if data_twitter_advanced_search is None:
            return {}

        data_twitter_search_name = [i[0] for i in self.cursor.description]
        data_twitter_search_value = list(data_twitter_advanced_search)
        data = dict(zip(data_twitter_search_name, data_twitter_search_value))

        data["nft_link"] = [x.split("/")[-1] for x in data["nft_link"].split(",")]
        data["defi_link"] = [x.split("/")[-1] for x in data["defi_link"].split(",")]
        data["web3_link"] = [x.split("/")[-1] for x in data["web3_link"].split(",")]
        data["ico_link"] = [x.split("/")[-1] for x in data["ico_link"].split(",")]

        data["nft_username"] = data["nft_username"].split(",")
        data["defi_username"] = data["defi_username"].split(",")
        data["web3_username"] = data["web3_username"].split(",")
        data["ico_username"] = data["ico_username"].split(",")

        data["nft_created"] = data["nft_created"].split(",")
        data["defi_created"] = data["defi_created"].split(",")
        data["web3_created"] = data["web3_created"].split(",")
        data["ico_created"] = data["ico_created"].split(",")

        data["nft_tag"] = [x.split(",") for x in data["nft_tag"].split("|")]
        data["defi_tag"] = [x.split(",") for x in data["defi_tag"].split("|")]
        data["web3_tag"] = [x.split(",") for x in data["web3_tag"].split("|")]
        data["ico_tag"] = [x.split(",") for x in data["ico_tag"].split("|")]

        result = {"datetime": filter_date,
                  "nft_link": data["nft_link"],
                  "defi_link": data["defi_link"],
                  "web3_link": data["web3_link"],
                  "ico_link": data["ico_link"],
                  "nft_username": data["nft_username"],
                  "defi_username": data["defi_username"],
                  "web3_username": data["web3_username"],
                  "ico_username": data["ico_username"],
                  "nft_created": data["nft_created"],
                  "defi_created": data["defi_created"],
                  "web3_created": data["web3_created"],
                  "ico_created": data["ico_created"],
                  "nft_tag": data["nft_tag"],
                  "defi_tag": data["defi_tag"],
                  "web3_tag": data["web3_tag"],
                  "ico_tag": data["ico_tag"]}

        return result


if __name__ == "__main__":
    date = datetime.fromtimestamp(datetime.utcnow().timestamp() - 10 * 60).replace(minute=0, second=0)
    date = date.strftime("%Y-%m-%dT%H:%M:%S")
    print(date)
    now = datetime.utcnow()
    data_nft, data_defi, data_web3, data_ico = get_advanced_filter_tweets_weekly(now)
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

    sqlTwitter = SqlTwitterAdvanceFilterWeeklyConnection()
    sqlTwitter.insert_data(data)
    sqlTwitter.close()

    # data_get = sqlTwitter.get_data_from_db(date)
    # sqlTwitter.close()
    # print(data_get)
