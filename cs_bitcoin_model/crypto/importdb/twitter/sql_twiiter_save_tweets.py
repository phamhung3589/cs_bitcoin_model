import mysql.connector
import requests

from mysql.connector import Error
from cs_bitcoin_model.crypto.utils.cs_config import CSConfig
from datetime import datetime


class SqlTwitterSaveTweetsConnection:
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
        tables = {'twitter_save_tweets':
                      "CREATE TABLE IF NOT EXISTS `user_save_tweets` ("
                      "  `datetime` DATETIME NOT NULL,"
                      "  `user_id` INT NOT NULL PRIMARY KEY,"
                      "  `list_save_tweets` TEXT NOT NULL"
                      ")"}

        self.cursor.execute(tables['twitter_save_tweets'])

    def insert_data(self, data: dict, merge_data=True):
        """
        :param data: data with key as the same in insert query and value as a String of Type-Coefficient
        :return: insert one row to db
        """
        # Create table if not exist in db
        self.create_table()

        # insert data in to table
        add_data = ("REPLACE INTO user_save_tweets "
                          "(datetime, user_id, list_save_tweets) "
                          "VALUES "
                          "(%(datetime)s, %(user_id)s, %(list_save_tweets)s)")

        user_save_tweets = self.get_data_from_db(data["user_id"])
        if len(user_save_tweets) != 0 and merge_data:
            if user_save_tweets["list_save_tweets"] != "":
                user_save_tweets = user_save_tweets["list_save_tweets"].split(",")
                user_new_tweets = data["list_save_tweets"].split(",")

                all_new_save_tweets = [x for x in user_new_tweets if x not in user_save_tweets]
                all_new_save_tweets = user_save_tweets + all_new_save_tweets
                all_new_save_tweets = ",".join(all_new_save_tweets)
                data["list_save_tweets"] = all_new_save_tweets

        self.cursor.execute(add_data, data)
        self.connector.commit()
        print("Insert to db done")

        return data

    def delete_data(self, key: datetime):
        sql_query = f"DELETE FROM user_save_tweets where datetime = '%s'"

        try:
            # Execute the SQL command
            self.cursor.execute(sql_query % key)
            print(f"delete data with datetime = {key}")
            # Commit your changes in the database
            self.connector.commit()

        except Exception as e:
            print(e)

    def delete_many(self, key: datetime):
        sql_query = f"DELETE FROM user_save_tweets where datetime < '%s'"

        try:
            # Execute the SQL command
            self.cursor.execute(sql_query % key)
            print(f"delete data with datetime < {key}")
            # Commit your changes in the database
            self.connector.commit()

        except Exception as e:
            print(e)

    def delete_tweets(self, user_id, list_tweets: list):
        user_save_tweets = self.get_data_from_db(user_id)
        data = ""
        if len(user_save_tweets) != 0:
            list_save_tweets = user_save_tweets["list_save_tweets"].split(",")
            list_save_tweets = [x for x in list_save_tweets if x not in list_tweets]
            list_save_tweets = ",".join(list_save_tweets)

            data = {"datetime": user_save_tweets["datetime"], "user_id": user_id, "list_save_tweets": list_save_tweets}
            self.insert_data(data, merge_data=False)

        return data

    def get_data_from_db(self, user_id):
        """
        :param filter_date: date for filter from sql tables
        :return: Row corresponding to date need to get
        """
        sql_query = f"SELECT * FROM user_save_tweets where user_id = '%s'"
        self.cursor.execute(sql_query % user_id)
        data_twitter_save = self.cursor.fetchone()

        if data_twitter_save is None:
            return {}

        data_twitter_save_name = [i[0] for i in self.cursor.description]
        data_twitter_save_value = list(data_twitter_save)
        data = dict(zip(data_twitter_save_name, data_twitter_save_value))

        result = {"user_id": user_id,
                  "datetime": data["datetime"],
                  "list_save_tweets": data["list_save_tweets"]}

        return result


if __name__ == "__main__":
    date = datetime.fromtimestamp(datetime.utcnow().timestamp() - 10 * 60).replace(minute=0, second=0)
    date = date.strftime("%Y-%m-%dT%H:%M:%S")
    print(date)
    # now = datetime.utcnow()
    # data = get_tweets_recent_search(now)
    data = {"user_id": 15,
            "datetime": date,
            "list_save_tweets": "1545112297726779393"}
    sqlTwitter = SqlTwitterSaveTweetsConnection()
    sqlTwitter.insert_data(data, merge_data=False)
    #
    # data_get = sqlTwitter.get_data_from_db(date)
    # print(data_get)
    # # sqlTwitter.delete_many(now)

    # r = requests.get("https://publish.twitter.com/oembed?url=https://twitter.com/Interior/status/463440424141459456")
    # print(r.json()["html"])
