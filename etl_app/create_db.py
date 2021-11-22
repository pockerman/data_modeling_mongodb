"""
Create the MongoDB database for the ETL.
https://www.mongodb.com/blog/post/getting-started-with-python-and-mongodb
"""
from pymongo import MongoClient
from pprint import pprint


def create_db(db_configuration):
    """
    Create the MongoDB database
    :param db_configuration:
    :return: connection
    """

    def check_db_exists(connection, db_name):
        db_names = connection.list_database_names()

        if db_name in db_names:
            return True
        return False

    if db_configuration["db_host"] == "ATLAS":
        # attempt to connect to ATLAS
        raise NotImplementedError("Connecting to ATLAS is not implemented yet")
        #return None
    elif db_configuration["db_host"] == "localhost":
        client = MongoClient(host="localhost", port=db_configuration["db_port"])

        if check_db_exists(connection=client, db_name=db_configuration["db_name"]):
            client.drop_database(db_configuration["db_name"])

        # create the database
        db = client[db_configuration["db_name"]]

        return client

    raise ValueError("Invalid db_host. {} is not a valid db_host".format(db_configuration))