"""
Create the MongoDB database for the ETL.
https://www.mongodb.com/blog/post/getting-started-with-python-and-mongodb
"""
import json
from pymongo import MongoClient
from pprint import pprint


USERS_COLLECTION = "users"
ARTISTS_COLLECTION = "artists"

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

        # create the collections
        db[USERS_COLLECTION]
        db[ARTISTS_COLLECTION]

        return client, db

    raise ValueError("Invalid db_host. {} is not a valid db_host".format(db_configuration))


def has_artist(db, artist_data):
    """
    Returns True if the given artist
    :param db:
    :param artist_data:
    :return:
    """
    result = db.find_one(artist_data)
    return result


def update_artist_songs(db, artist_data, songs):
    """
    Update the songs of the artist identified by artist data

    :param db: The DB instance
    :param artist_data: The data identifying the artist
    :param songs: The new songs to add
    :return: None
    """
    db.ARTISTS_COLLECTION.find_and_modify(query=artist_data, update={"$addToSet": songs})


def insert_one_artist(db, artist_data):
    """
    Persist the data into the ARTISTS_COLLECTION in the given database instance
    :param db: The DB instance
    :param artist_data: The data to persist
    :return: None
    """
    db.ARTISTS_COLLECTION.insert_one(artist_data)


def populate_users_collection(db, data):
    """
    Persist the data into the USERS_COLLECTION in the given database instance
    :param db: The DB instance
    :param data: The data to persist
    :return: None
    """

    for index, row in data.iterrows():
        data_row = {"user_id": row["userId"],
                    "firstName": row["firstName"],
                    "lastName": row["lastName"], "gender": row["gender"],
                    "level": row["level"]}

        db.USERS_COLLECTION.insert_one(data_row)
