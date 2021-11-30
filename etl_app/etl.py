import argparse
import json
from pathlib import Path

from etl_app.utils import read_configuration_file
from etl_app.utils import INFO
from etl_app.db_utilities import create_db
from etl_app.clean_and_create import process_log_files, process_artists, process_songplays_collection


def main(configuration):
    """
    Main function to run the ETL
    :param configuration: The configuration to create the ETL
    :return: None
    """

    # read the configuration file
    db_config_file = Path(configuration["db_config_file"])
    with open(configuration["db_config_file"], "r") as db_f:
        print("INFO: Loading configuration file at {0}".format(db_config_file))
        db_config = json.load(db_f)

        try:
            print("INFO: Creating DB....")
            connection, db = create_db(db_configuration=db_config)
            print("INFO: Done creating DB....")
            print("INFO: Create users collection....")

            # create the users collection
            process_log_files(db=db, filepath=Path(configuration["log_data_file"]))
            print("INFO: Done")

            print("INFO: Create artists collection....")
            process_artists(db=db, filepath=Path(configuration["song_data_file"]))
            print("INFO: Done")

            print("INFO: Create songplays collection....")
            process_songplays_collection(db=db, filepath=Path(configuration["song_data_file"]))
            print("INFO: Done")
        except Exception as e:
            print("ERROR: Could not create DB. Error msg: {0}".format(str(e)))
        finally:
            connection.close()


if __name__ == '__main__':

    print("{} Start MongoDB ETL...".format(INFO))
    parser = argparse.ArgumentParser(description='Read --config file.')
    parser.add_argument('--config', type=str, default='config_file.json',
                        help="You must specify a json formatted configuration file")

    args = parser.parse_args()
    configuration = read_configuration_file(args.config)
    main(configuration=configuration)
    print("{} Done MongoDB ETL...".format(INFO))
