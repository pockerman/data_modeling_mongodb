import argparse
import json

from etl_app.utils import read_configuration_file
from etl_app.utils import INFO
from etl_app.create_db import create_db


def main(configuration):
    """
    Main function to run the ETL
    :param configuration: The configuration to create the ETL
    :return: None
    """

    with open(configuration["db_config_file"], "r") as db_f:
        db_config = json.load(db_f)
        coonection = create_db(db_configuration=db_config)



if __name__ == '__main__':

    print("{} Start MongoDB ETL...".format(INFO))
    parser = argparse.ArgumentParser(description='Read --config file.')
    parser.add_argument('--config', type=str, default='config_file.json',
                        help="You must specify a json formatted configuration file")

    args = parser.parse_args()
    configuration = read_configuration_file(args.config)
    main(configuration=configuration)
    print("{} Done MongoDB ETL...".format(INFO))