import os
from pathlib import Path
import pandas as pd
import glob


def collect_files(filepath: Path) -> list:
    """
    Collect all the files in the given filepath
    :param filepath:
    :return: array containing the filepaths
    """

    # tmp holder for the artist files
    result_files = []

    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))

        for f in files:
            result_files.append(os.path.abspath(f))

    return result_files


def process_log_files(connection, db, filepath: Path, users_insert_func):

    files = collect_files(filepath=filepath)

    # get total number of files found
    num_files = len(files)
    print('INFO: {} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(files, 1):
        process_users_collection(db=db, filepath=Path(datafile), users_insert_func=users_insert_func)
        print('{}/{} files processed.'.format(i, num_files))


def process_users_collection(db, filepath: Path, users_insert_func):
    """
    Process the data for users collection
    :param connection:
    :param filepath:
    :param users_insert_func:
    :return:
    """

    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df = user_df.drop_duplicates()

    users_insert_func(db=db, data=user_df)

