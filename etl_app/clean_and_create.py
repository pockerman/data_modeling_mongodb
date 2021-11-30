import os
from pathlib import Path
import pandas as pd
import glob

from db_utilities import has_artist, update_artist_songs, \
    insert_one_artist, populate_users_collection, has_song, insert_into_time, insert_into_songplays


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


def process_artists_collection(db, filepath: Path):

    df = pd.read_json(filepath, lines=True)

    # we need to preprocess the column names
    df = df.rename(columns={"artist_name": "name", "artist_location": "location",
                            "artist_latitude": "latitude", "artist_longitude": "longitude"})

    # drop any rows if the artist_id and the artist name are not
    # defined
    df = df[(df['artist_id'].notna()) & (df['name'].notna())]

    # drop any row if the song title or song_id are
    # not defined
    df = df[(df['title'].notna()) & (df['song_id'].notna())]

    artist_data = df[['artist_id', 'name', 'location', 'latitude',
                      'longitude', 'title', 'year', 'duration', 'song_id']]

    # fix the artist data so that we have the songs
    # and a dictionary
    for index, row in artist_data.iterrows():

        # search by artist name
        artist_data_dict = {'name': row['name']}

        artist_exists = has_artist(db=db, artist_data=artist_data_dict)

        if artist_exists is None:

            artist_data_dict.update({'artist_id': row['artist_id'], 'location': row['location'],
                                    'latitude': row['latitude'], 'longitude': row['longitude'],
                                    'songs': [{'title': row['title'], 'year': row['year'],
                                           'duration': row['duration'], 'song_id': row['song_id']}]})
            insert_one_artist(db=db, artist_data=artist_data_dict)
        else:

            # the artist exist. Does it have the song?
            artist_has_song = has_song(db, artist_data=artist_data_dict, song_title=row['title'])

            if artist_has_song is None:

                update_artist_songs(db=db, artist_data=artist_data_dict,
                                    songs=[{'title': row['title'], 'year': row['year'], 'duration': row['duration']}])


def process_artists(db, filepath: Path):

    files = collect_files(filepath=filepath)

    # get total number of files found
    num_files = len(files)
    print('INFO: {} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(files, 1):
        process_artists_collection(db=db, filepath=Path(datafile))
        print('{}/{} files processed.'.format(i, num_files))


def process_log_files(db, filepath: Path):

    files = collect_files(filepath=filepath)

    # get total number of files found
    num_files = len(files)
    print('INFO: {} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(files, 1):
        process_users_collection(db=db, filepath=Path(datafile))
        print('{}/{} files processed.'.format(i, num_files))


def process_users_collection(db, filepath: Path):
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

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df = user_df.drop_duplicates()

    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.isocalendar().week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    #for i, row in time_df.iterrows():

    #    time_data = {"time": row["start_time"], "hour": row["hour"], "day": row["day"],
    #                 "week": row["week"], 'month': row["month"], "year": row["year"],
    #                 "weekday": row["weekday"]}
    #    insert_into_time(db=db, time_data=time_data)

    populate_users_collection(db=db, data=user_df)


def process_songplays_collection(db, filepath: Path) -> None:
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df = user_df.drop_duplicates()

    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.isocalendar().week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    # insert songplay records
    for index, row in df.iterrows():

        song = row["song"]
        artist = row["artist"]
        length = row["length"]

        # fetch the artist data for the artist with the
        # given name that has the song with the given name
        artist_query_data = {"name": artist}
        artist_data = has_song(artist_query_data, song_title=song)

        if artist_data is not None:

            song_data = {"time_start": row.ts, "user_id": row.userId,
                         "user_agent": row.userAgent, "ssession_id": row.sessionId,
                         "artist_id": artist_data["artist_id"],
                         "song_id": artist_data["songs"]}
            insert_into_songplays(db=db, song_data=song_data)

        # get songid and artistid from song and artist tables
        #cur.execute(song_select, (row.song, row.artist, row.length))
        #results = cur.fetchone()

        #if results:
        #    songid, artistid = results
        #else:
        #    songid, artistid = None, None

        # insert songplay record
        #songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        #cur.execute(songplay_table_insert, songplay_data)

    #for i, row in time_df.iterrows():
    #    time_data = {"time": row["start_time"], "hour": row["hour"], "day": row["day"],
    #                 "week": row["week"], 'month': row["month"], "year": row["year"],
    #                 "weekday": row["weekday"]}
    #    insert_into_time(db=db, time_data=time_data)

    #populate_users_collection(db=db, data=user_df)

