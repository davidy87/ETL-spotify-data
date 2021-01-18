from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import psycopg2
import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import os
from collections import defaultdict
import string
import random

"""
References:
    https://spotipy.readthedocs.io/en/2.16.1/
    https://github.com/plamere/spotipy
"""
import spotipy
from spotipy.oauth2 import SpotifyOAuth


def extract():
    """
    Extract data from Spotify and create a pandas dataframe.
    """

    client_id = '0f1f6981d38445b294dfd0001a32d65e'
    client_secret = '5f33872d0f204dc984cc92a92283dc15'
    state = ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase + string.digits, k=16))

    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=client_id, 
                                                   client_secret=client_secret,
                                                   redirect_uri="http://localhost:8888/callback",
                                                   state=state,
                                                   scope="user-read-recently-played"))

    yesterday = datetime.now() - timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    data = sp.current_user_recently_played(limit=50, after=yesterday_unix_timestamp)
    song_dict = defaultdict(list)

    # Try to store data into a pandas dataframe.
    try:
        for track_info in data["items"]:
            track = track_info["track"]
            song_dict["artists"].append(tuple(artist["name"] for artist in track["artists"]))
            song_dict["title"].append(track["name"])
            song_dict["timestamp"].append(track_info["played_at"][:10]) # yyyy-mm-dd
            song_dict["played_at"].append(track_info["played_at"])
            song_dict["urls"].append(track["external_urls"]["spotify"])
    except KeyError:
        print(data["error"]["message"])

    return pd.DataFrame(song_dict, columns=song_dict.keys())


def validate_df(df):
    """
    Check if the song dataframe is validate.
    """

    # There are no songs in the dataframe (perhaps I did not play any song).
    if df.empty:
        print("No songs have been downloaded.")
    
    # Check the primary key
    if not pd.Series(df['played_at']).is_unique:
        raise ValueError("Duplicates exist in a column. Cannot be an primary key.")
    
    # Check missing values
    if df.isna().values.any():
        raise ValueError("Missing values detected.")

    print("Data is valid. Proceed to the load stage.")


def load(df):
    db_string = 'postgres://david:testpass123!@localhost:5432/my_spotify_playlist'
    db = create_engine(db_string)
    conn = psycopg2.connect(database='my_spotify_playlist', 
                            user='david', 
                            host='localhost', 
                            password='testpass123!')
                            
    cursor = conn.cursor()
    
    query = """
            CREATE TABLE IF NOT EXISTS songs(
                title TEXT,
                artists TEXT,
                timestamp VARCHAR(255),
                played_at VARCHAR(255) PRIMARY KEY,
                urls TEXT
            )
            """

    cursor.execute(query)

    try:
        df.to_sql("songs", db, index=False, if_exists='append')
        print("Data successfully stored into the database.")
    except:
        print("Data already exists in the database.")

    conn.commit()
    conn.close()
    print("Database closed.")


def etl():
    """
    Perform ETL process
    """
    
    song_df = extract()
    print(song_df)

    try:
        validate_df(song_df)
    except:
        print("An error has occured. Data validation failed.")
    else:
        load(song_df)