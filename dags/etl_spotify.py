from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import psycopg2
import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import os
from collections import defaultdict


DB_STRING = 'postgres://david:testpass123!@localhost:5432/my_spotify_playlist'
USER_ID = 'vptcsa4x3uex55eyexny73e4o'
TOKEN = 'BQCwU_GRKCLOkE7xnIbiRIGB2RJVaRMGooYHoNfOvArDkW8DA7pr9Uq3BQYQB5MYZdCo8gd68VkMLJdEw-SQSRRpgxti5Yntvu5P8E-gfSZy3J00eYRB9vV4BsazcoWQdufE587rbA-Wfh1aOsyz-JDEwubyzfE_WYjG'


def extract():
    """
    Extract data from Spotify and create a pandas dataframe.
    """

    headers = {
        "Accecpt": "appliction/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=TOKEN)
    }

    endpoint = "https://api.spotify.com/v1/me/player/recently-played"
    yesterday = datetime.now() - timedelta(days=2)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get(
        endpoint + "?after={time}".format(time=yesterday_unix_timestamp), 
        headers=headers
    )

    data = r.json()
    # print(json.dumps(data, indent=2))

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
    
    # Check primary key
    if not pd.Series(df['played_at']).is_unique:
        raise ValueError("Duplicates exist in a column. Cannot be an primary key.")
    
    # Check missing values
    if df.isna().values.any():
        raise ValueError("Missing values detected.")

    # Check if all timestamps are of yesterday's date.
    # yesterday = datetime.now() - timedelta(days=1)
    # yesterday = yesterday.replace(hour=0, minute=0, microsecond=0)

    # for timestamp in df['timestamp']:
    #     if timestamp != yesterday.strftime("%Y-%m-%d"):
    #         raise ValueError("Wrong timestamp detected. At least one of the songs is not played within 24 hours.")

    print("Data is valid. Proceed to the load stage.")


def load(df):
    db = create_engine(DB_STRING)
    conn = psycopg2.connect(database='my_spotify_playlist', user='david', host='localhost', password='testpass123!')
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
    song_df = extract()

    try:
        validate_df(song_df)
    except:
        print("An error has occured. Data validation failed.")
    else:
        load(song_df)