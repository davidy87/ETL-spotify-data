import sqlalchemy
from sqlalchemy.orm import sessionmaker
import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import os
from collections import defaultdict


DATABASE_LOC = 'postgres://david:testpass123!@localhost:5432/my_spotify_playlist'
USER_ID = 'vptcsa4x3uex55eyexny73e4o'
TOKEN = 'BQB19vJScTo7tCrISEZqPyle2qaiDAGpClm6rdWAfx6cgnARpFAAQvzriQpaWm490EzbzeLDb7lGo7TQaVlvfTnI098oAkq3Rh12cqM8TKNHbh2h6yYzzbnwAD-RBBPOdZDCZWPbxW9UMbpWb3K9dbC-SW6BGkqZJB04'


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
    yesterday = datetime.now() - timedelta(days=1)
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
    yesterday = datetime.now() - timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, microsecond=0)

    for timestamp in df['timestamp']:
        if timestamp != yesterday.strftime("%Y-%m-%d"):
            raise ValueError("Wrong timestamp detected. At least one of the songs is not played within 24 hours.")

    print("Data is valid. Proceed to the load stage.")


if __name__ == "__main__":
    song_df = extract()

    try:
        validate_df(song_df)
    except:
        print("Data validation failed.")

