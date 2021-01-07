import sqlalchemy
from sqlalchemy.orm import sessionmaker
import requests
import json
import pandas as pd
from datetime import datetime
import datetime
import os
from collections import defaultdict


DATABASE_LOC = 'postgres://david:testpass123!@localhost:5432/my_spotify_playlist'
USER_ID = 'vptcsa4x3uex55eyexny73e4o'
TOKEN = 'BQDVuqfqlygcb7TMXsQPum-VbBBve8ge4V_Qbqg5TYjid1_vIfcmtsZ_xf7SQ4KtUSxgMOxFKnUZJ6EhlFvkkHxfHFpa6uB3m48BUWWR_4JVPOZKx-hroc9FrBXq-wHd0A642PKpF3ELv8H09q46ZHdhobIItpuedV5WX9Xl'


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
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get(endpoint + "?after={time}".format(time=yesterday_unix_timestamp), headers=headers)
    data = r.json()
    print(json.dumps(data, indent=2))

    song_dict = defaultdict(list)

    for track_info in data["items"]:
        track = track_info["track"]
        song_dict["artists"].append(tuple(artist["name"] for artist in track["artists"]))
        song_dict["title"].append(track["name"])
        song_dict["played_at"].append(track_info["played_at"])
        song_dict["urls"].append(track["external_urls"]["spotify"])

    return pd.DataFrame(song_dict, columns=song_dict.keys())


if __name__ == "__main__":
    song_df = extract()
