# ETL-spotify-data
It is a simple project that covers basic ETL (extract, transform, load) process and Airflow scheduling.

* Steps:
    1. Extract my recently played music data.
        - Use [`Spotipy`](https://spotipy.readthedocs.io/en/2.16.1/#welcome-to-spotipy) library to get my recent playlist data from Spotify.

    2. Transform and validate the data before loading into a database.
        - Filter the extracted data to select few columns that are useful for this project (title, artists, timestamp, played_at, urls) and change it to tabular format using `pandas.DataFrame`.

    3. Load the data into a database.
        - Use `PostgreSQL` for the database.

    4. Use Airflow scheduler to proceed it daily.

