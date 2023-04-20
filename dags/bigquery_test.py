from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator,
    BigQueryDeleteDatasetOperator
)
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import datetime, timedelta

PROJECT_ID="is3107-team-1"

client_credentials_manager = SpotifyClientCredentials(
    client_id='3e25cd440de940c0866e4dac787f2429',
    client_secret='bfd94bf1911544938ecc85326ba6fcb7'
)

sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

default_args = {
    'owner': 'admin',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

playlist_urls = [
    "https://open.spotify.com/playlist/37i9dQZEVXbLRQDuF5jeBp?si=be52041969ed49a2", # USA
    "https://open.spotify.com/playlist/37i9dQZEVXbJPcfkRz0wJ0", # AU
    "https://open.spotify.com/playlist/37i9dQZEVXbLnolsZ8PSNw", # UK
    "https://open.spotify.com/playlist/37i9dQZEVXbIPWwFssbupI", # FR
    "https://open.spotify.com/playlist/37i9dQZEVXbLoATJ81JYXz" # SE
]

playlists_country_dict = {
    0: "USA",
    1: "AU",
    2: "UK",
    3: "FR",
    4: "SE"
}

def get_top_playlists():
    playlists_per_country_dict = {}
    for idx, url in enumerate(playlist_urls):
        result = sp.playlist_items(playlist_id=url)
        playlists_per_country_dict[idx] = result['items']
    return playlists_per_country_dict

def get_tracks_audio_features(track_ids):
    track_audio_features_list = sp.audio_features(tracks=track_ids)
    return track_audio_features_list
        
with DAG(
    default_args=default_args,
    dag_id='spotify_ETL',
    start_date=datetime(2023, 4, 18),
    schedule_interval='@daily'
) as dag:
    
    task1 = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset",
        dataset_id="spotify",
        delete_contents=True
    )
    
    task2 = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id="spotify"
    )
    
    task3 = BigQueryCreateEmptyTableOperator(
        task_id="create_top_playlist_table",
        dataset_id="spotify",
        table_id="top_playlist",
        schema_fields=[
            {"name": "track_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "track_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "track_url", "type": "STRING", "mode": "REQUIRED"},
            {"name": "added_at", "type": "STRING", "mode": "REQUIRED"},
            {"name": "artists_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "artists_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "artists_url", "type": "STRING", "mode": "REQUIRED"},
            {"name": "release_date", "type": "STRING", "mode": "REQUIRED"},
            {"name": "popularity", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "duration", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "country", "type": "STRING", "mode": "REQUIRED"}
        ]
    )

    task4 = BigQueryCreateEmptyTableOperator(
        task_id="create_track_table",
        dataset_id="spotify",
        table_id="track",
        schema_fields=[
            {"name": "track_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "track_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "acousticness", "type": "FLOAT", "mode": "REQUIRED"},
            {"name": "danceability", "type": "FLOAT", "mode": "REQUIRED"},
            {"name": "energy", "type": "FLOAT", "mode": "REQUIRED"},
            {"name": "instrumentalness", "type": "FLOAT", "mode": "REQUIRED"},
            {"name": "key", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "liveness", "type": "FLOAT", "mode": "REQUIRED"},
            {"name": "loudness", "type": "FLOAT", "mode": "REQUIRED"},
            {"name": "mode", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "tempo", "type": "FLOAT", "mode": "REQUIRED"},
            {"name": "valence", "type": "FLOAT", "mode": "REQUIRED"},
            {"name": "country", "type": "STRING", "mode": "REQUIRED"}
        ]
    )

    task1 >> task2 >> [task3, task4]
    
    playlists_per_country_dict = get_top_playlists()
    for idx, items in playlists_per_country_dict.items():
        INSERT_QUERY_TOP_PLAYLISTS = (
            f"INSERT spotify.top_playlist VALUES "
        )

        INSERT_QUERY_TRACK = (
            f"INSERT spotify.track VALUES "
        )

        track_ids = []
        track_id_name_map = {}
        for j in items:
            track_ids.append(j["track"]["id"])
            track_id_name_map[j["track"]["id"]] = j["track"]["name"]
            artists_names = ""
            artists_ids = ""
            artists_urls = ""
            for artist in j['track']['artists']:
                artists_names += artist['name'] + ', '
                artists_ids += artist['id'] + ', '
                artists_urls += artist['external_urls']['spotify'] + ', '
            artists_names_re = artists_names[:len(artists_names) - 2]
            artists_ids_re = artists_ids[:len(artists_ids) - 2]
            artists_urls_re = artists_urls[:len(artists_urls) - 2]
            INSERT_QUERY_TOP_PLAYLISTS += \
                f'("{j["track"]["name"]}", "{j["track"]["id"]}", "{j["track"]["external_urls"]["spotify"]}", "{j["added_at"]}", "{artists_names_re}", "{artists_ids_re}", "{artists_urls_re}", "{j["track"]["album"]["release_date"]}", {j["track"]["popularity"]}, {j["track"]["duration_ms"]}, "{playlists_country_dict[idx]}"), '

        for audio_feature in get_tracks_audio_features(track_ids):
            INSERT_QUERY_TRACK += \
                f'("{audio_feature["id"]}", "{track_id_name_map[audio_feature["id"]]}", {audio_feature["acousticness"]}, {audio_feature["danceability"]}, {audio_feature["energy"]}, {audio_feature["instrumentalness"]}, {audio_feature["key"]}, {audio_feature["liveness"]}, {audio_feature["loudness"]}, {audio_feature["mode"]}, {audio_feature["tempo"]}, {audio_feature["valence"]}, "{playlists_country_dict[idx]}"), '

        FINAL_QUERY_TRACK = INSERT_QUERY_TRACK[:len(INSERT_QUERY_TRACK) - 2] + ";"
        FINAL_QUERY_TOP_PLAYLISTS = INSERT_QUERY_TOP_PLAYLISTS[:len(INSERT_QUERY_TOP_PLAYLISTS) - 2] + ";"
        task5 = BigQueryInsertJobOperator(
            task_id=f"insert_track_{idx}",
            configuration={
                "query": {
                    "query": FINAL_QUERY_TRACK,
                    "useLegacySql": False
                }
            }
        )
        task6 = BigQueryInsertJobOperator(
            task_id=f"insert_playlist_{idx}",
            configuration={
                "query": {
                    "query": FINAL_QUERY_TOP_PLAYLISTS,
                    "useLegacySql": False
                }
            }
        )
        task3 >> task4 >> [task5, task6]