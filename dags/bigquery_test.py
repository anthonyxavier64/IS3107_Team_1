from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryGetDataOperator
)
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import datetime, timedelta
from airflow.decorators import dag, task
import json

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

def test(items):
#     for j in items:
#         print(type(j))
    print(items)

def get_track_ids_and_countries(ti):
    track_ids_countries_list = ti.xcom_pull(task_ids="get_track_ids_and_countries")
    print(track_ids_countries_list, len(track_ids_countries_list))

# def load_playlist_item(item, country, idx):
#     task = BigQueryInsertJobOperator(
#         task_id=f"insert_playlist_{country}_{idx}",
#         configuration={
#             "query": {
#                 "query": "select * from spotify_ETL.spotify.top_playlists",
#                 "useLegacySql": False
#             }
#         }
#     )
#     return task

# def get_top_playlist_and_load(url):
#     result = sp.playlist_items(playlist_id=url)
#     idx = playlist_urls.index(url)
#     country = playlists_country_dict[idx]

#     for idx, item in enumerate(result['items']):
#        load_playlist_item(item, country, idx)

# @dag(dag_id='spotify_ETL',
#      default_args=default_args,
#      start_date=datetime(2023, 4, 18),
#      schedule_interval='@daily')
# def spotify_etl():

#     @task
#     def delete_dataset():
#         BigQueryDeleteDatasetOperator(
#             task_id="delete_dataset",
#             dataset_id="spotify",
#             delete_contents=True
#         )
    
    # @task
    # def create_dataset():
    #     BigQueryCreateEmptyDatasetOperator(
    #         task_id="create_dataset",
    #         dataset_id="spotify"
    #     )
    
    # @task
    # def create_table():
    #     BigQueryCreateEmptyTableOperator(
    #         task_id="create_table",
    #         dataset_id="spotify",
    #         table_id="top_playlists",
    #         schema_fields=[
    #             {"name": "track_name", "type": "STRING", "mode": "REQUIRED"},
    #             {"name": "track_id", "type": "STRING", "mode": "REQUIRED"},
    #             {"name": "track_url", "type": "STRING", "mode": "REQUIRED"},
    #             {"name": "added_at", "type": "STRING", "mode": "REQUIRED"},
    #             {"name": "artists_name", "type": "STRING", "mode": "REQUIRED"},
    #             {"name": "artists_id", "type": "STRING", "mode": "REQUIRED"},
    #             {"name": "artists_url", "type": "STRING", "mode": "REQUIRED"},
    #             {"name": "release_date", "type": "STRING", "mode": "REQUIRED"},
    #             {"name": "popularity", "type": "INTEGER", "mode": "REQUIRED"},
    #             {"name": "duration", "type": "INTEGER", "mode": "REQUIRED"}
    #             # {"name": "country", "type": "STRING", "mode": "REQUIRED"}
    #         ]
    #     )
    
    # @task
    # def get_top_playlists():
    #     playlists_per_country_dict = {}
    #     for idx, url in enumerate(playlist_urls):
    #         result = sp.playlist_items(playlist_id=url)
    #         playlists_per_country_dict[idx] = result['items']
    #     return playlists_per_country_dict

    # @task
    # def insert_playlist_items(playlist_items_tuple):
    #     idx = playlist_items_tuple[0]
    #     items = playlist_items_tuple[1]
    #     INSERT_QUERY = (
    #         f"INSERT spotify.top_playlists VALUES "
    #     )

    #     for j in items:
    #         artists_names = ""
    #         artists_ids = ""
    #         artists_urls = ""
    #         for artist in j['track']['artists']:
    #             artists_names += artist['name'] + ', '
    #             artists_ids += artist['id'] + ', '
    #             artists_urls += artist['external_urls']['spotify'] + ', '
    #         artists_names_re = artists_names[:len(artists_names) - 2]
    #         artists_ids_re = artists_ids[:len(artists_ids) - 2]
    #         artists_urls_re = artists_urls[:len(artists_urls) - 2]
    #         INSERT_QUERY += \
    #             f"({j['track']['name']}, {j['track']['id']}, {j['track']['external_urls']['spotify']}, {j['added_at']}, {artists_names_re}, {artists_ids_re}, {artists_urls_re}, {j['track']['album']['release_date']}, {j['track']['popularity']}, {j['track']['duration_ms']}, {playlists_country_dict[idx]}), "

    #     FINAL_QUERY = INSERT_QUERY[:len(INSERT_QUERY) - 2] + ";"
    #     BigQueryInsertJobOperator(
    #         task_id=f"insert_playlist_{idx}",
    #         configuration={
    #             "query": {
    #                 "query": FINAL_QUERY,
    #                 "useLegacySql": False
    #             }
    #         }
    #     )
    
#     delete_dataset()
#     # >> create_dataset() >> create_table() >> insert_playlist_items.expand(playlist_items_tuple=get_top_playlists())

# spotify_etl()

        
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
        task_id="create_top_playlists_table",
        dataset_id="spotify",
        table_id="top_playlists",
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

    # TODO
    # task4 = BigQueryCreateEmptyTableOperator(
    #     task_id="create_track_details_table",
    #     dataset_id="spotify",
    #     table_id="track_details",
    #     schema_fields=[
        
    #     ]
    # )

    task1 >> task2 >> task3
    
    playlists_per_country_dict = get_top_playlists()
    for idx, items in playlists_per_country_dict.items():
        # task4 = PythonOperator(
        #     task_id=f"{idx}",
        #     python_callable=test,
        #     op_kwargs={'items': items}
        # )
        # print(items)
        INSERT_QUERY = (
            f"INSERT spotify.top_playlists VALUES "  # INSERT to track details table also
        )

        for j in items:
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
            INSERT_QUERY += \
                f'("{j["track"]["name"]}", "{j["track"]["id"]}", "{j["track"]["external_urls"]["spotify"]}", "{j["added_at"]}", "{artists_names_re}", "{artists_ids_re}", "{artists_urls_re}", "{j["track"]["album"]["release_date"]}", {j["track"]["popularity"]}, {j["track"]["duration_ms"]}, "{playlists_country_dict[idx]}"), '

        FINAL_QUERY = INSERT_QUERY[:len(INSERT_QUERY) - 2] + ";"
        task4 = PythonOperator(
            task_id=f"{idx}",
            python_callable=test,
            op_kwargs={'items': FINAL_QUERY}
        )
        task5 = BigQueryInsertJobOperator(
            task_id=f"insert_playlist_{idx}",
            configuration={
                "query": {
                    "query": FINAL_QUERY,
                    "useLegacySql": False
                }
            }
        )
        task3 >> task4 >> task5

    # """
    # next step: select track_id, country from table, then use track_id to get all info about track
    # """

    # """
    # have to get tracks by country, as there is a limit of 100 rows being retrieved per query only
    # """

    # for value in playlists_country_dict.values():
    #     # add filtering by value
    #     task6 = BigQueryGetDataOperator(
    #         task_id="get_track_ids_and_countries",
    #         dataset_id="spotify",
    #         table_id="top_playlists",
    #         selected_fields="track_id, country"
    #     )
    #     # call function to return list 

    # # track_ids_countries_list = get_track_ids_and_countries()
    # # for idx, i in enumerate(track_ids_countries_list):
    # task7 = PythonOperator(
    #     task_id=f"test_2",
    #     python_callable=get_track_ids_and_countries
    # )

    # task5 >> task6 >> task7
    # task4 = [get_top_playlist_and_load(url) for url in playlist_urls]

    # task4 = PythonOperator(
    #     task_id="top_playlists_ETL",
    #     python_callable=get_top_playlists_and_load
    # )

    # task1 >> task2 >> task3 >> task4