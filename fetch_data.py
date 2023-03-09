import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd

#spotify api & auth
client_id = '' 
client_secret=''
client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)

#returns a list of track ids from playlist URL
def obtain_track_id(link):
    playlist_URI = link.split("/")[-1].split("?")[0]
    track_uris = [x["track"]["uri"].split(":")[-1] for x in sp.playlist_tracks(playlist_URI)["items"]]
    return track_uris

# returns dataframe containing audio features of all tracks
def obtain_audio_features(track_list):
    rows = []
    None_counter = 0

    for i in range(0,len(track_list)):
        feature_results = sp.audio_features(track_list[i])
        for i, t in enumerate(feature_results):
            if t == None:
                None_counter = None_counter + 1
            else:
                rows.append(t)
    df_audio_features = pd.DataFrame.from_dict(rows,orient='columns')
    return df_audio_features
    #TO DO - determine what form of output we would want from dataframe - csv? json?

'''
debugging statements
print(len(obtain_track_id('https://open.spotify.com/playlist/37i9dQZEVXbK4gjvS1FjPY')))
print(sp.audio_features('7oDd86yk8itslrA9HRP2ki'))
print(obtain_audio_features(obtain_track_id('https://open.spotify.com/playlist/37i9dQZEVXbK4gjvS1FjPY')))
'''
