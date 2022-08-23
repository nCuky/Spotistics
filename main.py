import datetime

from numpy.distutils.system_info import dfftw_info

from spotify_api_client import SpotifyAPIClient as spapi
from SpotifyDataSet import SpotifyDataSet as spdt
from pyspark.sql import SparkSession
import pandas as pd
from app_gui import GUI


def get_token(token_path=None):
    if token_path is None:
        token_path = "token"
    token_file = open(token_path, "r")
    token_file_text = token_file.readlines()
    token_file.close()
    return token_file_text


def find_playlist_in_list(playlist_items: list, playlist_name: str):
    """
    Finds a playlist by a given name.
    :return: Tracks' IDs of the found playlist, otherwise None
    """
    for plst in playlist_items:
        if plst['name'] == playlist_name:
            plst_tracks_id = plst['id']
            break

    return plst_tracks_id


def get_tracks_ids(tracks_items: list):
    '''
    Returns the IDs of all tracks in a given list.
    :param tracks_items:
    :return:
    '''
    out = list()
    for trk in tracks_items:
        out.append(trk['track']['id'])

    return out


def get_tracks_names(tracks_items: list):
    out = list()

    for trk in tracks_items:
        out.append(trk['track']['name'])

    return out


def get_tracks_artists(tracks_items: list):
    out = list()

    for trk in tracks_items:
        # curr_trk = dict()

        curr_artists = dict()

        for i, artist in enumerate(trk['track']['artists']):
            # if artist['name'] == 'Henry Purcell':
            #     x = 1

            curr_artists['artist_' + str(i)] = trk['track']['artists'][i]['name']

        out.append(curr_artists)

    return out


gui = GUI()

gui.close()


# spapic = spapi(token=get_token())
#
# zappa_artist_id = spapic.find_artist("Frank Zappa").id
# zappa_tracks = spapic.artist_get_all_tracks(zappa_artist_id)
# zappa_tracks_features = spapic.get_tracks_audio_features(zappa_tracks)

x = 1  # break



# PySpark
print(str(datetime.datetime.now()) + ' --- Building Spark Session...')
spark = SparkSession.builder.appName('Practice').getOrCreate()

print(str(datetime.datetime.now()) + ' --- Reading CSV... ')
df_pyspark = spark.read.csv('test1.csv', header=True, inferSchema = True)

print(str(datetime.datetime.now()) + ' --- Doing stuff... ')
df_pyspark.withColumnRenamed('track_name','name').show()






# user_playlists = spapic.get_all_user_playlists()
# test_plst = find_playlist(user_playlists, "Erez and Nadav")
# test_plst_tracks = spapic.get_all_playlist_tracks(test_plst)
# test_df = spapic.create_tracks_data_frame(tracks_items = test_plst_tracks,
#                                               audio_features_names = ['instrumentalness', 'energy', 'danceability', 'acousticness', 'tempo'])


# # ------------ local data csv's, local logic
# my_spoti_data = spdt()
# only_duration = my_spoti_data.data.groupby('key')['duration_ms'].sum()
# my_results = my_spoti_data.data.drop(columns='duration_ms').groupby('key').mean().assign(duration_ms=only_duration)
# my_results.to_csv("our_results.csv")


# my_spoti_data.data.groupby('key').mean().to_csv("mean_by_key.csv")


def calc_listen_data_by_key():
    '''
    Aggregates all listened tracks by key, and writes it as a csv file
    :return:
    '''
    my_spoti_data = spdt(aggr_level='track')
    only_duration = my_spoti_data.data.groupby('key')['duration_ms'].sum()
    my_results = my_spoti_data.data.drop(columns='duration_ms').groupby('key').mean().assign(
        duration_ms=only_duration)
    my_results.to_csv("listen_data_by_key.csv")


def calc_listen_data_mean_key():
    '''
    Aggregates all listened tracks by mean key
    :return:
    '''
    my_spoti_data = spdt(aggr_level='track')
    my_spoti_data.data.groupby('key').mean().to_csv("mean_by_key.csv")
