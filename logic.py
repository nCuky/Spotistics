from spotify_api_client import SpotifyAPIClient as spapi
from SpotifyDataSet import SpotifyDataSet as spdt
# import pyspark as sk
import seaborn as sns  # Plotting
import pandas as pd


def get_token(token_path=None):
    if token_path is None:
        token_path = "token"
    token_file = open(token_path, "r")
    token_file_text = token_file.readlines()
    token_file.close()
    return token_file_text


spapic = spapi(token=get_token())


def get_artist_audio_features_data(name: str):
    artist_id = spapic.find_artist(name).id
    tracks = spapic.artist_get_all_tracks(artist_id)
    tracks_ids = [track.id for track in tracks]
    tracks_features = spapic.get_tracks_audio_features(tracks_ids)

    pd.read_json()

    return tracks_features


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
    param tracks_items: Track items (objects)
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
