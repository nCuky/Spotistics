import spotipy as sp
from spotipy.oauth2 import SpotifyOAuth
from spotipy.oauth2 import SpotifyClientCredentials
import os
import pandas as pd
import numpy as np
import datetime as dt
import matplotlib as mpl

MAX_TRACKS_FOR_FEATURES = 100
MAX_TRACKS_FOR_PLAYLIST_ITEMS = 100
MAX_TRACKS_BATCH_SIZE_FOR_RECENTLY_PLAYED = 50
MAX_TRACKS_AMOUNT_FOR_RECENTLY_PLAYED = 1000
AUTH_SCOPE = "user-library-read playlist-read-collaborative playlist-read-private user-read-recently-played"
REDIRECT_URI = "http://localhost:8888/spotify/callback"


def get_token(token_path = None):
    if token_path is None:
        token_path = "token"
    token_file = open(token_path, "r")
    token_file_text = token_file.readlines()
    token_file.close()
    return token_file_text


def find_playlist(playlist_items: list, playlist_name: str):
    """
    Finds a playlist by a given name.
    :return: tracks' id of the found playlist, otherwise None
    """

    for plst in playlist_items:
        if plst['name'] == playlist_name:
            plst_tracks_id = plst['id']
            break

    return plst_tracks_id


def get_tracks_ids(tracks_items: list):
    '''

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


class SpotifyClient:
    client = None

    def __init__(self, token, auth_scope = AUTH_SCOPE,
                 redirect_uri = REDIRECT_URI):
        cid = token[0].strip()
        csecret = token[1].strip()
        self.auth_manager = SpotifyOAuth(client_id = cid,
                                         client_secret = csecret,
                                         redirect_uri = redirect_uri,
                                         scope = auth_scope)

    def connect(self):
        self.client = sp.Spotify(auth_manager = self.auth_manager)

    def disconnected(self):
        self.client = None

    def is_connected(self):
        if self.client is None:
            return False
        return True

    def validate_connection(self):
        if not self.is_connected():
            raise ValueError("Client is not connected to Spotify API")

    def get_all_user_playlists(self, limit = 50):
        self.validate_connection()
        response = self.client.current_user_playlists(limit = limit)
        results = response['items']
        offset = limit
        while response['next'] is not None:
            response = self.client.current_user_playlists(limit = limit, offset = offset)
            results.extend(response['items'])
            offset += limit

        return results

    def get_all_playlist_tracks(self, playlist_id: list,
                                limit = MAX_TRACKS_FOR_PLAYLIST_ITEMS):
        self.validate_connection()
        response = self.client.playlist_tracks(playlist_id, limit = limit)
        results = response['items']
        offset = limit
        while response['next'] is not None:
            response = self.client.playlist_tracks(playlist_id, limit = limit,
                                                    offset = offset)
            results.extend(response['items'])
            offset += limit
        return results

    def get_tracks_audio_features(self, tracks_items: list,
                                  limit = MAX_TRACKS_FOR_FEATURES):
        '''
        :param tracks_items:
        :param limit:
        :return:
        '''
        self.validate_connection()
        ids = get_tracks_ids(tracks_items)
        result = list()

        while len(ids) > 0:
            result.extend(self.client.audio_features(ids[0:limit]))

            # Removing the first {limit} items from the list:
            ids = ids[limit:]

        return result

    def get_specific_audio_feature(self,
                                   tracks_items: list,
                                   audio_feature: str,
                                   limit = MAX_TRACKS_FOR_FEATURES):
        '''
        Returns a list of a desired audio feature for all given tracks.
        :param tracks_items:
        :param audio_feature:
        :param limit:
        :return:
        '''
        self.validate_connection()
        tracks_audio_features = self.get_tracks_audio_features(tracks_items,
                                                               limit)
        result = list()

        for track in tracks_audio_features:
            # for audio_feature in audio_features
            result.append(track[audio_feature])

        return result

    def get_all_recently_played_tracks(self,
                                       max_tracks_amount = MAX_TRACKS_AMOUNT_FOR_RECENTLY_PLAYED,
                                       batch_size = MAX_TRACKS_BATCH_SIZE_FOR_RECENTLY_PLAYED):
        """
        Returns a large number of the user's recently played tracks.
        :param max_tracks_amount:
        :param batch_size:
        :return:
        """
        self.validate_connection()
        response = self.client.current_user_recently_played(limit = batch_size)

        results = response['items']
        next_batch_before_timestamp = response['cursors']['before']

        while  (response['next'] is not None) \
            or (len(results) <= max_tracks_amount):
            response = self.client.current_user_recently_played(limit = batch_size,
                                                                before = next_batch_before_timestamp)

            results.extend(response['items'])
            next_batch_before_timestamp = response['cursors']['before']

        return results

    def create_tracks_data_frame(self, tracks_items: list,
                                 audio_features_names: list,
                                 limit = MAX_TRACKS_FOR_FEATURES):
        self.validate_connection()
        tracks_with_features = {'track_name': get_tracks_names(tracks_items)}


        test_artists_list = get_tracks_artists(tracks_items)

        # tracks_with_features['artist'] =

        for feat_name in audio_features_names:
            # Inserting a new column, titled feat_name, containing a list of all the audio features for all the tracks:
            tracks_with_features[feat_name] = self.get_specific_audio_feature(tracks_items,
                                                                              audio_feature = feat_name,
                                                                              limit = limit)

        return pd.DataFrame(data = tracks_with_features)


class SpotifyData:
    MODE_MAJOR = ''
    MODE_MINOR = 'm'

    data = None

    def __init__(self, aggr_level = 'track', data_dir = 'data/spoti_data'):
        '''
        Reads data from Spotify CSV data files into a parse-able dataframe.
        :param aggr_level: Which dataset to load ('track', 'artist', 'genres', 'year', 'w_genres')
        :param data_dir: Directory of the data file
        '''
        dict_file_names = {'track'   : "data.csv",
                           'artist'  : "data_by_artist.csv",
                           'genres'  : "data_by_genres.csv",
                           'year'    : "data_by_year.csv",
                           'w_genres': "data_w_genres.csv"}

        file_path = data_dir + '/' + dict_file_names[aggr_level]
        self.data = self.prepare_data(pd.read_csv(file_path))


    def prepare_data(self, df_to_prepare):
        '''
        Prepares the data for musical analysis, e.g. translates the Spotify API's key and mode numerical values
        to human-readable musical Key signatures.
        :return: The prepared Spotify data.
        '''
        # Mode ("Modus") dictionary:
        modes_replacement_dict = {'from': [0, 1],
                                  'to'  : [SpotifyData.MODE_MINOR, SpotifyData.MODE_MAJOR]}

        df_to_prepare['mode'] = df_to_prepare['mode'].replace(to_replace = modes_replacement_dict['from'],
                                                              value = modes_replacement_dict['to'])

        # Key signature dictionary:
        keys_replacement_dict = {'from': np.arange(start = 0, stop = 12, step = 1),
                                 'to'  : ['C', 'C#', 'D', 'D#', 'E', 'F', 'F#', 'G', 'G#', 'A', 'A#', 'B']}

        df_to_prepare['key'] = df_to_prepare['key'].replace(to_replace = keys_replacement_dict['from'],
                                                            value = keys_replacement_dict['to'])

        # Full Key signature (Key and Modus):
        df_to_prepare['full_key'] = df_to_prepare['key'] + df_to_prepare['mode']

        return df_to_prepare


# my_spotify = SpotifyClient(get_token())
# my_spotify.connect()
# user_playlists = my_spotify.get_all_user_playlists()
# test_plst = find_playlist(user_playlists, "Erez and Nadav")
# test_plst_tracks = my_spotify.get_all_playlist_tracks(test_plst)
# test_df = my_spotify.create_tracks_data_frame(tracks_items = test_plst_tracks,
#                                               audio_features_names = ['instrumentalness', 'energy', 'danceability', 'acousticness', 'tempo'])

my_spoti_data = SpotifyData()
only_duration = my_spoti_data.data.groupby('full_key')['duration_ms'].sum()
# my_results = my_spoti_data.data.drop(columns = 'duration_ms').groupby('full_key').mean().assign(duration_ms = only_duration)
my_results = my_spoti_data.data.drop(columns = 'duration_ms').groupby('full_key').count().assign(duration_ms = only_duration)
my_results.to_csv("results_group_by_key_" + dt.datetime.now().strftime('%Y-%m-%d %H_%M_%S') + ".csv")

mpl.

# my_spoti_data.data.groupby('key').mean().to_csv("mean_by_key.csv")

client = SpotifyClient(token = get_token())
client.connect()

my_recently_played = client.get_all_recently_played_tracks()


x = 1

def calc_listen_data_by_key():
    '''
    Aggregates all listened tracks by key, and writes it as a csv file
    :return:
    '''
    my_spoti_data = SpotifyData(aggr_level = 'track')
    only_duration = my_spoti_data.data.groupby('key')['duration_ms'].sum()
    my_results = my_spoti_data.data.drop(columns = 'duration_ms').groupby('key').mean().assign(
        duration_ms = only_duration)
    my_results.to_csv("listen_data_by_key.csv")


def calc_listen_data_mean_key():
    '''
    Aggregates all listened tracks by mean key
    :return:
    '''
    my_spoti_data = SpotifyData(aggr_level = 'track')
    my_spoti_data.data.groupby('key').mean().to_csv("mean_by_key.csv")