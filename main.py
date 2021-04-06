import spotipy as sp
from spotipy.oauth2 import SpotifyOAuth
from spotipy.oauth2 import SpotifyClientCredentials
import os
import pandas as pd

MAX_TRACKS_FOR_FEATURES = 100
MAX_TRACKS_FOR_PLAYLIST_ITEMS = 100
AUTH_SCOPE = "user-library-read playlist-read-collaborative playlist-read-private"
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

    def get_specific_audio_feature(self, tracks_items: list,
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

    def create_tracks_data_frame(self, tracks_items: list,
                                 audio_features_names: list,
                                 limit = MAX_TRACKS_FOR_FEATURES):
        self.validate_connection()
        tracks_with_features = {'track_name': get_tracks_names(tracks_items)}

        for feat_name in audio_features_names:
            tracks_with_features[feat_name] = self.get_specific_audio_feature(tracks_items,
                                                                              audio_feature = feat_name,
                                                                              limit = limit)

        return pd.DataFrame(data = tracks_with_features)


my_spotify = SpotifyClient(get_token())
my_spotify.connect()
user_playlists = my_spotify.get_all_user_playlists()
test_plst = find_playlist(user_playlists, "Erez and Nadav")
test_plst_tracks = my_spotify.get_all_playlist_tracks(test_plst)
test_df = my_spotify.create_tracks_data_frame(tracks_items = test_plst_tracks,
                                              audio_features_names = ['instrumentalness', 'energy', 'danceability', 'acousticness', 'tempo'])

x = 1
