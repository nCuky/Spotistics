import spotipy as sp
from spotipy.oauth2 import SpotifyOAuth
from spotipy.oauth2 import SpotifyClientCredentials
import os
import pandas as pd

MAX_TRACKS_FOR_FEATURES = 100
MAX_TRACKS_FOR_PLAYLIST_ITEMS = 100
AUTH_SCOPE = "user-library-read playlist-read-collaborative playlist-read-private"


class SpClient:
    def __init__(self):
        pass
    pass


def get_token(token_path = None):
    if token_path is None:
        token_path = "token"
    token_file = open(token_path, "r")
    token_file_text = token_file.readlines()
    token_file.close()
    return token_file_text


token = get_token()
os.environ['SPOTIPY_CLIENT_ID'] = token[0].strip()
os.environ['SPOTIPY_CLIENT_SECRET'] = token[1].strip()
os.environ['SPOTIPY_REDIRECT_URI'] = 'http://localhost:8888/spotify/callback'


def get_client_object(auth_scope):
    return sp.Spotify(client_credentials_manager = SpotifyClientCredentials(),
                      auth_manager = SpotifyOAuth(scope = auth_scope))


def get_all_user_playlists(client, limit = 50):
    response = client.current_user_playlists(limit = limit)
    results = response['items']
    offset = limit
    while response['next'] is not None:
        response = client.current_user_playlists(limit = limit, offset = offset)
        results.extend(response['items'])
        offset += limit
    return results


def get_all_playlist_tracks(client, playlist_id: list, limit = MAX_TRACKS_FOR_PLAYLIST_ITEMS):
    response = client.playlist_tracks(playlist_id, limit = limit)
    results = response['items']
    offset = limit
    while response['next'] is not None:
        response = client.playlist_tracks(playlist_id, limit = limit,
                                                  offset = offset)
        results.extend(response['items'])
        offset += limit
    return results


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


def get_tracks_audio_features(client, tracks_items: list, limit = MAX_TRACKS_FOR_FEATURES):
    '''
    :param client:
    :param tracks_items:
    :param limit:
    :return:
    '''
    ids = get_tracks_ids(tracks_items)
    result = list()

    while len(ids) > 0:
        result.extend(client.audio_features(ids[0:limit]))

        # Removing the first {limit} items from the list:
        ids = ids[limit:]

    return result


def get_specific_audio_feature(client, tracks_items: list, audio_feature: str, limit = MAX_TRACKS_FOR_FEATURES):
    '''
    Returns a list of a desired audio feature for all given tracks.
    :param client:
    :param tracks_items:
    :param audio_feature:
    :param limit:
    :return:
    '''
    tracks_audio_features = get_tracks_audio_features(client, tracks_items, limit)
    result = list()

    for track in tracks_audio_features:
        # for audio_feature in audio_features
        result.append(track[audio_feature])

    return result


def create_tracks_data_frame(client, tracks_items: list, audio_features_names: list, limit = MAX_TRACKS_FOR_FEATURES):
    tracks_with_features = {'track_name': get_tracks_names(tracks_items)}

    for feat_name in audio_features_names:
        tracks_with_features[feat_name] = get_specific_audio_feature(client, tracks_items, audio_feature = feat_name, limit = limit)

    return pd.DataFrame(data = tracks_with_features)


spf_client = get_client_object("user-library-read playlist-read-collaborative playlist-read-private")
user_playlists = get_all_user_playlists(spf_client)
test_plst = find_playlist(user_playlists, "ProgPsy Rock and Neo Psy Rock")
test_plst_tracks = get_all_playlist_tracks(spf_client, test_plst)

test_df = create_tracks_data_frame(client = spf_client, tracks_items = test_plst_tracks, audio_features_names = ['instrumentalness', 'energy', 'danceability', 'acousticness', 'tempo'])

x = 1