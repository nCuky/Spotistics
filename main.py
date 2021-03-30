import spotipy as sp
from spotipy.oauth2 import SpotifyOAuth
from spotipy.oauth2 import SpotifyClientCredentials
import os

MAX_TRACKS_FOR_FEATURES = 100
MAX_TRACKS_FOR_PLAYLIST_ITEMS = 100
AUTH_SCOPE = "user-library-read playlist-read-collaborative playlist-read-private"


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


def get_all_playlist_tracks(client, playlist_id: list, limit = 100):
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


def get_tracks_ids(tracks: list):
    out = list()
    for trk in tracks:
        out.append(trk['track']['id'])
    return out


def get_tracks_audio_features(client, tracks_items: list, limit = 100):
    ids = get_tracks_ids(tracks_items)
    response = client.audio_features(ids)


spf_client = get_client_object("user-library-read playlist-read-collaborative playlist-read-private")
user_playlists = get_all_user_playlists(spf_client)
erez_nadav_plst = find_playlist(user_playlists, "Erez and Nadav")
plst_tracks = get_all_playlist_tracks(spf_client, erez_nadav_plst)

x = 1
