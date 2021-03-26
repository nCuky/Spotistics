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


spf_client = get_client_object("user-library-read playlist-read-collaborative playlist-read-private")


user_playlists = get_all_user_playlists(spf_client, limit = 5)
