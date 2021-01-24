import spotipy as sp
from spotipy.oauth2 import SpotifyOAuth
from spotipy.oauth2 import SpotifyClientCredentials
import os

os.environ['SPOTIPY_CLIENT_ID'] = ''
os.environ['SPOTIPY_CLIENT_SECRET'] = ''
os.environ['SPOTIPY_REDIRECT_URI'] = 'http://localhost:8888/spotify/callback'

auth_scope = "user-library-read playlist-read-collaborative playlist-read-private"

spf_client = sp.Spotify(client_credentials_manager = SpotifyClientCredentials(),
                        auth_manager = SpotifyOAuth(scope = auth_scope))

user_playlists = spf_client.current_user_playlists()

print(user_playlists)

my_plst_name = 'Erez and Nadav'


for plst in user_playlists['items']:
    if plst['name'] == my_plst_name:
        plst_tracks_id = plst['id']
        break

our_tracks = spf_client.playlist_items(plst_tracks_id)

def get_tracks_names(tracks):
    out = list()
    for i in tracks['items']:
        out.append(i['track']['name'])
    return out

print(get_tracks_names(our_tracks))