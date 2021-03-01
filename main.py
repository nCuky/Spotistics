import spotipy as sp
from spotipy.oauth2 import SpotifyOAuth
from spotipy.oauth2 import SpotifyClientCredentials
import os

MAX_TRACKS_FOR_FEATURES = 100
MAX_TRACKS_FOR_PLAYLIST_ITEMS = 100

def find_playlist(playlists: dict, playlist_name: str):
    """
    Finds a playlist by a given name.
    :return: tracks' id of the found playlist, otherwise None
    """

    for plst in playlists['items']:
        # Reading this way because this is a deep object, cannot find with hash
        if plst['name'] == playlist_name:
            plst_tracks_id = plst['id']
            break

    return plst_tracks_id


def get_tracks_names(tracks):
    out = list()
    for i in tracks['items']:
        out.append(i['track']['name'])
    return out


def get_tracks_ids(tracks):
    out = list()
    for i in tracks['items']:
        out.append(i['track']['id'])
    return out


def get_playlist_items(spf_client, playlist_id):
    playlist_items = spf_client.playlist_items(find_playlist(user_playlists, my_plst_name))


def get_items_list(items: list, max_items_in_chunk: int, spf_method: classmethod, **kwargs):
    total_items_amount = len(items)
    chunks_amount = (total_items_amount // max_items_in_chunk) + 1
    last_100_start = (chunks_amount - 1) * max_items_in_chunk
    last_id_index = total_items_amount - 1
    chunks_list = [None] * (chunks_amount)

    result = list()

    for i, value in enumerate(chunks_list):
        if i == chunks_amount - 1:
            ids_chunk = items[last_100_start:last_id_index]
        else:
            ids_chunk = items[(i * max_items_in_chunk):((i + 1) * max_items_in_chunk - 1)]

        chunks_list[i] = spf_client.audio_features(ids_chunk)
        chunks_list[i] = spf_method(ids_chunk, **kwargs)

        result.extend(chunks_list[i])

    return result


def get_tracks_features(client, tracks):
    ids = get_tracks_ids(tracks)

    n_chunks = (len(ids) // MAX_TRACKS_FOR_FEATURES) + 1
    last_100_start = (n_chunks - 1) * MAX_TRACKS_FOR_FEATURES
    last_id_index = len(ids) - 1
    chunks = [None] * (n_chunks)
    result = list()

    for i, value in enumerate(chunks):
        if i == n_chunks - 1:
            ids_chunk = ids[last_100_start:last_id_index]
        else:
            ids_chunk = ids[(i * MAX_TRACKS_FOR_FEATURES):((i + 1) * MAX_TRACKS_FOR_FEATURES - 1)]
        chunks[i] = client.audio_features(ids_chunk)

        result.extend(chunks[i])

    return result


token_file = open("token", "r")
token_file_text = token_file.readlines()
token_file.close()
os.environ['SPOTIPY_CLIENT_ID'] = token_file_text[0].strip()
os.environ['SPOTIPY_CLIENT_SECRET'] = token_file_text[1].strip()
os.environ['SPOTIPY_REDIRECT_URI'] = 'http://localhost:8888/spotify/callback'

auth_scope = "user-library-read playlist-read-collaborative playlist-read-private"

spf_client = sp.Spotify(client_credentials_manager = SpotifyClientCredentials(),
                        auth_manager = SpotifyOAuth(scope = auth_scope))

user_playlists = spf_client.current_user_playlists()

my_plst_name = 'Erez and Nadav'
# my_plst_name = 'ProgPsy Rock and Neo Psy Rock'

playlist_id = find_playlist(user_playlists, my_plst_name)

our_tracks = spf_client.playlist_items(playlist_id)


# our_tracks = get_items_list(, MAX_TRACKS_FOR_PLAYLIST_ITEMS, playlist_id = )

get_tracks_ids(our_tracks)

our_tracks_features = get_tracks_features(spf_client, our_tracks)