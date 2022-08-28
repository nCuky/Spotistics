from spotify_api_client import SpotifyAPIClient as spapi
from spotify_data_set import SpotifyDataSet as spdt
from datetime import datetime as dt
import pandas as pd
import log


# import pyspark as sk
# import seaborn as sns  # Plotting


def get_token(token_path = None):
    if token_path is None:
        token_path = "token"
    token_file = open(token_path, "r")
    token_file_text = token_file.readlines()
    token_file.close()
    return token_file_text


spapic = spapi(token = get_token())
my_spdt: spdt


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
    my_spoti_data = spdt(aggr_level = 'track')
    only_duration = my_spoti_data.data.groupby('key')['duration_ms'].sum()
    my_results = my_spoti_data.data.drop(columns = 'duration_ms').groupby('key').mean().assign(
        duration_ms = only_duration)
    my_results.to_csv("listen_data_by_key.csv", encoding = 'utf-8-sig')


def calc_listen_data_mean_key():
    '''
    Aggregates all listened tracks by mean key
    :return:
    '''
    my_spoti_data = spdt(aggr_level = 'track')
    my_spoti_data.data.groupby('key').mean().to_csv("mean_by_key.csv")


def collect_all_tracks_to_file():
    my_spdt = spdt(aggr_level = 'track')

    # my_spdt.all_tracks_json_df
    track_data = my_spdt.get_tracks_listen_data()

    # Writing to CSV file:
    track_file_name = 'all_my_tracks_{0}.csv'

    write_df_to_file(track_data, track_file_name)


def collect_unique_tracks_triplets_to_file():
    my_spdt = spdt(aggr_level = 'track')

    # my_spdt.all_tracks_json_df
    track_data = my_spdt.get_unique_tracks(
        by_column = ['master_metadata_track_name',
                     'master_metadata_album_artist_name',
                     'master_metadata_album_album_name'])

    # Writing to CSV file:
    track_file_name = 'unique_tracks_triplets_{0}.csv'

    write_df_to_file(track_data, track_file_name)


def write_df_to_file(df: pd.DataFrame, file_name: str) -> None:
    """
    Writes a given DataFrame to a file.
    :param file_name: Name of the desired file to write (without preceding path).
    :return: None.
    """
    file_path = 'data/personal_data/prepared/' + file_name.format(dt.now().strftime("%Y-%m-%d_%H-%M-%S"))
    log.write(log.WRITING_FILE.format(file_path))
    df.to_csv(path_or_buf = file_path,
              encoding = 'utf-8-sig',
              index = False)
    log.write(log.FILE_WRITTEN.format(file_path))

    # Writing to Excel doesn't work yet.
