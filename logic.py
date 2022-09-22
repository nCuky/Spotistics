from spotify_api_client import SpotifyAPIClient as spapi
import spotify_data_set as spdt
from datetime import datetime as dt
import pandas as pd
import tekore as tk
import log
# import pyspark as sk
import insert_tracks as db


class Logic:
    # region Utility Methods

    @staticmethod
    def get_token(token_path = None):
        if token_path is None:
            token_path = "token"
        token_file = open(token_path, "r")
        token_file_text = token_file.readlines()
        token_file.close()

        return token_file_text

    @staticmethod
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

    @staticmethod
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

    @staticmethod
    def get_tracks_names(tracks_items: list):
        out = list()

        for trk in tracks_items:
            out.append(trk['track']['name'])

        return out

    @staticmethod
    def get_tracks_artists(tracks_items: list):
        out = list()

        for trk in tracks_items:
            curr_artists = dict()

            for i, artist in enumerate(trk['track']['artists']):
                # if artist['name'] == 'Henry Purcell':
                #     x = 1

                curr_artists['artist_' + str(i)] = trk['track']['artists'][i]['name']

            out.append(curr_artists)

        return out

    @staticmethod
    def write_df_to_file(df: pd.DataFrame, file_name: str) -> None:
        """
        Writes a given DataFrame to a file.

        :param df: DataFrame to write to a file.
        :param file_name: Name of the desired file to write (without preceding path).
        :return: None.
        """
        file_path = 'data/personal_data/prepared/' + file_name.format(dt.now().strftime("%Y-%m-%d_%H-%M-%S"))
        log.write(log.WRITING_FILE.format(file_path))
        df.to_csv(path_or_buf = file_path,
                  encoding = 'utf-8-sig',  # UTF-8, explicitly signed with a BOM at the start of the file
                  index = False)
        log.write(log.FILE_WRITTEN.format(file_path))

        # Writing to Excel doesn't work yet.

    # endregion Utility Methods

    def __init__(self):
        self.my_spapi = spapi(token = Logic.get_token())
        self.my_spdt: spdt.SpotifyDataSet = None
        self.my_db = db.DB()

    def get_artist_audio_features_data(self, name: str):
        artist_id = self.my_spapi.find_artist(name).id
        tracks = self.my_spapi.artist_get_all_tracks(artist_id)
        tracks_ids = [track.id for track in tracks]
        tracks_features = self.my_spapi.get_tracks_audio_features(tracks_ids)

        pd.read_json()

        return tracks_features

    def calc_listen_data_by_key(self) -> None:
        '''
        Aggregates all listened tracks by key, and writes it as a csv file
        :return:
        '''
        self.my_spdt = spdt.SpotifyDataSet(aggr_level = spdt.SpotifyDataSet.AGG_LEVEL_TRACK)
        only_duration = self.my_spdt.get_tracks_listen_data().groupby(spdt.ColNames.SONG_KEY)[
            spdt.ColNames.MS_PLAYED].sum()

        my_results = self.my_spdt.get_tracks_listen_data().drop(columns = spdt.ColNames.MS_PLAYED).groupby(
            spdt.ColNames.SONG_KEY).mean().assign(
            duration_ms = only_duration)

        Logic.write_df_to_file(my_results, "listen_data_by_key.csv")

    def calc_listen_data_mean_key(self):
        '''
        Aggregates all listened tracks by mean key
        :return:
        '''
        self.my_spdt = spdt.SpotifyDataSet(aggr_level = spdt.SpotifyDataSet.AGG_LEVEL_TRACK)
        self.my_spdt.get_tracks_listen_data().groupby(spdt.ColNames.SONG_KEY).mean().to_csv("mean_by_key.csv")

    def collect_all_tracks_to_file(self):
        self.my_spdt = spdt.SpotifyDataSet(aggr_level = spdt.SpotifyDataSet.AGG_LEVEL_TRACK)

        # my_spdt.all_tracks_df
        track_data = self.my_spdt.get_tracks_listen_data()

        # Writing to CSV file:
        track_file_name = 'all_tracks_raw_{0}.csv'

        Logic.write_df_to_file(track_data, track_file_name)

    def collect_known_tracks_and_save(self):
        self.my_spdt = spdt.SpotifyDataSet(aggr_level = spdt.SpotifyDataSet.AGG_LEVEL_TRACK)
        full_tracks_mdlist = self.my_spapi.get_full_tracks(tracks = self.my_spdt.get_tracks_listen_data()[
            spdt.ColNames.TRACK_ID])

        self.save_full_tracks_to_db(full_tracks_mdlist)

        # Writing to CSV file:
        track_file_name = 'known_tracks_{0}.csv'

        Logic.write_df_to_file(self.my_spdt.get_tracks_listen_data(), track_file_name)

    def count_unique_tracks(self) -> pd.DataFrame:
        tracks_df = self.my_spdt.get_tracks_listen_data()

        # Removing all records of tracks that were played exactly 0 milliseconds:
        # tracks_df = tracks_df.drop(index = tracks_df.index[tracks_df[spdt.ColNames.MS_PLAYED].eq(0)], inplace = False)

        tracks_count = tracks_df.groupby(spdt.ColNames.TRACK_KNOWN_ID,
                                         as_index = False).agg(
            times_listened = (spdt.ColNames.TRACK_KNOWN_ID, 'count'),
            album_artist_name = (spdt.ColNames.ALBUM_ARTIST_NAME, 'first'),
            album_name = (spdt.ColNames.ALBUM_NAME, 'first'),
            track_name = (spdt.ColNames.TRACK_NAME, 'first'))

        return tracks_count

    def save_full_tracks_to_db(self, full_tracks: tk.model.ModelList[tk.model.FullTrack]) -> None:
        """
        From a given :class:`tk.model.ModelList` of :class:`tk.model.FullTrack` objects, extract the data according to the data model and
        save it into the DB.

        :param full_tracks: A ModelList of FullTrack objects.
        :return: None.
        """
        track_known_id_map = self.my_spapi.get_track_known_id_map(full_tracks = full_tracks)
        self.my_spdt.add_track_known_id(track_known_id_map)

        all_simple_artists: pd.DataFrame = None
        all_simple_albums: pd.DataFrame = None
        all_tracks_of_albums: pd.DataFrame = None
        all_albums_of_artists: pd.DataFrame = None

        for track in full_tracks:
            # Collecting all Artists:
            # track_artists = self.extract_simple_artists(track)
            # pd.concat([all_simple_artists, track_artists])

            # Collecting all Albums:
            album = pd.Series({'album_id'              : track.album.id,
                               'href'                  : track.album.href,
                               'uri'                   : track.album.uri,
                               'name'                  : track.album.name,
                               'album_type'            : track.album.album_type,
                               'total_tracks'          : track.album.total_tracks,
                               'release_date'          : track.album.release_date,
                               'release_date_precision': track.album.release_date_precision})

            pd.concat([all_simple_albums, album.to_frame().T], ignore_index = True)

            # Collecting all Tracks-of-Albums:
            track_of_album = pd.Series({'album_id': track.album.id, 'track_id': track.id})
            pd.concat([all_tracks_of_albums, track_of_album.to_frame().T], ignore_index = True)

            for artist in track.artists:
                # Collecting all Artists that belong to the track:
                all_simple_artists = pd.concat([all_simple_artists,
                                                pd.DataFrame([artist.id, artist.href, artist.uri,
                                                              artist.name, None, None],
                                                             columns = ['artist_id', 'href', 'uri',
                                                                        'name', 'total_followers', 'popularity'])],
                                               ignore_index = True)

                # Collecting Albums-of-Artists.
                # Only Track's Artists that also belong to the Track's Album's Artists
                # (not in an 'Appears On' relationship) are collected.
                if (artist in track.album.artists) & (
                        track.album.album_group is not None &
                        track.album.album_group != tk.model.AlbumGroup.appears_on):
                    album_of_artist = pd.Series({'artist_id': artist.id, 'album_id': track.album.id})
                    pd.concat([all_albums_of_artists, album_of_artist.to_frame().T], ignore_index = True)

            self.my_db.insert_track_full(track)

        all_simple_artists.drop_duplicates(subset = 'artist_id', inplace = True)
        all_simple_albums.drop_duplicates(subset = 'album_id', inplace = True)
        all_tracks_of_albums.drop_duplicates(subset = ['album_id', 'track_id'], inplace = True)
        all_albums_of_artists.drop_duplicates(subset = ['artist_id', 'album_id'], inplace = True)

        self.my_db.insert_listen_history(self.my_spdt.get_tracks_listen_data())

        self.my_db.commit()

    @staticmethod
    def extract_simple_artists(full_track: tk.model.FullTrack) -> pd.DataFrame:
        """From a given :class:`tk.model.FullTrack` object, extract its Artists and return them in a DataFrame.

        :param full_track: FullTrack object.
        :return: DataFrame with all the track's artists."""
        artists_df = pd.concat([pd.DataFrame([artist.id, artist.href, artist.uri, artist.name, None, None],
                                             columns = ['artist_id', 'href', 'uri', 'name', 'total_followers',
                                                        'popularity'])
                                for artist in full_track.artists], ignore_index = True)

        artists_df.drop_duplicates(subset = 'artist_id', inplace = True)

        return artists_df

    @staticmethod
    def extract_simple_album(full_track: tk.model.FullTrack) -> pd.Series:
        """From a given :class:`tk.model.FullTrack` object, extract its Album and return it in a Series.

        :param full_track: FullTrack object.
        :return: Series with the track's Album."""
        album = pd.Series({'album_id'              : full_track.album.id,
                           'href'                  : full_track.album.href,
                           'uri'                   : full_track.album.uri,
                           'name'                  : full_track.album.name,
                           'album_type'            : full_track.album.album_type,
                           'total_tracks'          : full_track.album.total_tracks,
                           'release_date'          : full_track.album.release_date,
                           'release_date_precision': full_track.album.release_date_precision})

        return album
