import numpy as np
import pandas as pd
import os.path
import log
from dataclasses import dataclass


@dataclass(frozen = True)
class ColNames:
    TIMESTAMP = 'ts'
    MS_PLAYED = 'ms_played'
    ALBUM_ARTIST_NAME = 'album_artist_name'
    ALBUM_NAME = 'album_name'
    TRACK_NAME = 'track_name'
    TRACK_ID = 'track_id'
    TRACK_KNOWN_ID = 'track_known_id'
    TRACK_URI = 'spotify_track_uri'
    CONN_COUNTRY = 'conn_country'
    TIMES_LISTENED = 'times_listened'
    USERNAME = 'username'
    IP_ADDRESS = 'ip_addr_decrypted'
    USER_AGENT = 'user_agent_decrypted'
    INCOGNITO = 'incognito_mode'
    SONG_MODE = 'mode'
    SONG_KEY = 'key'
    SONG_FULL_KEY = 'full_key'


class SpotifyDataSet:
    """
    Manages Spotify Data (originating from local JSON files), as pandas DataFrames.
    """
    TRACK_ID_COMBO_COL = [ColNames.TRACK_NAME,
                          ColNames.ALBUM_ARTIST_NAME,
                          ColNames.ALBUM_NAME]

    COLUMNS_TO_RENAME = {'master_metadata_track_name'       : ColNames.TRACK_NAME,
                         'master_metadata_album_artist_name': ColNames.ALBUM_ARTIST_NAME,
                         'master_metadata_album_album_name' : ColNames.ALBUM_NAME}

    AGG_LEVEL_TRACK = 'track'

    # region Utility Methods

    @staticmethod
    def collect_all_tracks_listen_history(data_dir: str) -> pd.DataFrame:
        """
        Reads Tracks Listen History from JSON files contained in the given folder, and collects
        them into a single DataFrame, sorted by timestamp of listen (ascending).
        :return: DataFrame of all Tracks Listen history, sorted by ascending timestamp.
        """
        track_file_idx = 0
        all_tracks_json_df: pd.DataFrame = None

        # # Trying to read a pre-combiled JSON file:
        # track_file_name = "endsong_{0}.json".format(track_file_idx)
        # file_path = data_dir + '/' + track_file_name

        # Trying to read all 'Track Listen History' files that are available in the given folder:
        while track_file_idx >= 0:
            curr_tracks_json_df = None

            track_file_name = "endsong_{0}.json".format(track_file_idx)
            file_path = data_dir + '/' + track_file_name

            if os.path.isfile(file_path):
                log.write(log.READING_FILE.format(track_file_name))

                curr_tracks_json_df = pd.read_json(file_path, encoding = 'utf-8')

            if curr_tracks_json_df is None:
                track_file_idx = -1

                log.write(log.NONEXISTENT_FILE.format(track_file_name))

            else:
                all_tracks_json_df = curr_tracks_json_df.copy() if all_tracks_json_df is None \
                    else pd.concat([all_tracks_json_df, curr_tracks_json_df])

                track_file_idx += 1

        if all_tracks_json_df is not None:
            # Sorting the DataFrame by timestamp of listening.
            # Because there are multiple source JSON files, each one of them has its own index starting from 0,
            # which causes duplicate index values in the final DataFrame.
            # This is why ignore_index = True is needed here:
            all_tracks_json_df.sort_values(by = ColNames.TIMESTAMP,  # 'ts'
                                           inplace = True,
                                           ascending = True,
                                           ignore_index = True)

        return all_tracks_json_df

    @staticmethod
    def add_track_id_column(updated_df: pd.DataFrame) -> None:
        """
        Adds a 'track_id' column based on the values of 'spotify_track_uri' column.
        :param updated_df: DataFrame with tracks listen history.
        :return: Updated dataframe with additional column 'track_id'.
        """
        col_idx_to_insert = updated_df.columns.get_loc(ColNames.TRACK_URI) + 1

        track_ids = updated_df[ColNames.TRACK_URI].replace(to_replace = 'spotify:track:',
                                                           value = '',
                                                           regex = True)

        updated_df.insert(loc = col_idx_to_insert,
                          column = ColNames.TRACK_ID,  # 'track_id'
                          value = track_ids)

    @staticmethod
    def rename_master_metadata_columns(updated_df: pd.DataFrame) -> None:
        updated_df.rename(columns = SpotifyDataSet.COLUMNS_TO_RENAME,
                          inplace = True)

    @staticmethod
    def prepare_audio_analysis_data(updated_df: pd.DataFrame) -> None:
        """
        Prepares the data for musical analysis, e.g. recodes key and mode fields to human-readable letters.
        :return: The prepared Spotify data.
        """
        modes_replacement_dict = {'from': [0, 1],
                                  'to'  : ['m', 'M']}

        keys_replacement_dict = {'from': np.arange(start = 0, stop = 12, step = 1),
                                 'to'  : ['C', 'C#', 'D', 'D#', 'E', 'F', 'F#', 'G', 'G#', 'A', 'A#', 'B']}

        # Preparing the song's Mode (Modus, i.e. Major (uppercase 'M') / Minor (lowercase 'm'):
        updated_df[ColNames.SONG_MODE] = updated_df[ColNames.SONG_MODE].replace(
            to_replace = modes_replacement_dict['from'],
            value = modes_replacement_dict['to'])

        # Preparing the song's Key (e.g. 'C#'):
        updated_df[ColNames.SONG_KEY] = updated_df[ColNames.SONG_KEY].replace(
            to_replace = keys_replacement_dict['from'],
            value = keys_replacement_dict['to'])

        updated_df[ColNames.SONG_FULL_KEY] = updated_df[ColNames.SONG_KEY] + updated_df[ColNames.SONG_MODE]

    # endregion Utility Methods

    def __init__(self, aggr_level = AGG_LEVEL_TRACK, data_dir = 'data/personal_data/raw_json'):
        """
        Reads data from Spotify JSON data files into a parse-able dataframe.
        Parameters
        ----------
        aggr_level: Which dataset to load ('track', 'artist', 'genres', 'year', 'w_genres').
        data_dir: Directory of the data files. Can be 'data/dl_sample_data' for the downloaded sample,
        or 'data/personal_data' for my personal account data.
        """
        # self.all_tracks_df: pd.DataFrame
        self.data_dir = data_dir
        self.all_tracks_df: pd.DataFrame = None

        if aggr_level == SpotifyDataSet.AGG_LEVEL_TRACK:
            self.all_tracks_df = self.get_tracks_listen_data()

    # region Instance Logic

    def get_tracks_listen_data(self) -> pd.DataFrame:
        if self.all_tracks_df is None:
            self.all_tracks_df = SpotifyDataSet.collect_all_tracks_listen_history(self.data_dir)

            # Cleaning and preparing the data:
            SpotifyDataSet.add_track_id_column(self.all_tracks_df)
            SpotifyDataSet.rename_master_metadata_columns(self.all_tracks_df)
            self.all_tracks_df.drop(columns = [ColNames.USERNAME, ColNames.IP_ADDRESS, ColNames.USER_AGENT])

            # Edge-case: Artist "Joey Bada$$" causes errors, because matplotlib interprets '$$' as math text.
            # Replacing all "$$" with "\$\$":
            self.all_tracks_df[ColNames.ALBUM_ARTIST_NAME].replace('\$\$', '\\$\\$', inplace = True)

            # self.all_tracks_df.drop(self.all_tracks_df.index[self.all_tracks_df[ColNames.MS_PLAYED].eq(0)],
            # inplace = True)

        return self.all_tracks_df

    def get_distinct_tracks(self) -> pd.DataFrame:
        """
        Return distinct tracks, based on the KnownTrackID column (must make sure beforehand that it exists!).
        :return: DataFrame, containing the unique instance of each track.
        """

        unique_tracks = self.all_tracks_df.drop_duplicates(
            subset = ColNames.TRACK_KNOWN_ID,
            keep = 'first')

        unique_tracks.sort_values(by = ColNames.TIMESTAMP,
                                  ascending = True,
                                  inplace = True)

        return unique_tracks

    def add_known_track_id(self, known_tracks_ids_map: dict) -> None:
        col_idx_to_insert = self.all_tracks_df.columns.get_loc(ColNames.TRACK_ID) + 1

        # Making sure that any track_id values that are missing in the mapping get mapped to themselves:
        unique_track_ids = self.all_tracks_df[ColNames.TRACK_ID].unique()
        updated_map = dict(zip(unique_track_ids, unique_track_ids))
        updated_map.update(known_tracks_ids_map)

        column_known_track_id = self.all_tracks_df[ColNames.TRACK_ID].map(updated_map)

        self.all_tracks_df.insert(loc = col_idx_to_insert,
                                  column = ColNames.TRACK_KNOWN_ID,
                                  value = column_known_track_id)

    # endregion Instance Logic
