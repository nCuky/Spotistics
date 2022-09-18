import numpy as np
import pandas as pd
import re
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
    def collect_all_listen_history(folder_path: str, filename_prefix: str = 'endsong') -> pd.DataFrame:
        """
        Reads all Listen History (tracks and episodes) from JSON files received from Spotify and contained in the given
        folder, and collects them into a single DataFrame.
        :param folder_path: Path to a folder containing at least one Spotify Listen History JSON file.
        Filenames should already start with the given filename_prefix (e.g. 'endsong.json' or 'endsong_3.json').
        :param filename_prefix: Filename Prefix for the desired files to read.
        :return: DataFrame of all Listen history, sorted by timestamp of listening (ascending) and Milliseconds played
        (ascending).
        """
        all_listens_df: pd.DataFrame = None
        files_list = [filename for filename in os.listdir(folder_path) if
                      re.match(string = filename, pattern = f'{filename_prefix}.*.json')]

        # Trying to read all 'Listen History' files that are available in the given folder:
        for filename in files_list:
            curr_listen_json_df = None
            file_path = folder_path + '/' + filename

            if os.path.isfile(file_path):
                log.write(log.READING_FILE.format(filename))

                curr_listen_json_df = pd.read_json(file_path, encoding = 'utf-8')

            if curr_listen_json_df is None:
                log.write(log.NONEXISTENT_FILE.format(filename))

            else:
                all_listens_df = curr_listen_json_df.copy() if all_listens_df is None \
                    else pd.concat([all_listens_df, curr_listen_json_df])

        return all_listens_df

    @staticmethod
    def prepare_track_listen_history(listen_history_df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans up, sorts and prepares a Tracks-only Listen History DataFrame for working upon.
        Sorts the records by timestamp and ms_played (both ascending);
        Adds a TrackID column (extracted from the SpotifyTrackURI), renames some columns to friendlier names,
        and removes other unwanted columns;
        Removes podcast-episodes listens, listens without any TrackID, and duplicated listens of the same track in
        the same timestamp;
        Takes care of other edge-cases.
        :param listen_history_df: Source DataFrame with the Listen History.
        :return: DataFrame with the prepared Tracks Listen History.
        """
        prepped_df = listen_history_df.copy()

        # Sorting the DataFrame by timestamp of listening, then by Milliseconds Played.
        # 1. Timestamp of listening is NOT unique. Sometimes, in a certain timestamp, multiple tracks were played,
        # or the same track multiple times. Usually, most of these instances would have ms_played = 0.
        # 2. Because there are multiple source JSON files, each one of them has its own index starting from 0,
        # which causes duplicate index values in the final DataFrame.
        # This is why ignore_index = True is needed here:
        prepped_df = prepped_df.sort_values(by = [ColNames.TIMESTAMP, ColNames.MS_PLAYED],
                                            ascending = True,
                                            ignore_index = True,
                                            inplace = False,)

        # Removing irrelevant columns:
        prepped_df = prepped_df.drop(columns = [ColNames.IP_ADDRESS, ColNames.USER_AGENT],
                                     inplace = False)

        # Removing listen-records with no SpotifyURI (i.e. podcasts episodes, and errors in the data)
        prepped_df = prepped_df.drop(
            prepped_df.index[prepped_df[ColNames.TRACK_URI].isnull()], inplace = False)

        SpotifyDataSet.add_track_id_column(prepped_df)

        # Errors in the source data can cause multiple tracks (the same one or different ones) to be listed in the
        # exact same timestamp. In a certain timestamp, I want to keep different listened tracks, but remove
        # the same track if played multiple times.
        # Here, I'm keeping only the last ms_played value (should be maximal) of each duplicate-tracks-cluster:
        prepped_df = prepped_df.drop_duplicates(subset = [ColNames.TIMESTAMP, ColNames.USERNAME, ColNames.TRACK_ID],
                                                keep = 'last', inplace = False)

        prepped_df = prepped_df.rename(columns = SpotifyDataSet.COLUMNS_TO_RENAME,
                                       inplace = False)

        # Edge-case: Artist "Joey Bada$$" causes errors, because matplotlib interprets '$$' as math text.
        # Replacing all "$$" with "\$\$":
        prepped_df[ColNames.ALBUM_ARTIST_NAME] = prepped_df[ColNames.ALBUM_ARTIST_NAME].replace('\$\$', '\\$\\$',
                                                                                                inplace = False)

        return prepped_df

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
            self.all_tracks_df = SpotifyDataSet.collect_all_listen_history(folder_path = self.data_dir)
            self.all_tracks_df = SpotifyDataSet.prepare_track_listen_history(listen_history_df = self.all_tracks_df)

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
