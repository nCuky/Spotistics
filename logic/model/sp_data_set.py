import numpy as np
import pandas as pd
import re
import os.path
from pathlib import Path
from logic.frontend import log
from logic.db import db
import json
from logic.model.sp_data_set_names import SPDT as spdtnm


class SpotifyDataSet:
    """
    Manages Spotify Data as DataFrame.
    """
    TRACK_ID_COMBO_COL = [spdtnm.TRACK_NAME,
                          spdtnm.ALBUM_ARTIST_NAME,
                          spdtnm.ALBUM_NAME]

    COLUMNS_TO_RENAME = {'ts'                               : spdtnm.TIMESTAMP,
                         'spotify_track_uri'                : spdtnm.TRACK_URI,
                         'master_metadata_track_name'       : spdtnm.TRACK_NAME,
                         'master_metadata_album_artist_name': spdtnm.ALBUM_ARTIST_NAME,
                         'master_metadata_album_album_name' : spdtnm.ALBUM_NAME}

    DEFAULT_JSON_FILE_PATH = '../../data/personal_data/raw_json'
    DEFAULT_JSON_FILE_PREFIX = 'endsong'

    # region Utility Methods

    @staticmethod
    def clean_json_for_public_repo():
        """
        Small utility meant to be performed only once.

        Removes private data (such as platform, IP address, etc.) from the listen history JSON files,
        so they can be included as sample data in the GitHub repo.

        Returns:
            None
        """
        files_list = [filename for filename in os.listdir(SpotifyDataSet.DEFAULT_JSON_FILE_PATH) if
                      re.match(string = filename, pattern = f'{SpotifyDataSet.DEFAULT_JSON_FILE_PREFIX}.*.json')]

        # Trying to read all 'Listen History' files that are available in the given folder:
        for filename in files_list:
            source_file_path = SpotifyDataSet.DEFAULT_JSON_FILE_PATH + '/' + filename

            if os.path.isfile(source_file_path):
                with open(source_file_path, "rt", encoding = 'utf-8') as file_to_read:
                    json_data = json.load(file_to_read)
                    cleaned_json_data = []

                    for item in json_data:
                        cleaned_item = item
                        cleaned_item[spdtnm.PLATFORM] = ''
                        cleaned_item[spdtnm.IP_ADDRESS] = ''
                        cleaned_item[spdtnm.USER_AGENT] = ''

                        cleaned_json_data.append(cleaned_item)

                dest_file_path = SpotifyDataSet.DEFAULT_JSON_FILE_PATH + '/cleaned'
                Path(dest_file_path).mkdir(parents = True, exist_ok = True)
                dest_file_path += '/' + filename

                with open(dest_file_path, 'wt', encoding = 'utf-8') as file_to_write:
                    json.dump(cleaned_json_data, file_to_write, indent = 4)

    @staticmethod
    def collect_all_listen_history(folder_path: str = None,
                                   filename_prefix: str = DEFAULT_JSON_FILE_PREFIX) -> pd.DataFrame:
        """
        Reads all Listen History data, either from an existing DB,
        or from JSON files (already requested and downloaded from Spotify) contained in the given
        folder. Prepares and organizes data into a single DataFrame.

        Parameters:
            folder_path: Path to a folder containing at least one Spotify Listen History JSON file.
                Filenames should already start with the given filename_prefix (e.g. 'endsong.json' or 'endsong_3.json').

            filename_prefix: Prefix for the names of the desired files to read.

        Returns:
            DataFrame of all Listen history, sorted by timestamp of listening (ascending) and Milliseconds played
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
        **Sorts** the records by timestamp and ms_played (both ascending);

        Adds a **TrackID column** (extracted from the SpotifyTrackURI), renames some columns to friendlier names,
        and removes other unwanted columns;

        Removes **podcast-episodes** listens, removes listens with no TrackID, and removes **duplicated listens** of
        the same track in the same timestamp;

        Takes care of other edge-cases.

        Parameters:
            listen_history_df: Source DataFrame with the Listen History.

        Returns:
            DataFrame with the prepared Tracks Listen History.
        """
        prepped_df = listen_history_df.copy()

        # Removing irrelevant columns:
        prepped_df = prepped_df.drop(
            columns = [spdtnm.IP_ADDRESS, spdtnm.USER_AGENT,
                       spdtnm.EPISODE_NAME, spdtnm.EPISODE_SHOW_NAME, spdtnm.EPISODE_URI],
            inplace = False)

        prepped_df = prepped_df.rename(columns = SpotifyDataSet.COLUMNS_TO_RENAME,
                                       inplace = False)

        # Sorting the DataFrame by username, then by timestamp of listening, then by Milliseconds Played.
        # 1. Timestamp of listening is NOT unique. Sometimes, in a certain timestamp, multiple tracks were played,
        #    or the same track multiple times. Usually, most of these instances would have ms_played = 0.
        # 2. Because there are multiple source JSON files, each one of them has its own index starting from 0,
        #    which causes duplicate index values in the final DataFrame.
        #    This is why ignore_index = True is needed here:
        prepped_df = prepped_df.sort_values(by = [spdtnm.USERNAME, spdtnm.TIMESTAMP, spdtnm.MS_PLAYED],
                                            ascending = True,
                                            ignore_index = True,
                                            inplace = False, )

        # Removing listen-records with no SpotifyURI (i.e. podcasts episodes, and errors in the data)
        prepped_df = prepped_df.drop(
            prepped_df.index[prepped_df[spdtnm.TRACK_URI].isnull()], inplace = False)

        SpotifyDataSet.add_track_id_column(prepped_df)

        # Errors in the source data can cause multiple tracks (the same one or different ones) to be listed in the
        # exact same timestamp. In a certain timestamp, I want to keep different listened tracks, but remove
        # the same track if played multiple times.
        # Here, I'm keeping only the last ms_played value (should be maximal) of each duplicate-tracks-cluster:
        prepped_df = prepped_df.drop_duplicates(subset = [spdtnm.TIMESTAMP, spdtnm.USERNAME, spdtnm.TRACK_ID],
                                                keep = 'last', inplace = False)

        # Edge-case: Artist "Joey Bada$$" causes errors, because matplotlib interprets '$$' as math text.
        # Replacing all "$$" with "\$\$":
        prepped_df[spdtnm.ALBUM_ARTIST_NAME] = prepped_df[spdtnm.ALBUM_ARTIST_NAME].replace('\$\$', '\\$\\$',
                                                                                            inplace = False)

        return prepped_df

    @staticmethod
    def add_track_id_column(updated_df: pd.DataFrame) -> None:
        """
        Adds a 'track_id' column based on the values of `spotify_track_uri` column.

        Parameters:
            updated_df: DataFrame with tracks listen history.

        Returns:
            An updated DataFrame with additional column 'track_id'.
        """
        col_idx_to_insert = updated_df.columns.get_loc(spdtnm.TRACK_URI) + 1

        track_ids = updated_df[spdtnm.TRACK_URI].replace(to_replace = 'spotify:track:',
                                                         value = '',
                                                         regex = True)

        updated_df.insert(loc = col_idx_to_insert,
                          column = spdtnm.TRACK_ID,
                          value = track_ids)

    @staticmethod
    def prepare_audio_analysis_data(updated_df: pd.DataFrame) -> None:
        """
        Prepares a given **Audio Features** DataFrame for human readability:
        Recodes key and mode fields to accepted musical terminology letters.

        This method **changes the given DataFrame** (Inplace = True).

        Parameters:
            updated_df: DataFrame to prepare.

        Returns:
            None (The given DataFrame is changed inplace).
        """
        modes_replacement_dict = {'from': [0, 1],
                                  'to'  : ['m', 'M']}

        keys_replacement_dict = {'from': np.arange(start = 0, stop = 12, step = 1),
                                 'to'  : ['C', 'C#', 'D', 'D#', 'E', 'F', 'F#', 'G', 'G#', 'A', 'A#', 'B']}

        # Preparing the song's Mode (Modus, i.e. Major (uppercase 'M') / Minor (lowercase 'm'):
        updated_df[spdtnm.SONG_MODE] = updated_df[spdtnm.SONG_MODE].replace(
            to_replace = modes_replacement_dict['from'],
            value = modes_replacement_dict['to'])

        # Preparing the song's Key (e.g. 'C#'):
        updated_df[spdtnm.SONG_KEY] = updated_df[spdtnm.SONG_KEY].replace(
            to_replace = keys_replacement_dict['from'],
            value = keys_replacement_dict['to'])

        updated_df[spdtnm.SONG_FULL_KEY] = updated_df[spdtnm.SONG_KEY] + updated_df[spdtnm.SONG_MODE]

    # endregion Utility Methods

    # region Instantiation Logic

    def __init__(self,
                 db_handler: db.DB = None,
                 data_dir: str = DEFAULT_JSON_FILE_PATH):
        """
        Initializes a dataset for managing the listen history and related data.
        This dataset can come either from Spotify JSON files, or from a given DB.

        Parameters:
            db_handler: DB Handler object, from which to fetch the data. If supplied, fetches the data from it.
                Otherwise, reads JSON files from the `data_dir` folder and calls the API to complete the missing data.

            data_dir: Directory of the JSON files to read, if `db_handler` was not supplied.
        """
        self.__db_handler = db_handler
        self._data_dir = data_dir
        self.__listen_history_df: pd.DataFrame = self.__init_listen_history_df()
        self._tracks_df: pd.DataFrame = None
        self._albums_df: pd.DataFrame = None
        self._artists_df: pd.DataFrame = None

    def __init_listen_history_df(self) -> pd.DataFrame:
        """
        Initializes the Listen History DataFrame.

        Returns:
            Listen History DataFrame.
        """
        if self.__db_handler is None:
            self.__listen_history_df = SpotifyDataSet.collect_all_listen_history(folder_path = self._data_dir)
            self.__listen_history_df = SpotifyDataSet.prepare_track_listen_history(self.__listen_history_df)

        else:
            self.__listen_history_df = self.__db_handler.get_listen_history_df()

        return self.__listen_history_df

    @property
    def listen_history_df(self) -> pd.DataFrame:
        """
        Returns the currently worked upon Listen History DataFrame.

        A "singleton getter", when called for the first time it collects the data, prepares it and saves it
        as an instance attribute. Afterwards, it returns the existing attribute.

        Returns:
            Listen History DataFrame.
        """
        if self.__listen_history_df is None:
            self.__init_listen_history_df()

        return self.__listen_history_df

    # endregion Instantiation Logic

    def get_listen_history_album_artist_aggd(self, aggfunc = pd.Series) -> pd.DataFrame:
        """
        When a listened track in a certain timestamp has multiple Album Artists, it is fetched in multiple rows,
        each row contains a different album artist but the rest of the fields are the same.

        This method aggregates the dataset so that each instance of a listened track appears only once, with all of its
        Album Artists joined into a single collection in the ``Album Artist Name`` field.

        Parameters:
            aggfunc: Type of collection to aggregate the Album Artists into.
                Example values: pd.Series | list | set

        Returns:
            DataFrame with the aggregated listen history.
        """
        group_columns_map = {column: 'first' for column in self.listen_history_df.columns}
        group_columns_map[spdtnm.ALBUM_ARTIST_NAME] = aggfunc

        return self.listen_history_df.groupby([spdtnm.USERNAME,
                                               spdtnm.TIMESTAMP,
                                               spdtnm.TRACK_KNOWN_ID],
                                              as_index = False).agg(group_columns_map)

    def add_track_known_id(self, known_tracks_ids_map: dict) -> None:
        """
        Adds a column with the Known Track ID for each track in the listen history.

        Parameters:
            known_tracks_ids_map: Dictionary mapping each TrackID from the listen history to its KnownID.

        Returns:
            None.
        """
        col_idx_to_insert = self.__listen_history_df.columns.get_loc(spdtnm.TRACK_ID) + 1

        # Making sure that any track_id values that are missing in the mapping get mapped to themselves:
        unique_track_ids = self.__listen_history_df[spdtnm.TRACK_ID].unique()
        updated_map = dict(zip(unique_track_ids, unique_track_ids))
        updated_map.update(known_tracks_ids_map)

        column_known_track_id = self.__listen_history_df[spdtnm.TRACK_ID].map(updated_map)

        self.__listen_history_df.insert(loc = col_idx_to_insert,
                                        column = spdtnm.TRACK_KNOWN_ID,
                                        value = column_known_track_id)

    def get_distinct_tracks(self) -> pd.DataFrame:
        """
        Return distinct tracks, based on the KnownTrackID column (must make sure beforehand that it exists!).

        Returns:
            DataFrame, containing the unique instance of each track.
        """
        unique_tracks = self.__listen_history_df.drop_duplicates(
            subset = spdtnm.TRACK_KNOWN_ID,
            keep = 'first')

        unique_tracks.sort_values(by = spdtnm.TIMESTAMP,
                                  ascending = True,
                                  inplace = True)

        return unique_tracks
