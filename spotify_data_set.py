import numpy as np
import pandas as pd
import os.path
import log


def prepare_audio_analysis_data(df_to_prepare) -> pd.DataFrame:
    """
    Prepares the data for musical analysis, e.g. recodes key and mode fields to human-readable letters.
    :return: The prepared Spotify data.
    """
    modes_replacement_dict = {'from': [0, 1],
                              'to': ['m', 'M']}

    keys_replacement_dict = {'from': np.arange(start=0, stop=12, step=1),
                             'to': ['C', 'C#', 'D', 'D#', 'E', 'F', 'F#', 'G', 'G#', 'A', 'A#', 'B']}

    df_to_prepare['mode'] = df_to_prepare['mode'].replace(to_replace=modes_replacement_dict['from'],
                                                          value=modes_replacement_dict['to'])

    df_to_prepare['key'] = df_to_prepare['key'].replace(to_replace=keys_replacement_dict['from'],
                                                        value=keys_replacement_dict['to'])

    df_to_prepare['full_key'] = df_to_prepare['key'] + df_to_prepare['mode']

    return df_to_prepare


def collect_all_tracks_listen_history(data_dir: str):
    # Collecting all Track listen data from a given folder containing JSON files,
    # and returns them in a single Dataframe, sorted by listen date (ascending):
    track_file_idx = 0
    all_tracks_json_df: pd.DataFrame() = None

    while track_file_idx >= 0:
        curr_tracks_json_df = None

        track_file_name = "endsong_{0}.json".format(track_file_idx)
        file_path = data_dir + '/' + track_file_name

        if os.path.isfile(file_path):
            log.write(log.READING_FILE.format(track_file_name))

            curr_tracks_json_df = pd.read_json(file_path)

        if curr_tracks_json_df is None:
            track_file_idx = -1

            log.write(log.NONEXISTENT_FILE.format(track_file_name))

        else:
            all_tracks_json_df = curr_tracks_json_df if all_tracks_json_df is None \
                else pd.concat([all_tracks_json_df, curr_tracks_json_df])

            track_file_idx += 1

    if all_tracks_json_df is not None:
        all_tracks_json_df.sort_values(by = 'ts',
                                       inplace = True,
                                       ascending = True)

    return all_tracks_json_df


def add_track_id_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a 'track_id' column based on the values of 'spotify_track_uri' column.
    :param df: Dataframe with tracks listen history.
    :return: Updated dataframe with additional column 'track_id'.
    """
    new_df = df

    uri_col_idx = new_df.columns.get_loc('spotify_track_uri')

    track_ids = new_df['spotify_track_uri'].replace(to_replace = 'spotify:track:',
                                              value = '',
                                              regex = True)

    new_df.insert(loc = uri_col_idx + 1, column = 'track_id', value = track_ids)

    return new_df


class SpotifyDataSet:
    def __init__(self, aggr_level='track', data_dir='data/personal_data/raw_json'):
        """
        Reads data from Spotify JSON data files into a parse-able dataframe.
        Parameters
        ----------
        aggr_level: Which dataset to load ('track', 'artist', 'genres', 'year', 'w_genres').
        data_dir: Directory of the data files. Can be 'data/dl_sample_data' for the downloaded sample,
        or 'data/personal_data' for my personal account data.
        """
        self.all_tracks_json_df: pd.DataFrame() = None
        self.data_dir = data_dir

        if aggr_level == 'track':
            self.get_tracks_listen_data(self.data_dir)


    def get_tracks_listen_data(self) -> pd.DataFrame:
        if self.all_tracks_json_df is None:
            self.all_tracks_json_df = collect_all_tracks_listen_history(self.data_dir)
            self.all_tracks_json_df = add_track_id_column(self.all_tracks_json_df)

        return self.all_tracks_json_df

    def get_unique_tracks(self) -> pd.DataFrame:
        """
        Returns IDs of unique tracks that were played, with timestamp set to the first time they were ever played.
        This removed duplicates based on column 'original_track_id' which must be available in the DataFrame.
        :return:
        """
        all_unique_tracks = self.get_tracks_listen_data().copy()
        # all_unique_tracks['guessed_song_id'] = all_unique_tracks.apply(
        #     lambda row: f'{row[7]}_{row[8]}_{row[9]}')
        all_unique_tracks

        all_unique_tracks = self.get_tracks_listen_data().sort_values(by = ['original_track_id', 'ts'], ascending = True)
        all_unique_tracks.drop_duplicates(subset = ['original_track_id'], keep = 'first', inplace = True)
        all_unique_tracks.sort_values(by = 'ts',
                                      inplace = True,
                                      ascending = True)

        return all_unique_tracks


    # def __init__(self, aggr_level='track', data_dir='data/dl_sample_data'):
    #     '''
    #     Reads data from Spotify CSV data files into a parse-able dataframe.
    #     :param aggr_level: Which dataset to load ('track', 'artist', 'genres', 'year', 'w_genres')
    #     :param data_dir: Directory of the data files. Can be 'data/dl_sample_data' for the downloaded sample,
    #     or 'data/personal_data' for my personal account data.
    #     '''
    #     dict_file_names = {'track': "data.csv",
    #                        'artist': "data_by_artist.csv",
    #                        'genres': "data_by_genres.csv",
    #                        'year': "data_by_year.csv",
    #                        'w_genres': "data_w_genres.csv"}
    #
    #     file_path = data_dir + '/' + dict_file_names[aggr_level]
    #     self.data = prepare_audio_analysis_data(pd.read_csv(file_path))
