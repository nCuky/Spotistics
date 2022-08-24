import numpy as np
import pandas as pd
import os.path


def prepare_audio_analysis_data(df_to_prepare):
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


class SpotifyDataSet:
    def __init__(self, aggr_level='track', data_dir='data/personal_data/raw_json'):
        """
        Reads data from Spotify JSON data files into a parse-able dataframe.
        Parameters
        ----------
        aggr_level: Which dataset to load ('track', 'artist', 'genres', 'year', 'w_genres').
        data_dir: Directory of the data files. Can be 'data/spoti_data' for the downloaded sample,
        or 'data/personal_data' for my personal account data.
        """
        track_file_idx = 0
        self.all_tracks_json_df: pd.DataFrame() = None

        if aggr_level == 'track':
            while track_file_idx >= 0:
                curr_tracks_json_df = None

                track_file_name = "endsong_{0}.json".format(track_file_idx)
                file_path = data_dir + '/' + track_file_name

                if os.path.isfile(file_path):
                    curr_tracks_json_df = pd.read_json(file_path)

                if curr_tracks_json_df is None:
                    track_file_idx = -1

                else:
                    self.all_tracks_json_df = curr_tracks_json_df if self.all_tracks_json_df is None \
                        else pd.concat([self.all_tracks_json_df, curr_tracks_json_df])

                    # self.data = prepare_audio_analysis_data(curr_tracks_json_df)

                    track_file_idx += 1

    def get_data(self) -> pd.DataFrame:
        return self.all_tracks_json_df


    # def __init__(self, aggr_level='track', data_dir='data/spoti_data'):
    #     '''
    #     Reads data from Spotify CSV data files into a parse-able dataframe.
    #     :param aggr_level: Which dataset to load ('track', 'artist', 'genres', 'year', 'w_genres')
    #     :param data_dir: Directory of the data files. Can be 'data/spoti_data' for the downloaded sample,
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
