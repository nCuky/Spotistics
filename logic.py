from spotify_api_client import SpotifyAPIClient as spapi
import spotify_data_set as spdt
from datetime import datetime as dt
import pandas as pd
import numpy as np
import tekore as tk
import log
# import pyspark as sk
import db
import pickle


class Logic:
    # region Utility Methods

    @staticmethod
    def get_token(token_path = None):
        if token_path is None:
            token_path = "token"
        token_file = open(token_path, "rt")
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

    @staticmethod
    def get_unique_values(dicts: list[dict]) -> list[dict] | None:
        """
        Keep only the unique values in a list of dictionaries. A whole dict is taken as a single "value" to compare
        to others.

        Each dict in the given list **must be flat**, i.e. not an object or a nested dict, because
        they are not supported here.

        Parameters:
            dicts (list[dict]): List of flat dictionaries.

        Returns:
            list[dict] | None: List of only the unique dictionaries, compared by all the values of each dict,
                or None if any dict contains an object or a nested dict.
        """
        unique_dict_list = list(map(dict, set(tuple(d.items()) for d in dicts)))

        return unique_dict_list

        # more info: https://stackoverflow.com/a/19804098/6202667

        # Those didn't work:
        # return list(map(pickle.loads, set(map(pickle.dumps, dicts))))
        # return list(map(dict, frozenset(frozenset(dict_item.items()) for dict_item in dicts)))

        # This returned a slightly smaller number of tracks:
        # return [eval(str_dict) for str_dict in list(np.unique(np.array(dicts).astype(str)))]      # except SyntaxError

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
        """
        Aggregates all listened tracks by key, and writes it as a csv file
        :return:
        """
        self.my_spdt = spdt.SpotifyDataSet(aggr_level = spdt.SpotifyDataSet.AGG_LEVEL_TRACK)
        only_duration = self.my_spdt.get_tracks_listen_data().groupby(spdt.ColNames.SONG_KEY)[
            spdt.ColNames.MS_PLAYED].sum()

        my_results = self.my_spdt.get_tracks_listen_data().drop(columns = spdt.ColNames.MS_PLAYED).groupby(
            spdt.ColNames.SONG_KEY).mean().assign(
            duration_ms = only_duration)

        Logic.write_df_to_file(my_results, "listen_data_by_key.csv")

    def calc_listen_data_mean_key(self):
        """
        Aggregates all listened tracks by mean key
        :return:
        """
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

        try:
            full_tracks_mdlist = self.my_spapi.get_full_tracks(tracks = self.my_spdt.get_tracks_listen_data()[
                spdt.ColNames.TRACK_ID])

            self.save_full_tracks_to_db(full_tracks_mdlist)

            # Writing to CSV file:
            track_file_name = 'known_tracks_{0}.csv'

            Logic.write_df_to_file(self.my_spdt.get_tracks_listen_data(), track_file_name)

        except tk.ServiceUnavailable as ex:
            return

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

        all_tracks_list = []
        all_artists_list = []
        all_albums_list = []
        all_albums_tracks_list = []
        all_artists_albums_list = []

        # all_tracks_set = set()
        # all_artists_set = set()
        # all_albums_set = set()
        # all_albums_tracks_set = set()
        # all_artists_albums_set = set()

        # all_tracks_df = pd.DataFrame(columns = [db.DB.TRACKS.ID, db.DB.TRACKS.HREF,
        #                                         db.DB.TRACKS.URI, db.DB.TRACKS.DISC_NUMBER,
        #                                         db.DB.TRACKS.DURATION_MS, db.DB.TRACKS.EXPLICIT,
        #                                         db.DB.TRACKS.NAME, db.DB.TRACKS.PREVIEW_URL,
        #                                         db.DB.TRACKS.TRACK_NUMBER, db.DB.TRACKS.IS_LOCAL,
        #                                         db.DB.TRACKS.POPULARITY, db.DB.TRACKS.IS_PLAYABLE])
        #
        # all_artists_df = pd.DataFrame(columns = [db.DB.ARTISTS.ID, db.DB.ARTISTS.HREF,
        #                                          db.DB.ARTISTS.URI, db.DB.ARTISTS.NAME,
        #                                          db.DB.ARTISTS.TOTAL_FOLLOWERS, db.DB.ARTISTS.POPULARITY])
        #
        # all_albums_df = pd.DataFrame(columns = [db.DB.ALBUMS.ID,
        #                                         db.DB.ALBUMS.HREF,
        #                                         db.DB.ALBUMS.URI,
        #                                         db.DB.ALBUMS.NAME,
        #                                         db.DB.ALBUMS.ALBUM_TYPE,
        #                                         db.DB.ALBUMS.TOTAL_TRACKS,
        #                                         db.DB.ALBUMS.RELEASE_DATE,
        #                                         db.DB.ALBUMS.RELEASE_DATE_PRECISION])
        #
        # all_albums_tracks_df = pd.DataFrame(columns = [db.DB.ALBUMS_TRACKS.ALBUM_ID, db.DB.ALBUMS_TRACKS.TRACK_ID])
        #
        # all_artists_albums_df = pd.DataFrame(columns = [db.DB.ARTISTS_ALBUMS.ARTIST_ID,
        #                                                 db.DB.ARTISTS_ALBUMS.ALBUM_ID])

        for track in full_tracks:
            # Copying current track to a collection of all Tracks:
            track_dict = {db.DB.TRACKS.ID          : track.id,
                          db.DB.TRACKS.NAME        : track.name,
                          db.DB.TRACKS.DURATION_MS : track.duration_ms,
                          db.DB.TRACKS.DISC_NUMBER : track.disc_number,
                          db.DB.TRACKS.TRACK_NUMBER: track.track_number,
                          db.DB.TRACKS.EXPLICIT    : track.explicit,
                          db.DB.TRACKS.POPULARITY  : track.popularity,
                          db.DB.TRACKS.IS_LOCAL    : track.is_local,
                          db.DB.TRACKS.IS_PLAYABLE : track.is_playable,
                          db.DB.TRACKS.ISRC        : track.external_ids['isrc'] if len(
                              track.external_ids) > 0 else None,
                          db.DB.TRACKS.HREF        : track.href,
                          db.DB.TRACKS.URI         : track.uri,
                          db.DB.TRACKS.PREVIEW_URL : track.preview_url}

            all_tracks_list.append(track_dict)

            # track_ser = pd.Series(track_dict)
            # all_tracks_df = pd.concat([all_tracks_df, track_ser.to_frame().T], ignore_index = True)

            # Copying current track's Album to a collection of all Albums:
            album_dict = {db.DB.ALBUMS.ID                    : track.album.id,
                          db.DB.ALBUMS.NAME                  : track.album.name,
                          db.DB.ALBUMS.TOTAL_TRACKS          : track.album.total_tracks,
                          db.DB.ALBUMS.RELEASE_DATE          : track.album.release_date,
                          db.DB.ALBUMS.RELEASE_DATE_PRECISION: track.album.release_date_precision.value,
                          db.DB.ALBUMS.ALBUM_TYPE            : track.album.album_type.value,
                          db.DB.ALBUMS.HREF                  : track.album.href,
                          db.DB.ALBUMS.URI                   : track.album.uri}

            all_albums_list.append(album_dict)

            # if album_dict not in all_albums_set:
            #     all_albums_set.add(album_dict)

            # album_ser = pd.Series(album_dict)
            # all_albums_df = pd.concat([all_albums_df, album_ser.to_frame().T], ignore_index = True)

            # Copying current track to a collection of all Tracks-of-Albums:
            album_track_dict = {db.DB.ALBUMS_TRACKS.ALBUM_ID: track.album.id,
                                db.DB.ALBUMS_TRACKS.TRACK_ID: track.id}

            all_albums_tracks_list.append(album_track_dict)

            # if album_track_dict not in all_albums_tracks_set:
            #     all_albums_tracks_set.add(album_track_dict)

            # album_track_ser = pd.Series(album_track_dict)
            # all_albums_tracks_df = pd.concat([all_albums_tracks_df, album_track_ser.to_frame().T],
            #                                  ignore_index = True)

            for artist in track.artists:
                # Copying current track's artists to a collection of all Artists:
                artist_dict = {db.DB.ARTISTS.ID             : artist.id,
                               db.DB.ARTISTS.NAME           : artist.name,
                               db.DB.ARTISTS.TOTAL_FOLLOWERS: None,
                               db.DB.ARTISTS.POPULARITY     : None,
                               db.DB.ARTISTS.HREF           : artist.href,
                               db.DB.ARTISTS.URI            : artist.uri}

                all_artists_list.append(artist_dict)

                # if artist_dict not in all_artists_set:
                #     all_artists_set.add(artist_dict)

                # all_artists_df = pd.concat([all_artists_df,
                #                             pd.DataFrame(data = [[artist.id, artist.href, artist.uri,
                #                                                   artist.name, None, None]],
                #                                          columns = all_artists_df.columns.to_list())],
                #                            ignore_index = True)

                # Copying current track's artists to a collection of all Albums-of-Artists.
                # Only Track's Artists that also belong to the Track's Album's Artists
                # (except when related to it with 'Appears On' relationship) are collected.
                if (artist in track.album.artists) & (
                        (track.album.album_group is None) |
                        (track.album.album_group != tk.model.AlbumGroup.appears_on)):
                    artist_album_dict = {db.DB.ARTISTS_ALBUMS.ARTIST_ID: artist.id,
                                         db.DB.ARTISTS_ALBUMS.ALBUM_ID : track.album.id}

                    all_artists_albums_list.append(artist_album_dict)

                    # if artist_album_dict not in all_artists_albums_set:
                    #     all_artists_albums_set.add(artist_album_dict)

                    # artist_album_ser = pd.Series(artist_album_dict)
                    # all_artists_albums_df = pd.concat([all_artists_albums_df, artist_album_ser.to_frame().T],
                    #                                   ignore_index = True)

            # self.my_db.insert_track_full(track)

        # all_artists_df.drop_duplicates(subset = db.DB.ARTISTS.ID, inplace = True)
        # all_albums_df.drop_duplicates(subset = db.DB.ALBUMS.ID, inplace = True)
        # all_albums_tracks_df.drop_duplicates(subset = all_albums_tracks_df.columns.to_list(), inplace = True)
        # all_artists_albums_df.drop_duplicates(subset = all_artists_albums_df.columns.to_list(), inplace = True)

        # region Keeping unique values in the default way:
        all_tracks_list_unq = Logic.get_unique_values(all_tracks_list)
        all_artists_list_unq = Logic.get_unique_values(all_artists_list)
        all_albums_list_unq = Logic.get_unique_values(all_albums_list)
        all_albums_tracks_list_unq = Logic.get_unique_values(all_albums_tracks_list)
        all_artists_albums_list_unq = Logic.get_unique_values(all_artists_albums_list)
        # endregion

        # region Keeping unique values:

        # endregion

        self.my_db.insert_tracks(all_tracks_list_unq)

        self.my_db.insert_listen_history(self.my_spdt.get_tracks_listen_data())

        self.my_db.commit()
