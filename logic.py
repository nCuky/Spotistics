import names
from spotify_api_client import SpotifyAPIClient as spapi
import spotify_data_set as spdt
from datetime import datetime as dt
import pandas as pd
import numpy as np
import tekore as tk
import log
# import pyspark as sk
import db
from names import Spdb as spdbnm
from names import Spdt as spdtnm
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
    def write_df_to_file(df: pd.DataFrame, file_name: str) -> None:
        """
        Writes a given DataFrame to a file.

        Parameters:
            df: DataFrame to write to a file.

            file_name: Name of the desired file to write (without preceding path).

        Returns:
            None.
        """
        file_path = 'data/personal_data/prepared/' + file_name.format(dt.now().strftime("%Y-%m-%d_%H-%M-%S"))
        log.write(log.WRITING_FILE.format(file_path))
        # Writing an Excel Spreadsheet doesn't work yet.
        df.to_csv(path_or_buf = file_path,
                  encoding = 'utf-8-sig',  # UTF-8, explicitly signed with a BOM at the start of the file
                  index = False)
        log.write(log.FILE_WRITTEN.format(file_path))

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
        # Taken from https://stackoverflow.com/a/19804098/6202667
        unique_dict_list = list(map(dict, set(tuple(d.items()) for d in dicts)))

        return unique_dict_list

    @staticmethod
    def full_track_to_dict():
        return None

    # endregion Utility Methods

    def __init__(self):
        """
        Initializes an instance of the app's main Logic.
        """
        self.my_spapi = spapi(token = Logic.get_token())
        self.my_db = db.DB()
        self.my_spdt = spdt.SpotifyDataSet()

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
        only_duration = self.my_spdt.get_tracks_listen_data().groupby(spdtnm.SONG_KEY)[
            spdtnm.MS_PLAYED].sum()

        my_results = self.my_spdt.get_tracks_listen_data().drop(columns = spdtnm.MS_PLAYED).groupby(
            spdtnm.SONG_KEY).mean().assign(
            duration_ms = only_duration)

        Logic.write_df_to_file(my_results, "listen_data_by_key.csv")

    def calc_listen_data_mean_key(self) -> None:
        """
        Aggregates all listened tracks by mean key, and saves it into a CSV file.

        Returns:
            None.
        """
        self.my_spdt.get_tracks_listen_data().groupby(spdtnm.SONG_KEY).mean().to_csv("mean_by_key.csv")

    def collect_listen_history_to_file(self) -> None:
        """
        Saves the listen history DataFrame to a CSV file.

        Returns:
            None
        """
        track_data = self.my_spdt.get_tracks_listen_data()

        # Writing to CSV file:
        track_file_name = 'all_tracks_raw_{0}.csv'

        Logic.write_df_to_file(track_data, track_file_name)

    def collect_data_and_save(self):
        """
        Collect all listen history, extract models' (tracks, artists, etc.) information from it
        and save it in the local DB.

        Returns:
            None.
        """
        try:
            full_tracks_mdlist = self.my_spapi.get_full_tracks(tracks = self.my_spdt.get_tracks_listen_data()[
                spdtnm.TRACK_ID])

            self.save_full_tracks_to_db(full_tracks_mdlist)

            # Writing to CSV file:
            track_file_name = 'known_tracks_{0}.csv'

            Logic.write_df_to_file(self.my_spdt.get_tracks_listen_data(), track_file_name)

        except tk.ServiceUnavailable as ex:
            return

    def count_unique_tracks(self) -> pd.DataFrame:
        tracks_df = self.my_spdt.get_tracks_listen_data()

        # Removing all records of tracks that were played exactly 0 milliseconds:
        # tracks_df = tracks_df.drop(index = tracks_df.index[tracks_df[spdtnm.MS_PLAYED].eq(0)], inplace = False)

        tracks_count = tracks_df.groupby(spdtnm.TRACK_KNOWN_ID,
                                         as_index = False).agg(
            times_listened = (spdtnm.TRACK_KNOWN_ID, 'count'),
            total_time_listened = (spdtnm.MS_PLAYED, 'sum'),
            album_artist_name = (spdtnm.ALBUM_ARTIST_NAME, 'first'),
            album_name = (spdtnm.ALBUM_NAME, 'first'),
            track_name = (spdtnm.TRACK_NAME, 'first'))

        return tracks_count

    def save_full_tracks_to_db(self, full_tracks: tk.model.ModelList[tk.model.FullTrack]) -> None:
        """
        From a given :class:`tk.model.ModelList` of :class:`tk.model.FullTrack` objects,
        extract the models' data and save it into the DB.

        Parameters:
            full_tracks: A ModelList of FullTrack objects.

        Returns:
            None.
        """
        track_known_id_map = self.my_spapi.get_track_known_id_map(full_tracks = full_tracks)
        self.my_spdt.add_track_known_id(track_known_id_map)

        all_tracks_list = []
        all_linked_tracks_list = []
        all_artists_list = []
        all_albums_list = []
        all_albums_tracks_list = []
        all_artists_albums_list = []

        for full_track in full_tracks:
            # Copying current track to a collection of all Tracks:
            track_dict = {spdbnm.TRACKS.ID          : full_track.id,
                          spdbnm.TRACKS.NAME        : full_track.name,
                          spdbnm.TRACKS.DURATION_MS : full_track.duration_ms,
                          spdbnm.TRACKS.DISC_NUMBER : full_track.disc_number,
                          spdbnm.TRACKS.TRACK_NUMBER: full_track.track_number,
                          spdbnm.TRACKS.EXPLICIT    : full_track.explicit,
                          spdbnm.TRACKS.POPULARITY  : full_track.popularity,
                          spdbnm.TRACKS.IS_LOCAL    : full_track.is_local,
                          spdbnm.TRACKS.IS_PLAYABLE : full_track.is_playable,
                          spdbnm.TRACKS.ISRC        : full_track.external_ids['isrc'] if len(
                              full_track.external_ids) > 0 else None,
                          spdbnm.TRACKS.HREF        : full_track.href,
                          spdbnm.TRACKS.URI         : full_track.uri,
                          spdbnm.TRACKS.PREVIEW_URL : full_track.preview_url}

            all_tracks_list.append(track_dict)

            # Copying current track's Album to a collection of all Albums:
            album_dict = {spdbnm.ALBUMS.ID                    : full_track.album.id,
                          spdbnm.ALBUMS.NAME                  : full_track.album.name,
                          spdbnm.ALBUMS.TOTAL_TRACKS          : full_track.album.total_tracks,
                          spdbnm.ALBUMS.RELEASE_DATE          : full_track.album.release_date,
                          spdbnm.ALBUMS.RELEASE_DATE_PRECISION: full_track.album.release_date_precision.value,
                          spdbnm.ALBUMS.ALBUM_TYPE            : full_track.album.album_type.value,
                          spdbnm.ALBUMS.HREF                  : full_track.album.href,
                          spdbnm.ALBUMS.URI                   : full_track.album.uri}

            all_albums_list.append(album_dict)

            linked_track_dict = db.DB.get_linked_from_for_insert(full_track)
            all_linked_tracks_list.append(linked_track_dict)

            # Copying current track to a collection of all Tracks-of-Albums:
            album_track_dict = {spdbnm.ALBUMS_TRACKS.ALBUM_ID: full_track.album.id,
                                spdbnm.ALBUMS_TRACKS.TRACK_ID: full_track.id}

            all_albums_tracks_list.append(album_track_dict)

            for artist in full_track.artists:
                # Copying current track's artists to a collection of all Artists:
                artist_dict = {spdbnm.ARTISTS.ID             : artist.id,
                               spdbnm.ARTISTS.NAME           : artist.name,
                               spdbnm.ARTISTS.TOTAL_FOLLOWERS: None,
                               spdbnm.ARTISTS.POPULARITY     : None,
                               spdbnm.ARTISTS.HREF           : artist.href,
                               spdbnm.ARTISTS.URI            : artist.uri}

                all_artists_list.append(artist_dict)

                # Copying current track's artists to a collection of all Albums-of-Artists.
                # Only Track's Artists that also belong to the Track's Album's Artists
                # (except when related to it with 'Appears On' relationship) are collected.
                if (artist in full_track.album.artists) & (
                        (full_track.album.album_group is None) |
                        (full_track.album.album_group != tk.model.AlbumGroup.appears_on)):
                    artist_album_dict = {spdbnm.ARTISTS_ALBUMS.ARTIST_ID: artist.id,
                                         spdbnm.ARTISTS_ALBUMS.ALBUM_ID : full_track.album.id}

                    all_artists_albums_list.append(artist_album_dict)

        # region Keeping unique values in the default way:
        all_tracks_list_unq = Logic.get_unique_values(all_tracks_list)
        all_linked_tracks_list_unq = Logic.get_unique_values(all_linked_tracks_list)
        all_artists_list_unq = Logic.get_unique_values(all_artists_list)
        all_albums_list_unq = Logic.get_unique_values(all_albums_list)
        all_albums_tracks_list_unq = Logic.get_unique_values(all_albums_tracks_list)
        all_artists_albums_list_unq = Logic.get_unique_values(all_artists_albums_list)
        # endregion

        # region Keeping unique values:

        # endregion

        self.my_db.insert_tracks(all_tracks_list_unq)
        self.my_db.insert_linked_tracks(all_linked_tracks_list_unq)
        self.my_db.insert_artists(all_artists_list_unq)
        self.my_db.insert_albums(all_albums_list_unq)
        self.my_db.insert_albums_tracks(all_albums_tracks_list_unq)
        self.my_db.insert_artists_albums(all_artists_albums_list_unq)

        self.my_db.insert_listen_history(self.my_spdt.get_tracks_listen_data())

        self.my_db.commit()
