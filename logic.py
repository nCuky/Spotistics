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
        Keep only the unique **dictionaries** in a list of dicts. Each whole dict is taken as a single "value" to
        compare to others.

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
    def clean_json_for_public_repo():
        """
        Small utility meant to be performed only once.

        Removes private data (such as platform, IP address, etc.) from the listen history JSON files,
        so they can be included as sample data in the GitHub repo.

        Returns:
            None
        """
        spdt.SpotifyDataSet.clean_json_for_public_repo()

    # endregion Utility Methods

    def __init__(self, listen_history_from: str = 'db'):
        """
        Initializes an instance of the app's main Logic.

        Parameters:
            listen_history_from: Where to fetch the listen history from.

                Possible values:
                'db' = fetch from an existing DB file.
                'json' = fetch from JSON files downloaded from Spotify.
        """
        self.my_spapi = spapi(token = Logic.get_token())
        self.my_db = db.DB()
        use_db = self.my_db if listen_history_from == 'db' else None
        self.my_spdt = spdt.SpotifyDataSet(db_handler = use_db)

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

    def collect_data_and_save(self, to_csv_also: bool = False):
        """
        Collect all listen history, extract models' (tracks, artists, etc.) information from it
        and save it in the local DB.

        Parameters:
            to_csv_also: Whether to also save the listen history to a CSV file, after saving into the DB.

        Returns:
            None.
        """
        try:
            full_tracks_mdlist = self.my_spapi.get_full_tracks(tracks = self.my_spdt.get_tracks_listen_data()[
                spdtnm.TRACK_ID])

            self.save_full_tracks_to_db(full_tracks_mdlist)

            if to_csv_also:
                # Writing the listen history into a CSV file:
                track_file_name = 'known_tracks_{0}.csv'

                Logic.write_df_to_file(self.my_spdt.get_tracks_listen_data(), track_file_name)

        except tk.ServiceUnavailable as ex:
            return

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

        # linked_from_tracks_ids = []
        all_albums_ids = set()
        all_artists_ids = set()

        for full_track in full_tracks:
            # track_considered_dead = False

            # Building a collection of all Linked Tracks, and a collection of all Tracks-of-Albums:
            if full_track.linked_from is None:
                # For non-linked tracks, "fake-linking" the Track ID to itself, to maintain consistency later:
                linked_track_dict = {spdbnm.TRACKS_LINKED_FROM.FROM_ID    : full_track.id,
                                     spdbnm.TRACKS_LINKED_FROM.RELINKED_ID: full_track.id}

                # For non-linked tracks, linking the Album ID to the Track ID:
                album_track_dict = {spdbnm.ALBUMS_TRACKS.ALBUM_ID: full_track.album.id,
                                    spdbnm.ALBUMS_TRACKS.TRACK_ID: full_track.id}

            else:
                # For linked tracks, linking the Track ID to the Track's Linked From ID:
                linked_track_dict = {spdbnm.TRACKS_LINKED_FROM.FROM_ID    : full_track.linked_from.id,
                                     spdbnm.TRACKS_LINKED_FROM.RELINKED_ID: full_track.id}

                # For linked tracks, linking the Album ID to the Track's Linked From ID:
                album_track_dict = {spdbnm.ALBUMS_TRACKS.ALBUM_ID: full_track.album.id,
                                    spdbnm.ALBUMS_TRACKS.TRACK_ID: full_track.linked_from.id}

                # linked_from_tracks_ids.append(full_track.linked_from.id)

            all_linked_tracks_list.append(linked_track_dict)
            all_albums_tracks_list.append(album_track_dict)

            # Building a collection of all Tracks:
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

            # Building a collection of all Albums:
            album_dict = {spdbnm.ALBUMS.ID                    : full_track.album.id,
                          spdbnm.ALBUMS.NAME                  : full_track.album.name,
                          spdbnm.ALBUMS.TOTAL_TRACKS          : full_track.album.total_tracks,
                          spdbnm.ALBUMS.RELEASE_DATE          : full_track.album.release_date,
                          spdbnm.ALBUMS.RELEASE_DATE_PRECISION: full_track.album.release_date_precision.value,
                          spdbnm.ALBUMS.ALBUM_TYPE            : full_track.album.album_type.value,
                          spdbnm.ALBUMS.IS_AVAILABLE          : None,
                          spdbnm.ALBUMS.HREF                  : full_track.album.href,
                          spdbnm.ALBUMS.URI                   : full_track.album.uri}

            all_albums_list.append(album_dict)
            all_albums_ids.add(full_track.album.id)

            for artist in full_track.artists:
                # Copying current track's artists to a collection of all Artists:
                artist_dict = {spdbnm.ARTISTS.ID             : artist.id,
                               spdbnm.ARTISTS.NAME           : artist.name,
                               spdbnm.ARTISTS.TOTAL_FOLLOWERS: None,
                               spdbnm.ARTISTS.POPULARITY     : None,
                               spdbnm.ARTISTS.HREF           : artist.href,
                               spdbnm.ARTISTS.URI            : artist.uri}

                all_artists_list.append(artist_dict)
                all_artists_ids.add(artist.id)

                # Building a collection of all Albums-of-Artists.
                # Only the track's Artists that also belong to the Track's Album's Artists (except when related to it
                # with 'Appears On' relationship) are collected.
                if artist in full_track.album.artists:
                    # and ((full_track.album.album_group is None) or
                    #      (full_track.album.album_group != tk.model.AlbumGroup.appears_on)):
                    artist_album_dict = {spdbnm.ARTISTS_ALBUMS.ARTIST_ID  : artist.id,
                                         spdbnm.ARTISTS_ALBUMS.ALBUM_ID   : full_track.album.id,
                                         spdbnm.ARTISTS_ALBUMS.ALBUM_GROUP: full_track.album.album_group.value if
                                         full_track.album.album_group is not None else None}

                    all_artists_albums_list.append(artist_album_dict)

        # Keeping only the unique values in each list:
        all_tracks_list_unq = Logic.get_unique_values(all_tracks_list)
        all_linked_tracks_list_unq = Logic.get_unique_values(all_linked_tracks_list)
        all_artists_list_unq = Logic.get_unique_values(all_artists_list)
        all_albums_list_unq = Logic.get_unique_values(all_albums_list)
        all_albums_tracks_list_unq = Logic.get_unique_values(all_albums_tracks_list)
        all_artists_albums_list_unq = Logic.get_unique_values(all_artists_albums_list)

        # Filling attribute `is_available` for each album:
        full_albums = self.my_spapi.get_full_albums(all_albums_ids)

        albums_availability = {_full_album.id: len(_full_album.available_markets) > 0 for _full_album in full_albums}

        for i, full_album in enumerate(all_albums_list_unq):
            all_albums_list_unq[i][spdbnm.ALBUMS.IS_AVAILABLE] = albums_availability[full_album[spdbnm.ALBUMS.ID]]

        # Filling attribute `album_group` for each album-artist:
        full_artists_albums = self.my_spapi.get_artists_albums(all_artists_ids)

        # Inserting all values to the corresponding DB-tables:
        self.my_db.insert_listen_history(self.my_spdt.get_tracks_listen_data())
        self.my_db.insert_tracks(all_tracks_list_unq)
        self.my_db.insert_linked_tracks(all_linked_tracks_list_unq)
        self.my_db.insert_artists(all_artists_list_unq)
        self.my_db.insert_albums(all_albums_list_unq)
        self.my_db.insert_albums_tracks(all_albums_tracks_list_unq)
        self.my_db.insert_artists_albums(all_artists_albums_list_unq)

        self.my_db.commit()
