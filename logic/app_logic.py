from logic.model.spotify_api_client import SpotifyAPIClient as spapi
from logic import general_utils as utl
from logic.db import db_names as SPDBNM, db
from logic.model import sp_data_set as spdt
from logic.model.sp_data_set_names import SPDT as SPDTNM
from logic.frontend import plotting_names as PLTNM, log
import numpy as np
import pandas as pd
import tekore as tk


class Logic:
    """
    The app's main Logic. Can be used for initializing the dataset and DB, fetching data, or for performing calculations
    on it.
    """

    # region Utility Methods

    @staticmethod
    def get_token(token_path = None):
        if token_path is None:
            token_path = "./token"
        token_file = open(token_path, "rt")
        token_file_text = token_file.readlines()
        token_file.close()

        return token_file_text

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

    # region Saving data

    @staticmethod
    def _add_track_to_list_to_save(full_track: tk.model.FullTrack,
                                   all_tracks_list: list[dict]) -> None:
        """
        From a given :class:`tk.model.FullTrack` object, takes the **Track** attributes and adds them to a given
        list of named dictionaries.

        **This changes the given collection (inplace = True)**.

        Parameters:
            full_track: FullTrack object, from which to take the relevant attributes.

            all_tracks_list: List of all the tracks that are supposed to later be saved into the DB.

        Returns:
            None - This method changed the given collection.
        """
        # Building a named dictionary from the given FullTrack:
        track_dict_to_insert = {SPDBNM.TRACKS.ID          : full_track.id,
                                SPDBNM.TRACKS.NAME        : full_track.name,
                                SPDBNM.TRACKS.DURATION_MS : full_track.duration_ms,
                                SPDBNM.TRACKS.DISC_NUMBER : full_track.disc_number,
                                SPDBNM.TRACKS.TRACK_NUMBER: full_track.track_number,
                                SPDBNM.TRACKS.EXPLICIT    : full_track.explicit,
                                SPDBNM.TRACKS.POPULARITY  : full_track.popularity,
                                SPDBNM.TRACKS.IS_LOCAL    : full_track.is_local,
                                SPDBNM.TRACKS.IS_PLAYABLE : full_track.is_playable,
                                SPDBNM.TRACKS.ISRC        : full_track.external_ids['isrc'] if len(
                                    full_track.external_ids) > 0 else None,
                                SPDBNM.TRACKS.HREF        : full_track.href,
                                SPDBNM.TRACKS.URI         : full_track.uri,
                                SPDBNM.TRACKS.PREVIEW_URL : full_track.preview_url}

        all_tracks_list.append(track_dict_to_insert)

    @staticmethod
    def _add_album_to_list_to_save(full_track: tk.model.FullTrack,
                                   all_albums_list: list[dict],
                                   all_albums_ids: set) -> None:
        """
        From a given :class:`tk.model.FullTrack` object, takes the **Album** attributes and adds them to a given
        list of named dictionaries. Also adds the AlbumID to another given set.

        **This changes the given collections (inplace = True)**.

        Parameters:
            full_track: FullTrack object, from which to take the relevant attributes.

            all_albums_list: List of named dicts, each dict contains an album that is supposed to be saved into the
                DB later.

            all_albums_ids: Set of all Album IDs, later used for fetching missing data.

        Returns:
            None - This method changed the given collections.
        """
        # Regardless to whether the track or album are linked or not, building a collection of all Albums:
        album_dict_to_insert = {SPDBNM.ALBUMS.ID                    : full_track.album.id,
                                SPDBNM.ALBUMS.NAME                  : full_track.album.name,
                                SPDBNM.ALBUMS.TOTAL_TRACKS          : full_track.album.total_tracks,
                                SPDBNM.ALBUMS.RELEASE_DATE          : full_track.album.release_date,
                                SPDBNM.ALBUMS.RELEASE_DATE_PRECISION: full_track.album.release_date_precision.value,
                                SPDBNM.ALBUMS.ALBUM_TYPE            : full_track.album.album_type.value,
                                SPDBNM.ALBUMS.IS_AVAILABLE          : None,
                                SPDBNM.ALBUMS.HREF                  : full_track.album.href,
                                SPDBNM.ALBUMS.URI                   : full_track.album.uri}

        all_albums_list.append(album_dict_to_insert)
        all_albums_ids.add(full_track.album.id)

    @staticmethod
    def _add_artists_to_list_to_save(full_track: tk.model.FullTrack,
                                     all_artists_list: list[dict] = None,
                                     all_artists_ids: set = None,
                                     all_artists_albums_list: list[dict] = None) -> None:
        """
        From a given :class:`tk.model.FullTrack` object, takes the **Artists** attributes
        (:class:`tk.model.SimpleArtist`) and adds them to a given list of named dictionaries.
        Also, adds the Artist's Album to another list of named dicts.

        **This changes the given collections (inplace = True)**.

        Parameters:
            full_track: FullTrack object, from which to take the relevant attributes.

            all_artists_list: List of dicts, each dict contains an Artist that is supposed to be saved into the DB
                later.

            all_artists_albums_list: List of dicts, each dict contains a link between the Artist and their Album.

            all_artists_ids: Set of all Artist IDs, later used for fetching missing data.

        Returns:
            None - This method changed the given collections.
        """
        # Building a collection of all Artists:
        for simple_artist in full_track.artists:
            if all_artists_list is not None:
                Logic._add_artist_dict_to_list(artist = simple_artist,
                                               all_artists_list = all_artists_list)

            if all_artists_ids is not None:
                all_artists_ids.add(simple_artist.id)

            if all_artists_albums_list is not None:
                # Building a collection of all Albums-of-Artists.
                # Only the track's Artists that also belong to the Track's Album's Artists (except when related to it
                # with 'Appears On' relationship) are collected.
                if simple_artist in full_track.album.artists:
                    # and ((full_track.album.album_group is None) or
                    #      (full_track.album.album_group != tk.model.AlbumGroup.appears_on)):
                    artist_album_dict_to_insert = {SPDBNM.ARTISTS_ALBUMS.ARTIST_ID  : simple_artist.id,
                                                   SPDBNM.ARTISTS_ALBUMS.ALBUM_ID   : full_track.album.id,
                                                   SPDBNM.ARTISTS_ALBUMS.ALBUM_GROUP: None}

                    all_artists_albums_list.append(artist_album_dict_to_insert)

    @staticmethod
    def _add_artist_dict_to_list(artist: tk.model.Artist,
                                 all_artists_list: list[dict],
                                 all_genres_set: set = None,
                                 all_artists_genres: list[dict] = None) -> None:
        """
        From a given :class:`tk.model.Artist` object, takes its attributes and adds them to the given
        list of dictionaries ``all_artists_list``.

        If ``artist`` is a :class:`tk.model.FullArtist`, this also builds a set of Genres' names (independent),
        and builds another list linking the artist to its genres.

        **This changes the given collections (inplace = True)**.

        Parameters:
            artist: SimpleArtist or FullArtist object, from which to take the relevant attributes.

            all_artists_list: List of dicts, each dict contains an Artist that is supposed to be saved into the DB
                later.

            all_genres_set: Set of strings, containing the genre names that are in the artist's attributes.

            all_artists_genres: List of dicts, each dict linking the artist to one of its genres.

        Returns:
            None - This method changed the given collections.
        """
        match artist:
            case tk.model.SimpleArtist() as artist:
                artist_dict_to_insert = {SPDBNM.ARTISTS.ID             : artist.id,
                                         SPDBNM.ARTISTS.NAME           : artist.name,
                                         SPDBNM.ARTISTS.TOTAL_FOLLOWERS: None,
                                         SPDBNM.ARTISTS.POPULARITY     : None,
                                         SPDBNM.ARTISTS.HREF           : artist.href,
                                         SPDBNM.ARTISTS.URI            : artist.uri}

            case tk.model.FullArtist() as artist:
                artist_dict_to_insert = {SPDBNM.ARTISTS.ID             : artist.id,
                                         SPDBNM.ARTISTS.NAME           : artist.name,
                                         SPDBNM.ARTISTS.TOTAL_FOLLOWERS: artist.followers.total,
                                         SPDBNM.ARTISTS.POPULARITY     : artist.popularity,
                                         SPDBNM.ARTISTS.HREF           : artist.href,
                                         SPDBNM.ARTISTS.URI            : artist.uri}

                if all_genres_set is not None:
                    for genre in artist.genres:
                        all_genres_set.add(genre)
                        all_artists_genres.append({SPDBNM.ARTISTS_GENRES.ARTIST_ID : artist.id,
                                                   SPDBNM.ARTISTS_GENRES.GENRE_NAME: genre})

            case tk.model.LocalArtist() as artist:
                # Not supported at the moment
                artist_dict_to_insert = None

            case other:
                artist_dict_to_insert = None

        if artist_dict_to_insert is not None:
            all_artists_list.append(artist_dict_to_insert)

    def save_listen_history_to_csv(self, filename: str = 'all_tracks_raw_{0}.csv') -> None:
        """
        Saves the listen history DataFrame to a CSV file.

        Parameters:
            filename: Desired filename to save the csv file as.

        Returns:
            None
        """
        # Writing to CSV file:
        track_file_name = filename if filename is not None and filename != '' else 'all_tracks_raw_{0}.csv'

        utl.write_df_to_file(self.spdt.listen_history_df, track_file_name)

    def collect_data_and_save(self, to_csv_also: bool = False):
        """
        Collects all listen history, extracts models' (tracks, artists, etc.) information from it
        and saves it in the local DB.
        Then, replaces the inner :class:`SPDT.SpotifyDataSet` dataset manager with a new manager
        containing all the updated data.

        Parameters:
            to_csv_also: Whether to also save the listen history to a CSV file, after saving into the DB.

        Returns:
            None.
        """
        try:
            log.write(log.GETTING_ORIGINAL_TRACKS)

            full_tracks_mdlist = self.spapi.get_full_tracks(self.spdt.listen_history_df[SPDTNM.TRACK_ID])

            self.save_full_tracks_to_db(full_tracks_mdlist)

        except tk.ServiceUnavailable as ex:
            log.write(log.API_SERVICE_UNAVAILABLE.format(ex))

            return

        if to_csv_also:
            self.save_listen_history_to_csv('known_listen_history_{0}.csv')

        self._spdt = spdt.SpotifyDataSet(db_handler = self.db)

    def save_full_tracks_to_db(self, full_tracks: tk.model.ModelList[tk.model.FullTrack]) -> None:
        """
        From a given :class:`tk.model.ModelList` of :class:`tk.model.FullTrack` objects,
        extracts the models' data, fetches additional data from the API, and saves it into the DB.

        Parameters:
            full_tracks: A ModelList of FullTrack objects.

        Returns:
            None.
        """
        all_tracks_list_to_insert = []
        all_linked_tracks_list_to_insert = []
        all_linked_albums_list_to_insert = []
        all_artists_list_to_insert = []
        all_genres_set_to_insert = set()
        all_artists_genres_list_to_insert = []
        all_albums_list_to_insert = []
        all_albums_tracks_list_to_insert = []
        all_artists_albums_list_to_insert = []

        map_known_tracks_to_albums = {}
        map_albums_to_tracks = {}
        known_ids_of_linked_tracks = set()
        map_linked_to_known_tracks = {}
        map_linked_to_known_albums = {}
        all_suspected_album_ids = set()
        all_albums_ids_set = set()
        all_artists_ids_set = set()

        for full_track in full_tracks:
            Logic._add_track_to_list_to_save(full_track, all_tracks_list_to_insert)

            # A Track sometimes change its ID, because of many reasons. The old TrackID is then regarded as
            # a 'LinkedFrom' Track, which is Relinked to its current, up-to-date, 'Known' Track, this linkage can be
            # fetched from the API. The problem is, *Albums* too can be Relinked, but the API does not supply this
            # information in a direct or confident way.
            # I need this linkage, OldAlbumID <-> KnownAlbumID, to correctly count the unique albums
            # in the listen history.
            #
            # The API *does* give me the following information (in the ``full_tracks`` object):
            # 1. Linkage between an obsolete LinkedFrom TrackID and its 'uncertain but suspected as obsolete' AlbumID;
            # 2. Linkage between an obsolete LinkedFrom TrackID and its up-to-date 'Known' TrackID;
            # 3. Linkage between a Known TrackID and its up-to-date AlbumID.
            #
            # Therefore, my plan is to go along the link chain as follows:
            # 1. Collect all those links: LinkedFromTrackIDs <-> OldAlbumIDs,
            #                             LinkedFromTrackIDs <-> KnownTrackIDs,
            #                             KnownTrackIDs <-> KnownAlbumIDs;
            # 2. For each OldAlbumID:
            # 2.a. Find its tracks in the list of LinkedFromTrackIDs;
            # 2.b. For the found LinkedFromTrackID, get the KnownTrackID;
            # 2.c. For the found KnownTrackID, get the KnownAlbumID and write it for the current OldAlbumID.
            # 3. End up with a mapping of each OldAlbumID to its KnownAlbumID, and
            #    insert it to the DB table `linked_albums`.
            if full_track.linked_from is None:
                # --Non-Linked track, which means its Album is also not Linked--
                # "Fake-linking" the Track ID to itself, to maintain consistency later in the DB:
                linked_track_dict_to_insert = {SPDBNM.LINKED_TRACKS.FROM_ID    : full_track.id,
                                               SPDBNM.LINKED_TRACKS.RELINKED_ID: full_track.id}

                # "Fake-linking" the Album ID to itself, to maintain consistency later in the DB:
                linked_album_dict_to_insert = {SPDBNM.LINKED_ALBUMS.FROM_ID    : full_track.album.id,
                                               SPDBNM.LINKED_ALBUMS.RELINKED_ID: full_track.album.id}

                all_linked_albums_list_to_insert.append(linked_album_dict_to_insert)

                # Assigning the Track ID to the Album ID:
                album_track_dict_to_insert = {SPDBNM.ALBUMS_TRACKS.ALBUM_ID: full_track.album.id,
                                              SPDBNM.ALBUMS_TRACKS.TRACK_ID: full_track.id}

                # A track *is not supposed* to belong to more than one album. I hope I don't override
                # an existing TrackID Key with a different AlbumID Value:
                map_known_tracks_to_albums[full_track.id] = full_track.album.id

                utl.add_to_mapping_of_sets(mapping = map_albums_to_tracks,
                                           key = full_track.album.id,
                                           value = full_track.id)

                map_linked_to_known_tracks[full_track.id] = full_track.id

                # If the current FullTrack object is for the KnownTrackID of a track that was previously determined as
                # relinked to another LinkedFromTrack, it means that this FullTrack object contains the up-to-date
                # KnownAlbumID I need. Thus, I don't need to fetch it later, so I remove it:
                if known_ids_of_linked_tracks is not None and full_track.id in known_ids_of_linked_tracks:
                    known_ids_of_linked_tracks.discard(full_track.id)

            else:
                # --Linked Track, which means its Album is also suspected as Linked--
                # Linking the Track's LinkedFrom ID to the Track ID:
                linked_track_dict_to_insert = {SPDBNM.LINKED_TRACKS.FROM_ID    : full_track.linked_from.id,
                                               SPDBNM.LINKED_TRACKS.RELINKED_ID: full_track.id}

                # Linking the (suspected as linked) Album ID to the Track's LinkedFrom ID:
                album_track_dict_to_insert = {SPDBNM.ALBUMS_TRACKS.ALBUM_ID: full_track.album.id,
                                              SPDBNM.ALBUMS_TRACKS.TRACK_ID: full_track.linked_from.id}

                utl.add_to_mapping_of_sets(mapping = map_albums_to_tracks,
                                           key = full_track.album.id,
                                           value = full_track.linked_from.id)

                map_linked_to_known_tracks[full_track.linked_from.id] = full_track.id

                known_ids_of_linked_tracks.add(full_track.id)

                # Trying to use a previously-saved link, to build the mapping during the loop:
                known_album_id = map_linked_to_known_albums.get(full_track.album.id)

                if known_album_id is None:
                    # Trying to go along the link chain:
                    track_known_id = map_linked_to_known_tracks[full_track.linked_from.id]
                    known_album_id = map_known_tracks_to_albums.get(track_known_id)

                if known_album_id is None:
                    all_suspected_album_ids.add(full_track.album.id)

                else:
                    map_linked_to_known_albums[full_track.album.id] = known_album_id

                    linked_album_dict_to_insert = {SPDBNM.LINKED_ALBUMS.FROM_ID    : full_track.album.id,
                                                   SPDBNM.LINKED_ALBUMS.RELINKED_ID: known_album_id}

                    all_linked_albums_list_to_insert.append(linked_album_dict_to_insert)

            all_linked_tracks_list_to_insert.append(linked_track_dict_to_insert)
            all_albums_tracks_list_to_insert.append(album_track_dict_to_insert)

            Logic._add_album_to_list_to_save(full_track, all_albums_list_to_insert, all_albums_ids_set)

            Logic._add_artists_to_list_to_save(full_track = full_track,
                                               # all_artists_list = all_artists_list_to_insert,
                                               all_artists_ids = all_artists_ids_set,
                                               all_artists_albums_list = all_artists_albums_list_to_insert)

        # I've collected the ID's of only the tracks that are Relinked from other tracks.
        # Here, fetching FullTrack attributes only for those relinked tracks (their IDs were only discovered earlier
        # in the iterated FullTracks list, and now I need the rest of their attributes):
        log.write(log.GETTING_RELINKED_TRACKS)
        known_full_tracks = self.spapi.get_full_tracks(tracks_ids = known_ids_of_linked_tracks)

        for known_full_track in known_full_tracks:
            map_known_tracks_to_albums[known_full_track.id] = known_full_track.album.id

            utl.add_to_mapping_of_sets(mapping = map_albums_to_tracks,
                                       key = known_full_track.album.id,
                                       value = known_full_track.id)

            Logic._add_track_to_list_to_save(known_full_track, all_tracks_list_to_insert)

            Logic._add_album_to_list_to_save(known_full_track, all_albums_list_to_insert, all_albums_ids_set)

            Logic._add_artists_to_list_to_save(known_full_track,
                                               # all_artists_list = all_artists_list_to_insert,
                                               all_artists_ids = all_artists_ids_set,
                                               all_artists_albums_list = all_artists_albums_list_to_insert)

        # Mapping the rest of the unmapped OldAlbumIDs to their KnownAlbumIDs:
        for suspected_album_id in all_suspected_album_ids:
            known_album_id = map_linked_to_known_albums.get(suspected_album_id)

            if known_album_id is None:
                # Trying to go along the link chain, this time using the tracks that belong to the album:
                for album_track in map_albums_to_tracks.get(suspected_album_id):
                    track_known_id = map_linked_to_known_tracks[album_track]
                    known_album_id = map_known_tracks_to_albums.get(track_known_id)

                    if known_album_id is not None:
                        # By now, I don't really need to map the found KnownAlbumID, as it will not be searched anymore:
                        map_linked_to_known_albums[suspected_album_id] = known_album_id

                        linked_album_dict_to_insert = {SPDBNM.LINKED_ALBUMS.FROM_ID    : suspected_album_id,
                                                       SPDBNM.LINKED_ALBUMS.RELINKED_ID: known_album_id}

                        all_linked_albums_list_to_insert.append(linked_album_dict_to_insert)

                    break

            else:
                # By now, I don't really need to map the found KnownAlbumID, as it will not be searched anymore:
                map_linked_to_known_albums[suspected_album_id] = known_album_id

                linked_album_dict_to_insert = {SPDBNM.LINKED_ALBUMS.FROM_ID    : suspected_album_id,
                                               SPDBNM.LINKED_ALBUMS.RELINKED_ID: known_album_id}

                all_linked_albums_list_to_insert.append(linked_album_dict_to_insert)

        # Keeping only the unique values in each list:
        all_tracks_list_unq = utl.get_unique_dicts(all_tracks_list_to_insert)
        all_linked_tracks_list_unq = utl.get_unique_dicts(all_linked_tracks_list_to_insert)
        all_linked_albums_list_unq = utl.get_unique_dicts(all_linked_albums_list_to_insert)
        # all_artists_list_unq = utl.get_unique_dicts(all_artists_list_to_insert)
        all_albums_list_unq = utl.get_unique_dicts(all_albums_list_to_insert)
        all_albums_tracks_list_unq = utl.get_unique_dicts(all_albums_tracks_list_to_insert)
        all_artists_albums_list_unq = utl.get_unique_dicts(all_artists_albums_list_to_insert)

        # Filling attribute `is_available` for each album:
        full_albums = self.spapi.get_full_albums(all_albums_ids_set)
        albums_availability = {_full_album.id: len(_full_album.available_markets) > 0 for _full_album in full_albums}

        for i, full_album in enumerate(all_albums_list_unq):
            all_albums_list_unq[i][SPDBNM.ALBUMS.IS_AVAILABLE] = albums_availability[full_album[SPDBNM.ALBUMS.ID]]

        # Fetching Artists' attributes, including their Genres:
        full_artists = self.spapi.get_full_artists(all_artists_ids_set)

        for full_artist in full_artists:
            Logic._add_artist_dict_to_list(artist = full_artist,
                                           all_artists_list = all_artists_list_to_insert,
                                           all_genres_set = all_genres_set_to_insert,
                                           all_artists_genres = all_artists_genres_list_to_insert)

        all_artists_list_unq = utl.get_unique_dicts(all_artists_list_to_insert)
        all_artists_genres_list_unq = utl.get_unique_dicts(all_artists_genres_list_to_insert)

        # Inserting all values to the corresponding DB-tables:
        self.db.insert_listen_history(self.spdt.listen_history_df)
        self.db.insert_tracks(all_tracks_list_unq)
        self.db.insert_linked_tracks(all_linked_tracks_list_unq)
        self.db.insert_linked_albums(all_linked_albums_list_unq)
        self.db.insert_artists(all_artists_list_unq)
        self.db.insert_genres([{SPDBNM.GENRES.GENRE_NAME: genre_name} for genre_name in all_genres_set_to_insert])
        self.db.insert_artists_genres(all_artists_genres_list_unq)
        self.db.insert_albums(all_albums_list_unq)
        self.db.insert_albums_tracks(all_albums_tracks_list_unq)
        self.db.insert_artists_albums(all_artists_albums_list_unq)

        self.db.commit()

    # endregion Saving data

    # endregion Utility Methods

    # region Initialization

    def __init__(self, listen_history_from: str = 'db'):
        """
        Initializes an instance of the app's main Logic.

        Parameters:
            listen_history_from: Where to fetch the listen history from.

                Possible values:
                'db' = fetch from an existing DB file.
                'json' = fetch from JSON files downloaded from Spotify.
        """
        self._spapi = spapi(token_keys = Logic.get_token())
        self._db = db.DB()

        if listen_history_from == 'json':
            self._spdt = spdt.SpotifyDataSet(db_handler = None)
            self.collect_data_and_save(to_csv_also = False)

        else:
            self._spdt = spdt.SpotifyDataSet(db_handler = self.db)

    @property
    def spapi(self) -> spapi:
        return self._spapi

    @property
    def db(self) -> db.DB:
        return self._db

    @property
    def spdt(self) -> spdt.SpotifyDataSet:
        return self._spdt

    def get_listen_history_df(self) -> pd.DataFrame:
        return self.spdt.listen_history_df.copy()

    # endregion Initialization

    def get_artist_audio_features_data(self, name: str):
        artist_id = self.spapi.find_artist(name).id
        artist_tracks = self.spapi.artists_get_all_tracks(artist_id)
        tracks_ids = [track.id for album in artist_tracks[artist_id] for track in album[1]]
        tracks_features = self.spapi.get_tracks_audio_features(tracks_ids)

        # pd.read_json()

        return tracks_features

    # region Calculations for plotting

    def calc_listen_data_by_key(self) -> None:
        """
        Aggregates all listened tracks by key, and writes it as a csv file
        :return:
        """
        only_duration = self.spdt.listen_history_df.groupby(SPDTNM.SONG_KEY)[
            SPDTNM.MS_PLAYED].sum()

        my_results = self.spdt.listen_history_df.drop(columns = SPDTNM.MS_PLAYED).groupby(
            SPDTNM.SONG_KEY).mean().assign(
            duration_ms = only_duration)

        utl.write_df_to_file(my_results, "listen_data_by_key.csv")

    def calc_listen_data_mean_key(self) -> None:
        """
        Aggregates all listened tracks by mean key, and saves it into a CSV file.

        Returns:
            None.
        """
        self.spdt.listen_history_df.groupby(SPDTNM.SONG_KEY).mean().to_csv("mean_by_key.csv")

    def calc_top_artists_by_listen_count(self, top_artists_amount = 50) -> pd.DataFrame:
        tracks_count = self.agg_unique_tracks_by_listens().sort_values(by = SPDTNM.TOTAL_LISTEN_TIME, ascending = False)

        times_listened_by_artist = tracks_count.groupby(by = SPDTNM.ALBUM_ARTIST_NAME, as_index = True).agg(
            times_listened = (SPDTNM.TIMES_LISTENED, 'sum')).sort_values(SPDTNM.TIMES_LISTENED,
                                                                         ascending = False).head(top_artists_amount)

        return times_listened_by_artist

    def calc_top_artists_by_total_listen_time(self, top_artists_amount = 30) -> pd.DataFrame:
        tracks_count = self.agg_unique_tracks_by_listens().sort_values(by = SPDTNM.TOTAL_LISTEN_TIME, ascending = False)

        total_listen_time_by_artist = tracks_count.groupby(by = SPDTNM.ALBUM_ARTIST_NAME, as_index = True).agg(
            total_listen_time = (SPDTNM.TOTAL_LISTEN_TIME, 'sum')).sort_values(
            SPDTNM.TOTAL_LISTEN_TIME,
            ascending = False).head(top_artists_amount)

        total_listen_time_by_artist[SPDTNM.TOTAL_LISTEN_TIME] = total_listen_time_by_artist[
            SPDTNM.TOTAL_LISTEN_TIME].apply(lambda val: val / 1000 / 60 / 60)

        return total_listen_time_by_artist

    def calc_top_artists_albums_completion(self,
                                           top_artists_amount = 10,
                                           min_track_listen_percentage = 0.75,
                                           album_groups: list[str | tk.model.AlbumGroup] = None) -> pd.DataFrame:
        """
        Calculates how much of an artist's discography was listened to, for each top artist
        (top artists are determined by total listen time to their tracks regardless of discography).

        Parameters:
            top_artists_amount: Amount of artists considered "Top Artists" for calculating.

            min_track_listen_percentage: Decimal value between 0 and 1 (including boundaries), determining
                the percentage of a track's duration that should have been played in order for the track to be
                considered as "listened" for the calculation.

                Examples: 0.30 = at least 30% of the track should have been played.
                1.00 = the whole track should have been played.
                0.00 = even if the track was only skipped through, without being played at all, but still being documented
                in the listen history, it is considered as 'listened'.

            album_groups: List with the desired types of albums to fetch.
                Possible values: 'album', 'appears_on', 'compilation', 'single'.
                Default: ['album', 'appears_on'].

        Returns:
            DataFrame with the calculated values.
        """
        if min_track_listen_percentage < 0:
            min_track_listen_percentage = 0

        elif min_track_listen_percentage > 1:
            min_track_listen_percentage = 1

        tracks_count = self.agg_unique_tracks_by_listens().sort_values(by = SPDTNM.TOTAL_LISTEN_TIME, ascending = False)

        # Getting the top artists by total listen time (different from ``calc_top_artists_by_total_listen_time``):
        total_listen_time_by_artist = tracks_count.groupby(by = SPDBNM.V_KNOWN_LISTEN_HISTORY.ALBUM_ARTIST_ID,
                                                           as_index = True).agg(
            total_listen_time = (SPDTNM.TOTAL_LISTEN_TIME, 'sum'),
            album_artist_name = (SPDBNM.V_KNOWN_LISTEN_HISTORY.ALBUM_ARTIST_NAME, 'first')).sort_values(
            by = SPDTNM.TOTAL_LISTEN_TIME, ascending = False).head(top_artists_amount)

        artists_ids = total_listen_time_by_artist.index.to_list()

        listen_history_df = self.get_listen_history_df()
        listen_history_df = listen_history_df.drop(index = listen_history_df.index[
            ~listen_history_df[SPDBNM.V_KNOWN_LISTEN_HISTORY.ALBUM_ARTIST_ID].isin(artists_ids)], inplace = False)

        # Keeping only the most listened-to instance of each track by the top artists:
        history_max_played = listen_history_df.sort_values(
            by = [SPDBNM.V_KNOWN_LISTEN_HISTORY.TRACK_KNOWN_ID,
                  SPDBNM.V_KNOWN_LISTEN_HISTORY.MS_PLAYED],
            ascending = True).drop_duplicates(SPDBNM.V_KNOWN_LISTEN_HISTORY.TRACK_KNOWN_ID,
                                              keep = 'last')
        history_max_played.set_index(SPDBNM.V_KNOWN_LISTEN_HISTORY.TRACK_KNOWN_ID, inplace = True)

        history_max_played[PLTNM.IS_CONSIDERED_LISTENED] = np.select(
            [history_max_played[SPDBNM.V_KNOWN_LISTEN_HISTORY.MS_PLAYED] >= history_max_played[
                SPDBNM.V_KNOWN_LISTEN_HISTORY.TRACK_DURATION_MS] * min_track_listen_percentage], [True], False)

        artists_tracks = self.spapi.artists_get_all_tracks(artists_ids = artists_ids,
                                                           album_groups = album_groups)

        # Building a DataFrame with the 'is_considered_listened' value for each track for each artist:
        artists_tracks_flat = {}
        artist_track_listened_list_of_srs = []
        series_cols = [PLTNM.ARTIST_ID, PLTNM.TRACK_ID, PLTNM.IS_TRACK_LISTENED]

        for artist_id, albums in artists_tracks.items():
            for album_tuple in albums:
                for simple_track in album_tuple[1]:
                    try:
                        track_listened = history_max_played.loc[simple_track.id][PLTNM.IS_CONSIDERED_LISTENED]

                    except KeyError:
                        track_listened = False

                    if artists_tracks_flat.get(artist_id) is None:
                        artists_tracks_flat[artist_id] = {}

                    artists_tracks_flat[artist_id][simple_track.id] = track_listened

                    artist_track_listened_ser = pd.Series([artist_id, simple_track.id, track_listened],
                                                          index = series_cols)
                    artist_track_listened_list_of_srs.append(artist_track_listened_ser)

        artist_tracks_completion_df = pd.DataFrame(artist_track_listened_list_of_srs, columns = series_cols)

        # Calculating the percentage of listened tracks per each artist:
        artist_tracks_completion_df = artist_tracks_completion_df.groupby(PLTNM.ARTIST_ID).agg(
            artist_id = (PLTNM.ARTIST_ID, 'first'),
            listened_tracks = (PLTNM.IS_TRACK_LISTENED, 'sum'),
            total_tracks = (PLTNM.TRACK_ID, 'count')).set_index(PLTNM.ARTIST_ID)
        artist_tracks_completion_df.sort_index(ascending = True, inplace = True)

        artist_tracks_completion_df[PLTNM.PERCENTAGE_LISTENED] = \
            artist_tracks_completion_df[PLTNM.LISTENED_TRACKS].divide(
                artist_tracks_completion_df[PLTNM.TOTAL_TRACKS]) * 100

        # Adding the artists' names:
        names = listen_history_df[
            listen_history_df[SPDBNM.V_KNOWN_LISTEN_HISTORY.ALBUM_ARTIST_ID].isin(artist_tracks_completion_df.index)][
            [SPDBNM.V_KNOWN_LISTEN_HISTORY.ALBUM_ARTIST_ID,
             SPDBNM.V_KNOWN_LISTEN_HISTORY.ALBUM_ARTIST_NAME]].drop_duplicates().set_index(
            SPDBNM.V_KNOWN_LISTEN_HISTORY.ALBUM_ARTIST_ID)
        names.sort_index(ascending = True, inplace = True)

        artist_tracks_completion_df[PLTNM.ARTIST_NAME] = names
        artist_tracks_completion_df.reset_index(inplace = True)
        artist_tracks_completion_df.sort_values(by = PLTNM.PERCENTAGE_LISTENED,
                                                ascending = False,
                                                inplace = True)

        return artist_tracks_completion_df

    def calc_track_of_the_time_period(self,
                                      time_period: str = 'month',
                                      ) -> pd.DataFrame:
        """
        Calculates the most-listened track for each time period in the history, according to the given parameter.

        Parameters:
            time_period: The desired time period during which to measure the most listened track.
                Possible values: 'month', 'quarter', 'year'.

        Returns:
            Dataframe with the calculated results.
        """
        history_by_time_period = self.get_listen_history_df().copy()
        history_by_time_period['timestamp_std'] = pd.to_datetime(
            arg = history_by_time_period.loc[SPDBNM.V_KNOWN_LISTEN_HISTORY.TIMESTAMP],
            errors = 'raise',
            yearfirst = True)

    def agg_unique_tracks_by_listens(self) -> pd.DataFrame:
        """
        Returns the Listen History dataframe of the unique tracks (ids), each track with its aggregated
        total listens (count) and total listen time (sum), as well as its Album Artist ID and Name, & Album ID
        and Name.

        Returns:
            DataFrame with the aggregated listen history by each track's listens count and total listen time.
        """
        tracks_df = self.get_listen_history_df()

        # Removing all records of tracks that were played exactly 0 milliseconds:
        tracks_df = tracks_df.drop(index = tracks_df.index[tracks_df[SPDTNM.MS_PLAYED].eq(0)], inplace = False)

        tracks_count = tracks_df.groupby(SPDTNM.TRACK_KNOWN_ID,
                                         as_index = False).agg(
            times_listened = (SPDTNM.TRACK_KNOWN_ID, 'count'),
            total_listen_time = (SPDTNM.MS_PLAYED, 'sum'),
            album_artist_id = (SPDBNM.V_KNOWN_LISTEN_HISTORY.ALBUM_ARTIST_ID, 'first'),
            album_artist_name = (SPDTNM.ALBUM_ARTIST_NAME, 'first'),
            album_known_id = (SPDBNM.V_KNOWN_LISTEN_HISTORY.ALBUM_KNOWN_ID, 'first'),
            album_name = (SPDTNM.ALBUM_NAME, 'first'),
            track_known_id = (SPDBNM.V_KNOWN_LISTEN_HISTORY.TRACK_KNOWN_ID, 'first'),
            track_name = (SPDTNM.TRACK_NAME, 'first'))

        return tracks_count

    # endregion Calculations for plotting
