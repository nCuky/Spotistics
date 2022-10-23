import log
from spotify_api_client import SpotifyAPIClient as spapi
import general_utils as utl
import db
import db_names as spdbnm
import sp_data_set as spdt
from sp_data_set_names import Spdt as spdtnm
import pandas as pd
import tekore as tk


# import pickle
# import pyspark as sk


class Logic:
    """
    The app's main Logic.
    """

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
    def clean_json_for_public_repo():
        """
        Small utility meant to be performed only once.

        Removes private data (such as platform, IP address, etc.) from the listen history JSON files,
        so they can be included as sample data in the GitHub repo.

        Returns:
            None
        """
        spdt.SpotifyDataSet.clean_json_for_public_repo()

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
        track_dict_to_insert = {spdbnm.TRACKS.ID          : full_track.id,
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
        album_dict_to_insert = {spdbnm.ALBUMS.ID                    : full_track.album.id,
                                spdbnm.ALBUMS.NAME                  : full_track.album.name,
                                spdbnm.ALBUMS.TOTAL_TRACKS          : full_track.album.total_tracks,
                                spdbnm.ALBUMS.RELEASE_DATE          : full_track.album.release_date,
                                spdbnm.ALBUMS.RELEASE_DATE_PRECISION: full_track.album.release_date_precision.value,
                                spdbnm.ALBUMS.ALBUM_TYPE            : full_track.album.album_type.value,
                                spdbnm.ALBUMS.IS_AVAILABLE          : None,
                                spdbnm.ALBUMS.HREF                  : full_track.album.href,
                                spdbnm.ALBUMS.URI                   : full_track.album.uri}

        all_albums_list.append(album_dict_to_insert)
        all_albums_ids.add(full_track.album.id)

    @staticmethod
    def _add_artist_to_list_to_save(full_track: tk.model.FullTrack,
                                    all_artists_list: list[dict],
                                    all_artists_ids: set,
                                    all_artists_albums_list: list[dict]) -> None:
        """
        From a given :class:`tk.model.FullTrack` object, takes the **Artists** attributes and adds them to a given
        list of named dictionaries. Also, adds the Artist's Album to another list of named dicts.

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
        for artist in full_track.artists:
            artist_dict_to_insert = {spdbnm.ARTISTS.ID             : artist.id,
                                     spdbnm.ARTISTS.NAME           : artist.name,
                                     spdbnm.ARTISTS.TOTAL_FOLLOWERS: None,
                                     spdbnm.ARTISTS.POPULARITY     : None,
                                     spdbnm.ARTISTS.HREF           : artist.href,
                                     spdbnm.ARTISTS.URI            : artist.uri}

            all_artists_list.append(artist_dict_to_insert)
            all_artists_ids.add(artist.id)

            # Building a collection of all Albums-of-Artists.
            # Only the track's Artists that also belong to the Track's Album's Artists (except when related to it
            # with 'Appears On' relationship) are collected.
            if artist in full_track.album.artists:
                # and ((full_track.album.album_group is None) or
                #      (full_track.album.album_group != tk.model.AlbumGroup.appears_on)):
                artist_album_dict_to_insert = {spdbnm.ARTISTS_ALBUMS.ARTIST_ID  : artist.id,
                                               spdbnm.ARTISTS_ALBUMS.ALBUM_ID   : full_track.album.id,
                                               spdbnm.ARTISTS_ALBUMS.ALBUM_GROUP: None}

                all_artists_albums_list.append(artist_album_dict_to_insert)

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
        self.my_spapi = spapi(token_keys = Logic.get_token())
        self.my_db = db.DB()

        if listen_history_from == 'json':
            self.my_spdt = spdt.SpotifyDataSet(db_handler = None)
            self.collect_data_and_save(to_csv_also = False)

        else:
            self.my_spdt = spdt.SpotifyDataSet(db_handler = self.my_db)

    def get_artist_audio_features_data(self, name: str):
        artist_id = self.my_spapi.find_artist(name).id
        artist_tracks = self.my_spapi.artists_get_all_tracks(artist_id)
        tracks_ids = [track.id for album in artist_tracks[artist_id] for track in album[1]]
        tracks_features = self.my_spapi.get_tracks_audio_features(tracks_ids)

        # pd.read_json()

        return tracks_features

    def calc_listen_data_by_key(self) -> None:
        """
        Aggregates all listened tracks by key, and writes it as a csv file
        :return:
        """
        only_duration = self.my_spdt.listen_history_df.groupby(spdtnm.SONG_KEY)[
            spdtnm.MS_PLAYED].sum()

        my_results = self.my_spdt.listen_history_df.drop(columns = spdtnm.MS_PLAYED).groupby(
            spdtnm.SONG_KEY).mean().assign(
            duration_ms = only_duration)

        utl.write_df_to_file(my_results, "listen_data_by_key.csv")

    def calc_listen_data_mean_key(self) -> None:
        """
        Aggregates all listened tracks by mean key, and saves it into a CSV file.

        Returns:
            None.
        """
        self.my_spdt.listen_history_df.groupby(spdtnm.SONG_KEY).mean().to_csv("mean_by_key.csv")

    def count_unique_tracks(self) -> pd.DataFrame:
        tracks_df = self.my_spdt.listen_history_df

        # Removing all records of tracks that were played exactly 0 milliseconds:
        # tracks_df = tracks_df.drop(index = tracks_df.index[tracks_df[spdtnm.MS_PLAYED].eq(0)], inplace = False)

        tracks_count = tracks_df.groupby(spdtnm.TRACK_KNOWN_ID,
                                         as_index = False).agg(
            times_listened = (spdtnm.TRACK_KNOWN_ID, 'count'),
            total_listen_time = (spdtnm.MS_PLAYED, 'sum'),
            album_artist_name = (spdtnm.ALBUM_ARTIST_NAME, 'first'),
            album_name = (spdtnm.ALBUM_NAME, 'first'),
            track_name = (spdtnm.TRACK_NAME, 'first'))

        return tracks_count

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

        utl.write_df_to_file(self.my_spdt.listen_history_df, track_file_name)

    def collect_data_and_save(self, to_csv_also: bool = False):
        """
        Collects all listen history, extracts models' (tracks, artists, etc.) information from it
        and saves it in the local DB.
        Then, replaces the inner :class:`spdt.SpotifyDataSet` dataset manager with a new manager
        containing all the updated data.

        Parameters:
            to_csv_also: Whether to also save the listen history to a CSV file, after saving into the DB.

        Returns:
            None.
        """
        try:
            full_tracks_mdlist = self.my_spapi.get_full_tracks(self.my_spdt.listen_history_df[spdtnm.TRACK_ID])

            self.save_full_tracks_to_db(full_tracks_mdlist)

        except tk.ServiceUnavailable as ex:
            log.write(log.API_SERVICE_UNAVAILABLE.format(ex))

            return

        if to_csv_also:
            self.save_listen_history_to_csv('known_listen_history_{0}.csv')

        self.my_spdt = spdt.SpotifyDataSet(db_handler = self.my_db)

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
        all_albums_list_to_insert = []
        all_albums_tracks_list_to_insert = []
        all_artists_albums_list_to_insert = []

        map_known_tracks_to_albums = {}
        map_albums_to_tracks = {}
        known_ids_of_linked_tracks = set()
        map_linked_to_known_tracks = {}
        map_linked_to_known_albums = {}
        all_suspected_album_ids = set()
        all_albums_ids = set()
        all_artists_ids = set()

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
                linked_track_dict_to_insert = {spdbnm.LINKED_TRACKS.FROM_ID    : full_track.id,
                                               spdbnm.LINKED_TRACKS.RELINKED_ID: full_track.id}

                # "Fake-linking" the Album ID to itself, to maintain consistency later in the DB:
                linked_album_dict_to_insert = {spdbnm.LINKED_ALBUMS.FROM_ID    : full_track.album.id,
                                               spdbnm.LINKED_ALBUMS.RELINKED_ID: full_track.album.id}

                all_linked_albums_list_to_insert.append(linked_album_dict_to_insert)

                # Assigning the Track ID to the Album ID:
                album_track_dict_to_insert = {spdbnm.ALBUMS_TRACKS.ALBUM_ID: full_track.album.id,
                                              spdbnm.ALBUMS_TRACKS.TRACK_ID: full_track.id}

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
                linked_track_dict_to_insert = {spdbnm.LINKED_TRACKS.FROM_ID    : full_track.linked_from.id,
                                               spdbnm.LINKED_TRACKS.RELINKED_ID: full_track.id}

                # Linking the (suspected as linked) Album ID to the Track's LinkedFrom ID:
                album_track_dict_to_insert = {spdbnm.ALBUMS_TRACKS.ALBUM_ID: full_track.album.id,
                                              spdbnm.ALBUMS_TRACKS.TRACK_ID: full_track.linked_from.id}

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

                    linked_album_dict_to_insert = {spdbnm.LINKED_ALBUMS.FROM_ID    : full_track.album.id,
                                                   spdbnm.LINKED_ALBUMS.RELINKED_ID: known_album_id}

                    all_linked_albums_list_to_insert.append(linked_album_dict_to_insert)

            all_linked_tracks_list_to_insert.append(linked_track_dict_to_insert)
            all_albums_tracks_list_to_insert.append(album_track_dict_to_insert)

            Logic._add_album_to_list_to_save(full_track, all_albums_list_to_insert, all_albums_ids)

            Logic._add_artist_to_list_to_save(full_track,
                                              all_artists_list_to_insert,
                                              all_artists_ids,
                                              all_artists_albums_list_to_insert)

        # I've collected the ID's of only the tracks that are Relinked from other tracks.
        # Here, fetching FullTrack attributes only for those relinked tracks (their IDs were only discovered earlier
        # in the iterated FullTracks list, and now I need the rest of their attributes):
        known_full_tracks = self.my_spapi.get_full_tracks(tracks_ids = known_ids_of_linked_tracks)

        for known_full_track in known_full_tracks:
            map_known_tracks_to_albums[known_full_track.id] = known_full_track.album.id

            utl.add_to_mapping_of_sets(mapping = map_albums_to_tracks,
                                       key = known_full_track.album.id,
                                       value = known_full_track.id)

            Logic._add_track_to_list_to_save(known_full_track, all_tracks_list_to_insert)

            Logic._add_album_to_list_to_save(known_full_track, all_albums_list_to_insert, all_albums_ids)

            Logic._add_artist_to_list_to_save(known_full_track,
                                              all_artists_list_to_insert,
                                              all_artists_ids,
                                              all_artists_albums_list_to_insert)

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

                        linked_album_dict_to_insert = {spdbnm.LINKED_ALBUMS.FROM_ID    : suspected_album_id,
                                                       spdbnm.LINKED_ALBUMS.RELINKED_ID: known_album_id}

                        all_linked_albums_list_to_insert.append(linked_album_dict_to_insert)

                    break

            else:
                # By now, I don't really need to map the found KnownAlbumID, as it will not be searched anymore:
                map_linked_to_known_albums[suspected_album_id] = known_album_id

                linked_album_dict_to_insert = {spdbnm.LINKED_ALBUMS.FROM_ID    : suspected_album_id,
                                               spdbnm.LINKED_ALBUMS.RELINKED_ID: known_album_id}

                all_linked_albums_list_to_insert.append(linked_album_dict_to_insert)

        # Keeping only the unique values in each list:
        all_tracks_list_unq = utl.get_unique_dicts(all_tracks_list_to_insert)
        all_linked_tracks_list_unq = utl.get_unique_dicts(all_linked_tracks_list_to_insert)
        all_linked_albums_list_unq = utl.get_unique_dicts(all_linked_albums_list_to_insert)
        all_artists_list_unq = utl.get_unique_dicts(all_artists_list_to_insert)
        all_albums_list_unq = utl.get_unique_dicts(all_albums_list_to_insert)
        all_albums_tracks_list_unq = utl.get_unique_dicts(all_albums_tracks_list_to_insert)
        all_artists_albums_list_unq = utl.get_unique_dicts(all_artists_albums_list_to_insert)

        # Filling attribute `is_available` for each album:
        full_albums = self.my_spapi.get_full_albums(all_albums_ids)

        albums_availability = {_full_album.id: len(_full_album.available_markets) > 0 for _full_album in full_albums}

        for i, full_album in enumerate(all_albums_list_unq):
            all_albums_list_unq[i][spdbnm.ALBUMS.IS_AVAILABLE] = albums_availability[full_album[spdbnm.ALBUMS.ID]]

        # Inserting all values to the corresponding DB-tables:
        self.my_db.insert_listen_history(self.my_spdt.listen_history_df)
        self.my_db.insert_tracks(all_tracks_list_unq)
        self.my_db.insert_linked_tracks(all_linked_tracks_list_unq)
        self.my_db.insert_linked_albums(all_linked_albums_list_unq)
        self.my_db.insert_artists(all_artists_list_unq)
        self.my_db.insert_albums(all_albums_list_unq)
        self.my_db.insert_albums_tracks(all_albums_tracks_list_unq)
        self.my_db.insert_artists_albums(all_artists_albums_list_unq)

        self.my_db.commit()
