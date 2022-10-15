import pandas as pd
import tekore as tk
from dataclasses import dataclass
import log
import sp_utils as ut


@dataclass(frozen = True)
class AttrNames:
    TRACK_ID = 'track_id'
    ARTIST = 'artist'
    CONN_COUNTRY = 'conn_country'


class SpotifyAPIClient:
    """
    Spotify API Client logic. uses "Tekore" module to call Spotify API.
    """
    MAX_TRACKS_FOR_FEATURES = 100
    MAX_TRACKS_FOR_PLAYLIST_ITEMS = 100
    MAX_TRACKS_BATCH_SIZE_FOR_RECENTLY_PLAYED = 50
    MAX_TRACKS_AMOUNT_FOR_RECENTLY_PLAYED = 1000
    AUTH_SCOPE = "user-library-read playlist-read-collaborative playlist-read-private user-read-recently-played"
    REDIRECT_URI = "http://localhost:8888/spotify/callback"

    def __init__(self, token):
        self.app_token: tk.RefreshingToken = None
        self.user_token: tk.RefreshingToken = None
        self.client = tk.Spotify(token = self.get_app_token(token),
                                 max_limits_on = True,
                                 chunked_on = True)

        self.input_user_token(token)

    # region Connection logic

    def get_app_token(self,
                      token = None) -> tk.RefreshingToken:
        if token is not None:
            cl_id = token[0].strip()
            cl_secret = token[1].strip()
            self.app_token = tk.request_client_token(cl_id,
                                                     cl_secret)

        return self.app_token

    def input_user_token(self, token):
        if token is not None:
            cl_id = token[0].strip()
            cl_secret = token[1].strip()

            redirect_uri = 'http://localhost:8888/spotify/callback'

            self.user_token = tk.prompt_for_user_token(client_id = cl_id,
                                                       client_secret = cl_secret,
                                                       redirect_uri = redirect_uri,
                                                       scope = [tk.scope.user_read_private,
                                                                tk.scope.user_read_recently_played])

    def connect(self) -> None:
        self.client = tk.Spotify(token = self.get_app_token(),
                                 max_limits_on = True,
                                 chunked_on = True)

    def disconnect(self) -> None:
        if self.is_connected():
            self.client.close()

    def is_connected(self) -> bool:
        return False if self.client is None else True

    def validate_connection(self) -> None:
        if not self.is_connected():
            self.connect()

            if not self.is_connected():
                raise ValueError("Client is not connected to Spotify API")

    # endregion

    def user_get_all_recently_played(self) -> None | list[tk.model.PlayHistory]:
        with self.client.token_as(self.user_token):
            log.write(message = log.FETCHING_RECENTLY_PLAYED)

            recently_played_paging = None

            try:
                # Calling the API to get the current user's Recently Played tracks:
                recently_played_paging = self.client.playback_recently_played(limit = 50)
                all_recently_played = recently_played_paging.items

                while recently_played_paging.next is not None:
                    recently_played_paging = self.client.next(recently_played_paging)
                    all_recently_played.extend(recently_played_paging.items)

                log.write(message = log.RECENTLY_PLAYED_FETCHED)

            except tk.ServiceUnavailable as ex:
                message = log.API_SERVICE_UNAVAILABLE.format(ex)
                log.write(message = message)

                raise tk.ServiceUnavailable(message = message)

        return all_recently_played

    def get_all_user_playlists(self, limit = 50):
        self.validate_connection()

        response = self.client.playlists(user_id = self.client.current_user().id,
                                         limit = limit)
        results = response['items']

        while response.next is not None:
            results.extend(response.next())

        return results

    def playlist_get_all_tracks(self,
                                playlist_id: list,
                                limit = MAX_TRACKS_FOR_PLAYLIST_ITEMS):
        self.validate_connection()

        response = self.client.playlist_items(playlist_id, limit = limit)
        results = response['items']
        offset = limit
        while response['next'] is not None:
            response = self.client.playlist_items(playlist_id,
                                                  limit = limit,
                                                  offset = offset)
            results.extend(response['items'])
            offset += limit

        return results

    def find_artist(self,
                    name: str):
        '''
        param name: Name of the desired Artist to find
        '''
        self.validate_connection()

        result = self.client.search(name,
                                    types = (AttrNames.ARTIST,),
                                    limit = 1)

        return result[0].items[0]

    def artist_get_all_tracks(self,
                              artist_id: str):
        '''
        Returns a list of all the tracks by the given Artist.
        param artist_id: ID of the desired Artist
        '''
        self.validate_connection()

        all_tracks = tk.model.ModelList()

        artist_albums = self.artist_get_all_albums(artist_id)
        curr_i = 1

        for album in artist_albums:
            print("Album #" + str(curr_i) + ' - ' + album.name)

            album_tracks = self.album_get_all_tracks(album.id)

            # for track in album_tracks:
            all_tracks.extend(album_tracks)

            curr_i += 1

        return all_tracks

    def artist_get_all_albums(self,
                              artist_id: str) -> tk.model.ModelList:
        self.validate_connection()

        albums_paging = self.client.artist_albums(artist_id = artist_id,
                                                  include_groups = [tk.model.AlbumGroup.album, ])
        all_albums = albums_paging.items

        while albums_paging.next is not None:
            albums_paging = self.client.next(albums_paging)
            all_albums.extend([item for item in albums_paging.items if item.album_type == tk.model.AlbumType.album])

        return all_albums

    def album_get_all_tracks(self,
                             album_id: str) -> tk.model.ModelList:
        '''
        Returns all Tracks in a given Album.
        :param album_id: ID of the desired Album.
        :return: All Tracks in the desired Album (list)
        '''
        self.validate_connection()

        tracks_paging = self.client.album_tracks(album_id = album_id)
        all_tracks = tracks_paging.items

        while tracks_paging.next is not None:
            tracks_paging = self.client.next(tracks_paging)
            all_tracks.extend(tracks_paging.items)

        for track in all_tracks:
            print(str(track.disc_number) + '-' + str(track.track_number) + '. ' + track.name)

        print('\n')

        return all_tracks

    def get_tracks_audio_features(self,
                                  tracks_ids: list):
        '''
        :param tracks_ids: IDs of the desired tracks to get Audio Features of.
        :param limit:
        :return: List of
        '''
        self.validate_connection()

        all_features = self.client.tracks_audio_features(track_ids = tracks_ids)

        return all_features

    def get_specific_audio_feature(self,
                                   tracks_items: list,
                                   audio_feature: str):
        '''
        Returns a list of a desired audio feature for all given tracks.
        :param tracks_items:
        :param audio_feature:
        :return:
        '''
        self.validate_connection()

        tracks_audio_features = self.get_tracks_audio_features(tracks_items)
        result = list()

        for track in tracks_audio_features:
            # for audio_feature in audio_features
            result.append(track[audio_feature])

        return result

    def get_audio_analysis(self,
                           track_id):
        self.validate_connection()

        analysis = self.client.audio_analysis(track_id)

        return analysis

    def get_audio_analysis(self,
                           tracks_ids: list):
        self.validate_connection()

        analyses = list()

        for track_id in tracks_ids:
            analyses.append(self.client.audio_analysis(track_id))

        return analyses

    # def create_df_full_tracks(self, tracks_items: list,
    #                           audio_features_names: list,
    #                           limit = MAX_TRACKS_FOR_FEATURES) -> pd.DataFrame:
    #     self.validate_connection()
    #
    #     # tracks_with_features = {'track_name': get_tracks_names(tracks_items)}
    #     #
    #     # test_artists_list = get_tracks_artists(tracks_items)
    #     #
    #     # tracks_with_features['artist'] =
    #     #
    #     # for feat_name in audio_features_names:
    #     #     # Inserting a new column, titled feat_name, containing a list of all the audio features for all the tracks:
    #     #     tracks_with_features[feat_name] = self.get_specific_audio_feature(tracks_items,
    #     #                                                                       audio_feature = feat_name,
    #     #                                                                       limit = limit)
    #     #
    # return pd.DataFrame(data = tracks_with_features)

    def get_full_tracks(self, tracks_ids: pd.Series | set | list) -> tk.model.ModelList[tk.model.FullTrack]:
        """
        For each Track ID in the given collection, fetches its :class:`tk.model.FullTrack` object from the API.

        Parameters:
            tracks_ids: All the required tracks' ID's.

        Returns:
            Tekore ModelList of FullTracks, for the given tracks collection.

        Raises:
            tk.ServiceUnavailable: if Spotify's API service is unavailable.
        """
        unique_tracks_list = ut.get_unique_vals_list(tracks_ids)

        full_tracks = None

        with self.client.token_as(self.user_token):
            log.write(message = log.FETCHING_TRACKS_ATTRS.format(len(unique_tracks_list)))

            try:
                # Calling the API to get FullTracks for the given tracks' ID's.
                # Parameter `market` is needed here, to get the Linked Track for each track:
                full_tracks = self.client.tracks(track_ids = unique_tracks_list,
                                                 market = self.client.current_user().country)

                log.write(message = log.TRACKS_ATTRS_FETCHED.format(len(full_tracks)))

            except tk.ServiceUnavailable as ex:
                message = log.API_SERVICE_UNAVAILABLE.format(ex)
                log.write(message = message)

                raise tk.ServiceUnavailable(message = message)

        return full_tracks

    def get_full_artists(self, artists_ids: pd.Series | set | list) -> tk.model.ModelList[tk.model.FullArtist]:
        """
        For each Artist ID in the given collection, fetches its :class:`tk.model.FullArtist` object from the API.

        Parameters:
            artists_ids: All the required artists' ID's.

        Returns:
            Tekore ModelList of FullArtists, for the given artists collection.

        Raises:
            tk.ServiceUnavailable: if Spotify's API service is unavailable.
        """
        unique_artists_list = ut.get_unique_vals_list(artists_ids)
        full_artists = None

        with self.client.token_as(self.user_token):
            log.write(message = log.FETCHING_ARTISTS_ATTRS.format(len(unique_artists_list)))

            try:
                # Calling the API to get FullArtists for the given artists' ID's.
                full_artists = self.client.artists(artist_ids = unique_artists_list)

                log.write(message = log.ARTISTS_ATTRS_FETCHED.format(len(full_artists)))

            except tk.ServiceUnavailable as ex:
                message = log.API_SERVICE_UNAVAILABLE.format(ex)
                log.write(message = message)

                raise tk.ServiceUnavailable(message = message)

        return full_artists

    def get_full_albums(self, albums_ids: pd.Series | set | list) -> tk.model.ModelList[tk.model.FullAlbum]:
        """
        For each Album ID in the given collection, fetches its :class:`tk.model.FullAlbum` object from the API.

        Parameters:
            albums_ids: All the required albums' ID's.

        Returns:
            Tekore ModelList of FullAlbums, for the given albums series.

        Raises:
            tk.ServiceUnavailable: if Spotify's API service is unavailable.
        """
        unique_albums_list = ut.get_unique_vals_list(albums_ids)
        full_albums = None

        with self.client.token_as(self.user_token):
            log.write(message = log.FETCHING_ALBUMS_ATTRS.format(len(unique_albums_list)))

            try:
                # Calling the API to get FullAlbums for the given albums' ID's.
                # Parameter `market` is not sent, in order to get the Available Markets for each album:
                full_albums = self.client.albums(album_ids = unique_albums_list)

                log.write(message = log.ALBUMS_ATTRS_FETCHED.format(len(full_albums)))

            except tk.ServiceUnavailable as ex:
                message = log.API_SERVICE_UNAVAILABLE.format(ex)
                log.write(message = message)

                raise tk.ServiceUnavailable(message = message)

        return full_albums

    def get_artists_albums(self,
                           artists_ids: pd.Series | set | list) -> dict:
        """
        For each Artist ID in the given collection, fetches all of their albums from the API.

        **Note:** This method is **very inefficient** and can **take a very long time to finish**.

        This is because the Spotify API only allows to get the albums of a single artist in a single API call.
        At the time of development, there is no option to batch-fetch the albums of multiple artists in the same call,
        and it took me 20 minutes to fetch the albums of 6700 artists.

        Parameters:
            artists_ids: ID's of all the required artists.

        Returns:
            Dictionary, containing one entry for each artist, with a ModelList of all that artist's albums.

        Raises:
            tk.ServiceUnavailable: if Spotify's API service is unavailable.
        """
        unique_artists_list = []
        all_artists_albums = {}

        match artists_ids:
            case pd.Series() as artists_ids:
                unique_artists_list = artists_ids.dropna().unique().tolist()

            case set() as artists_ids:
                unique_artists_list = list(artists_ids)

            case list() as artists_ids:
                unique_artists_list = artists_ids

        with self.client.token_as(self.user_token):
            log.write(message = log.FETCHING_ARTISTS_ALBUMS_ATTRS.format(len(unique_artists_list)))

            try:
                # Calling the API to get all albums for each given Artist ID:
                for artist in unique_artists_list:
                    all_artists_albums[artist] = self.client.artist_albums(artist_id = artist)

                log.write(message = log.ARTISTS_ALBUMS_ATTRS_FETCHED.format(len(unique_artists_list)))

            except tk.ServiceUnavailable as ex:
                message = log.API_SERVICE_UNAVAILABLE.format(ex)
                log.write(message = message)

                raise tk.ServiceUnavailable(message = message)

        return all_artists_albums

    def get_track_known_id_map(self,
                               full_tracks: tk.model.ModelList[tk.model.FullTrack],
                               tracks: pd.Series = None) -> dict:
        """
        For each given Track, determine the single TrackID that is known to be valid and available.
        There are two possible cases:

        * if the track **has** a ``linked_from`` object, the LinkedFrom ID is mapped to the given ID.
        * if the track **has no** ``linked_from`` object, the given id is mapped to itself.
        If a ``full_tracks`` object is given, the method uses it without calling the API.
        Otherwise, the method calls the API with the given ``tracks`` Series object.

        Parameters:
            full_tracks: Tekore ModelList of FullTracks, from which to take the TrackKnownID values.
            tracks: Series of TrackID values, to call the API with.

        Returns:
            Dictionary mapping each given ID to its 'known' ID (can be the same ID or different).
        """
        try:
            _full_tracks = full_tracks if full_tracks is not None else self.get_full_tracks(tracks)
            tracks_known_ids_map = {}

            for track in _full_tracks:
                try:
                    if (track is not None) and getattr(track, 'linked_from') is not None:
                        tracks_known_ids_map[track.linked_from.id] = track.id

                    else:
                        tracks_known_ids_map[track.id] = track.id

                except AttributeError:
                    tracks_known_ids_map[track.id] = track.id

            return tracks_known_ids_map

        except tk.ServiceUnavailable as ex:
            return
