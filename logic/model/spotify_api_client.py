import pandas as pd
import tekore as tk
from dataclasses import dataclass
from logic.frontend import log
from logic import general_utils as ut


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

    def __init__(self, token_keys: list[str]):
        self.__token_keys = {'id': '', 'secret': ''}
        self.__app_token: tk.RefreshingToken = None
        self.__user_token: tk.RefreshingToken = None
        self.__redirect_uri: str = ''

        self.client = tk.Spotify(token = self.get_app_token(token_keys),
                                 max_limits_on = True,
                                 chunked_on = True)

        self.__input_user_token(token_keys)

    # region Connection logic

    @property
    def user_token(self) -> tk.RefreshingToken:
        """
        Returns the existing User-Token (:class:`tk.RefreshingToken`) if available,
        otherwise requests, generates and returns a new one.

        Returns:
            User-Token object, either existing one or a newly generated one.
        """
        if self.__user_token is None:
            self.__input_user_token()

        return self.__user_token

    def __input_user_token(self, token_keys: list[str] = None) -> tk.RefreshingToken:
        """
        Inputs a new User-Token (:class:`tk.RefreshingToken`), either from the ``token_keys`` parameter (when supplied),
        or from the existing ClientID and ClientSecret.

        Parameters:
            token_keys: List containing the ClientID and ClientSecret, used as keys for generating a new User-Token.

        Returns:
            tk.RefreshingToken: The newly generated User Token.
        """
        if token_keys is not None:
            self.__set_token_keys(token_keys[0].strip(), token_keys[1].strip())

        self.__redirect_uri = 'http://localhost:8888/spotify/callback'

        self.__user_token = tk.prompt_for_user_token(client_id = self.token_keys['id'],
                                                     client_secret = self.token_keys['secret'],
                                                     redirect_uri = self.__redirect_uri,
                                                     scope = [tk.scope.user_read_private,
                                                              tk.scope.user_read_recently_played])

        return self.__user_token

    @property
    def token_keys(self) -> dict:
        """
        Returns the inner token keys dictionary, containing the ClientID and ClientSecret.

        Returns:
            Dictionary with the token keys.
        """
        return self.__token_keys

    def __set_token_keys(self,
                         client_id: str = '',
                         client_secret: str = '') -> None:
        """
        Sets the ClientID and ClientSecret, used as keys for the app- and user-token.

        Parameters:
            client_id: Client ID (string), got from the Spotify for Developers account details.
            client_secret: Client Secret (string), got from the Spotify for Developers account details.

        Returns:
            None.
        """
        self.__token_keys['id'] = client_id
        self.__token_keys['secret'] = client_secret

    def get_app_token(self,
                      token_keys: list[str] = None) -> tk.RefreshingToken:
        """
        Returns the client's App Token. If does not exist yet, it generates a new one from the existing ClientID and
        ClientSecret.

        Parameters:
            token_keys: List with the keys (ClientID, ClientSecret) for generating the App Token.

        Returns:
            The App token.
        """
        if token_keys is not None:
            self.__set_token_keys(token_keys[0].strip(), token_keys[1].strip())

        if self.__app_token is None:
            self.__app_token = tk.request_client_token(self.token_keys['id'],
                                                       self.token_keys['secret'])

        return self.__app_token

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

    # endregion Connection logic

    def user_get_all_recently_played(self) -> None | list[tk.model.PlayHistory]:
        """
        **Does not work as intended.**

        **Supposed** to fetch all recently played tracks for the current user, but the Spotify API limits the
        results in a way that can't be circumvented at the moment.

        Thus, in practice this method **only fetches the 50 most recent tracks**.

        Returns:
            tk.model.PlayHistory object, containing the user's 50 recently played tracks.
        """
        with self.client.token_as(self.user_token):
            log.write(message = log.FETCHING_RECENTLY_PLAYED)

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

                raise tk.ServiceUnavailable(message = message, request = ex.request, response = ex.response)

        return all_recently_played

    def get_all_user_playlists(self, limit = 50):
        self.validate_connection()

        response = self.client.playlists(user_id = self.client.current_user().id,
                                         limit = limit)
        results = response['items']

        while response.next is not None:
            results.extend(response.next)

        return results

    def playlist_get_all_tracks(self,
                                playlist_id: str,
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
        """
        Searches for an artist by a given name.

        Parameters:
            name: Name of the desired Artist to find.

        Returns:
            The ID of the found artist, if found.
        """
        self.validate_connection()

        result = self.client.search(name,
                                    types = (AttrNames.ARTIST,),
                                    limit = 1)

        return result[0].items[0]

    def album_get_all_tracks(self,
                             album_id: str) -> list[tk.model.SimpleTrack]:
        """
        Returns all Tracks in a given Album.

        Parameters:
            album_id: ID of the desired Album.

        Returns:
            All Tracks in the desired Album (list)
        """
        self.validate_connection()

        tracks_paging = self.client.album_tracks(album_id = album_id)
        all_tracks = tracks_paging.items

        while tracks_paging.next is not None:
            tracks_paging = self.client.next(tracks_paging)
            all_tracks.extend(tracks_paging.items)

        return all_tracks

    def artists_get_all_tracks(self,
                               artists_ids: str | set | list | pd.Series,
                               album_groups: list[str | tk.model.AlbumGroup] = None) -> dict[
        str, list[tuple[tk.model.SimpleAlbum, list[tk.model.FullTrack]]]]:
        """
        For each Artist ID in the given collection, fetches all of their **Tracks** from the API.

        **Note:** This method is **very inefficient** and can **take a very long time to finish** if called on a large
        number of artists' IDs.

        This is because the Spotify API only allows to get the Albums of a **single** artist in a single API call.
        The *albums* are needed in order to get each album's *tracks*
        (thankfully, it *is* possible to get the tracks for multiple albums a single API call).

        At the time of development, there is no option to batch-fetch the albums of multiple artists in the same call,
        and it took me 20 minutes to fetch the albums of 6700 artists.

        Parameters:
            artists_ids: A single ID or multiple IDs of all the desired artists.

            album_groups: List with the desired types of albums to fetch.
                Possible values: 'album', 'appears_on', 'compilation', 'single'.
                Default: ['album', 'appears_on'].

        Returns:
            dict[str, list[tuple[tk.model.SimpleAlbum, list[tk.model.FullTrack]]]]:
                Dictionary of lists of tuples of Full[Model] objects, as follows:
                The dict's keys are the artists' IDs. For each artist, the dict's value is a list of tuples, each tuple
                containing two items: [0] = FullAlbum object, [1] = list of FullTracks that belong to that album.

        Raises:
            tk.ServiceUnavailable: if Spotify's API service is unavailable.
        """
        self.validate_connection()

        unique_artists_list = ut.get_unique_vals_list(artists_ids)
        all_artists_tracks = {}

        with self.client.token_as(self.user_token):
            log.write(message = log.FETCHING_ARTISTS_TRACKS_ATTRS.format(len(unique_artists_list)))

            try:
                # Calling the API to get all albums for each given Artist ID:
                all_artists_albums_dict = self.artists_get_all_albums(artists_ids = unique_artists_list,
                                                                      album_groups = album_groups)
                all_albums_ids = [album.id for albums_list in all_artists_albums_dict.values() for album in albums_list]

                log.write(message = log.FETCHING_ALBUMS_ATTRS.format(len(all_albums_ids)))

                all_full_albums = self.client.albums(album_ids = all_albums_ids,
                                                     market = self.client.current_user().country)

                log.write(message = log.ALBUMS_ATTRS_FETCHED.format(len(all_albums_ids)))

                for album in all_full_albums:
                    for artist in album.artists:
                        if artist.id in unique_artists_list:
                            if all_artists_tracks.get(artist.id) is None:
                                all_artists_tracks[artist.id] = []

                            album_tracks_paging = album.tracks
                            album_tracks_list = album_tracks_paging.items

                            while album_tracks_paging.next is not None:
                                album_tracks_paging = self.client.next(album_tracks_paging)
                                album_tracks_list.extend(album_tracks_paging.items)

                            # It would have been best to put the tracks list as the `tracks` property of `albums`,
                            # but unfortunately `tracks` is a Paging object and can't be replaced with a list object.
                            # The `tracks` Paging object might not contain all the tracks
                            # (if their amount exceeds the limit), and I want to have all of them without needing to
                            # call the API's `next()` method.
                            # Thus, I had to save both objects inside a tuple:
                            artist_album = (album, album_tracks_list)
                            all_artists_tracks[artist.id].append(artist_album)

                log.write(message = log.ARTISTS_TRACKS_ATTRS_FETCHED.format(len(unique_artists_list)))

            except tk.ServiceUnavailable as ex:
                message = log.API_SERVICE_UNAVAILABLE.format(ex)
                log.write(message = message)

                raise tk.ServiceUnavailable(message = message, request = ex.request, response = ex.response)

            except tk.Unauthorised as ex:
                message = log.API_SERVICE_UNAUTHORIZED.format(ex)
                log.write(message = message)

                raise tk.Unauthorised(message = message, request = ex.request, response = ex.response)

        return all_artists_tracks

    def artists_get_all_albums(self,
                               artists_ids: str | set | list | pd.Series,
                               album_groups: list[str | tk.model.AlbumGroup] = None) -> dict[
        str, list[tk.model.SimpleAlbum]]:
        """
        For each Artist ID in the given collection, fetches all of their **Albums** from the API.

        **Note:** This method is **very inefficient** and can **take a very long time to finish** if called on a large
        number of artists' IDs.

        This is because the Spotify API only allows to get the albums of a **single** artist in a single API call.
        At the time of development, there is no option to batch-fetch the albums of multiple artists in the same call,
        and it took me 20 minutes to fetch the albums of 6700 artists.

        Parameters:
            artists_ids: A single ID or multiple IDs of all the desired artists.

            album_groups: List with the desired types of albums to fetch.
                Possible values: 'album', 'appears_on', 'compilation', 'single'.
                Default: ['album', 'appears_on'].

        Returns:
            Dictionary, containing one entry for each artist, with a ModelList of all that artist's albums.

        Raises:
            tk.ServiceUnavailable: if Spotify's API service is unavailable.
        """
        include_album_groups = album_groups if album_groups is not None else [tk.model.AlbumGroup.album,
                                                                              tk.model.AlbumGroup.appears_on]
        unique_artists_list = ut.get_unique_vals_list(artists_ids)
        all_artists_albums = {}

        with self.client.token_as(self.user_token):
            log.write(message = log.FETCHING_ARTISTS_ALBUMS_ATTRS.format(len(unique_artists_list)))

            try:
                # Calling the API to get all albums for each given Artist ID:
                for artist in unique_artists_list:
                    # client.artist_albums() is not chunked, meaning the results are paged.
                    artist_albums_paging = self.client.artist_albums(artist_id = artist,
                                                                     include_groups = include_album_groups)
                    all_artists_albums[artist] = artist_albums_paging.items

                    while artist_albums_paging.next is not None:
                        artist_albums_paging = self.client.next(artist_albums_paging)
                        all_artists_albums[artist].extend(artist_albums_paging.items)

                log.write(message = log.ARTISTS_ALBUMS_ATTRS_FETCHED.format(len(unique_artists_list)))

            except tk.ServiceUnavailable as ex:
                message = log.API_SERVICE_UNAVAILABLE.format(ex)
                log.write(message = message)

                raise tk.ServiceUnavailable(message = message, request = ex.request, response = ex.response)

            except tk.Unauthorised as ex:
                message = log.API_SERVICE_UNAUTHORIZED.format(ex)
                log.write(message = message)

                raise tk.Unauthorised(message = message, request = ex.request, response = ex.response)

        return all_artists_albums

    def get_full_tracks(self, tracks_ids: str | set | list | pd.Series) -> tk.model.ModelList[tk.model.FullTrack]:
        """
        For each Track ID in the given collection, fetches its :class:`tk.model.FullTrack` object from the API.

        Parameters:
            tracks_ids: All the required tracks' ID's.

        Returns:
            Tekore ModelList of FullTracks, for the given tracks collection.

        Raises:
            tk.ServiceUnavailable: if Spotify's API service is unavailable.
        """
        self.validate_connection()

        unique_tracks_list = ut.get_unique_vals_list(tracks_ids)

        full_tracks = None

        with self.client.token_as(self.user_token):
            log.write(message = log.FETCHING_TRACKS_ATTRS.format(len(unique_tracks_list)))

            try:
                # Calling the API to get FullTracks for the given tracks' ID's.
                # Parameter `market` is needed here, to get the Linked Track for each track.
                # client.tracks() is chunked, meaning a single call returns *all* the desired results without paging.
                full_tracks = self.client.tracks(track_ids = unique_tracks_list,
                                                 market = self.client.current_user().country)

                log.write(message = log.TRACKS_ATTRS_FETCHED.format(len(full_tracks)))

            except tk.ServiceUnavailable as ex:
                message = log.API_SERVICE_UNAVAILABLE.format(ex)
                log.write(message = message)

                raise tk.ServiceUnavailable(message = message, request = ex.request, response = ex.response)

            except tk.Unauthorised as ex:
                message = log.API_SERVICE_UNAUTHORIZED.format(ex)
                log.write(message = message)

                raise tk.Unauthorised(message = message, request = ex.request, response = ex.response)

        return full_tracks

    def get_full_artists(self, artists_ids: str | set | list | pd.Series) -> tk.model.ModelList[tk.model.FullArtist]:
        """
        For each Artist ID in the given collection, fetches its :class:`tk.model.FullArtist` object from the API.

        Parameters:
            artists_ids: All the required artists' ID's.

        Returns:
            Tekore ModelList of FullArtists, for the given artists collection.

        Raises:
            tk.ServiceUnavailable: if Spotify's API service is unavailable.
        """
        self.validate_connection()

        unique_artists_list = ut.get_unique_vals_list(artists_ids)
        full_artists = None

        with self.client.token_as(self.user_token):
            log.write(message = log.FETCHING_ARTISTS_ATTRS.format(len(unique_artists_list)))

            try:
                # Calling the API to get FullArtists for the given artists' ID's.
                # client.artists() is chunked, meaning a single call returns *all* the desired results without paging.
                full_artists = self.client.artists(artist_ids = unique_artists_list)

                log.write(message = log.ARTISTS_ATTRS_FETCHED.format(len(full_artists)))

            except tk.ServiceUnavailable as ex:
                message = log.API_SERVICE_UNAVAILABLE.format(ex)
                log.write(message = message)

                raise tk.ServiceUnavailable(message = message, request = ex.request, response = ex.response)

            except tk.Unauthorised as ex:
                message = log.API_SERVICE_UNAUTHORIZED.format(ex)
                log.write(message = message)

                raise tk.Unauthorised(message = message, request = ex.request, response = ex.response)

        return full_artists

    def get_full_albums(self, albums_ids: str | set | list | pd.Series) -> tk.model.ModelList[tk.model.FullAlbum]:
        """
        For each Album ID in the given collection, fetches its :class:`tk.model.FullAlbum` object from the API.

        Parameters:
            albums_ids: All the required albums' ID's.

        Returns:
            Tekore ModelList of FullAlbums, for the given albums series.

        Raises:
            tk.ServiceUnavailable: if Spotify's API service is unavailable.
        """
        self.validate_connection()

        unique_albums_list = ut.get_unique_vals_list(albums_ids)
        full_albums = None

        with self.client.token_as(self.user_token):
            log.write(message = log.FETCHING_ALBUMS_ATTRS.format(len(unique_albums_list)))

            try:
                # Calling the API to get FullAlbums for the given albums' ID's.
                # Parameter `market` is not sent, in order to get the Available Markets for each album.
                # client.albums() is chunked, meaning a single call returns *all* the desired results without paging.
                full_albums = self.client.albums(album_ids = unique_albums_list)

                log.write(message = log.ALBUMS_ATTRS_FETCHED.format(len(full_albums)))

            except tk.ServiceUnavailable as ex:
                message = log.API_SERVICE_UNAVAILABLE.format(ex)
                log.write(message = message)

                raise tk.ServiceUnavailable(message = message, request = ex.request, response = ex.response)

            except tk.Unauthorised as ex:
                message = log.API_SERVICE_UNAUTHORIZED.format(ex)
                log.write(message = message)

                raise tk.Unauthorised(message = message, request = ex.request, response = ex.response)

        return full_albums

    def get_tracks_audio_features(self,
                                  tracks_ids: list[str]) -> tk.model.ModelList[tk.model.AudioFeatures]:
        """
        Returns :class:`tk.model.AudioFeatures` for the given track(s).

        Parameters:
            tracks_ids: IDs of the desired tracks to get Audio Features for.

        Returns:
            List of AudioFeatures for the requested track(s).
        """
        self.validate_connection()

        all_tracks_features = self.client.tracks_audio_features(track_ids = ut.get_unique_vals_list(tracks_ids))

        return all_tracks_features

    def get_specific_audio_feature(self,
                                   tracks_ids: list[str],
                                   audio_feature: str):
        """
        Returns a list of a desired audio feature for all given tracks. Performance-wise it is the same as getting all
        audio features, because the API doesn't allow to get only a specific one (instead, this method fetches them all
        and returns only a subset).

        Parameters:
            tracks_ids: IDs of the desired tracks to get Audio Features for.

            audio_feature: The desired Audio Feature to get (string).
                Can be one of the following values:

                'acousticness',
                'analysis_url',
                'danceability',
                'duration_ms',
                'energy',
                'instrumentalness',
                'key',
                'liveness',
                'loudness',
                'mode',
                'speechiness',
                'tempo',
                'time_signature',
                'track_href',
                'type',
                'uri',
                'valence',

        Returns:
        """
        self.validate_connection()

        tracks_audio_features = self.get_tracks_audio_features(ut.get_unique_vals_list(tracks_ids))
        result = []

        for track in tracks_audio_features:
            result.append(getattr(track.time_signature, audio_feature, None))

        return result

    def get_tracks_audio_analysis(self,
                                  tracks_ids: str | set | list | pd.Series) -> None | tk.model.AudioAnalysis | list[
        tk.model.AudioAnalysis]:
        """
        Returns :class:`tk.model.AudioAnalysis` for the given track(s).

        **Note:** This method is **very inefficient** and can **take a very long time to finish** if called on a large
        number of tracks' IDs.

        This is because the Spotify API only allows to get the audio analysis of a **single** track in a
        single API call.
        At the time of development, there is no option to batch-fetch the audio analysis of multiple tracks
        in the same call. So sadly this calls the API for each track separately.

        Parameters:
            tracks_ids: IDs of the desired track(s) to get Audio Analysis for.

        Returns:
            an AudioAnalysis object for a given single track, or a list of AudioAnalysis objects for multiple tracks,
                or None if it wasn't found.
        """
        self.validate_connection()

        if type(tracks_ids) == str:
            tracks_analysis = self.client.track_audio_analysis(tracks_ids)

        else:
            tracks_analysis = []

            for track_id in ut.get_unique_vals_list(tracks_ids):
                tracks_analysis.append(self.client.track_audio_analysis(track_id = track_id))

        return tracks_analysis

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
        tracks_known_ids_map = {}

        try:
            _full_tracks = full_tracks if full_tracks is not None else self.get_full_tracks(tracks)

            for track in _full_tracks:
                if track is not None:
                    try:
                        if track.linked_from is None:
                            tracks_known_ids_map[track.id] = track.id

                        else:
                            tracks_known_ids_map[track.linked_from.id] = track.id

                    except AttributeError:
                        tracks_known_ids_map[track.id] = track.id

        except tk.ServiceUnavailable as ex:
            log.write(log.API_SERVICE_UNAVAILABLE.format(ex))

        return tracks_known_ids_map
