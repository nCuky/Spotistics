import pandas as pd
import tekore as tk
from dataclasses import dataclass
import log


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

    def get_full_tracks(self, tracks: pd.Series) -> tk.model.ModelList[tk.model.FullTrack]:
        """
        For each track in the given series, fetches its :class:`tk.model.FullTrack` object from the API.

        Parameters:
            tracks: All the required tracks' ID's.

        Returns:
            Tekore ModelList of FullTracks, for the given tracks series.

        Raises:
            tk.ServiceUnavailable: if Spotify's API service is unavailable.
        """
        unique_tracks = tracks.dropna().unique()
        unique_tracks_list = unique_tracks.tolist()
        full_tracks = None

        with self.client.token_as(self.user_token):
            log.write(message = log.FETCHING_TRACKS_ATTRS.format(unique_tracks.size))

            try:
                # Calling the API to get FullTracks for the given tracks' ID's:
                full_tracks = self.client.tracks(track_ids = unique_tracks_list,
                                                 market = self.client.current_user().country)

                log.write(message = log.TRACKS_ATTRS_FETCHED.format(len(full_tracks)))

            except tk.ServiceUnavailable as ex:
                message = log.API_SERVICE_UNAVAILABLE.format(ex)
                log.write(message = message)

                raise tk.ServiceUnavailable(message = message)

        return full_tracks

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
