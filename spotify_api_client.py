import tekore as tk


class SpotifyAPIClient:
    '''
    Spotify API Client logic. uses "Tekore" module to call Spotify API.
    '''
    MAX_TRACKS_FOR_FEATURES = 100
    MAX_TRACKS_FOR_PLAYLIST_ITEMS = 100
    MAX_TRACKS_BATCH_SIZE_FOR_RECENTLY_PLAYED = 50
    MAX_TRACKS_AMOUNT_FOR_RECENTLY_PLAYED = 1000
    AUTH_SCOPE = "user-library-read playlist-read-collaborative playlist-read-private user-read-recently-played"
    REDIRECT_URI = "http://localhost:8888/spotify/callback"

    def __init__(self,
                 token):
        self.app_token: tk.RefreshingToken() = None
        self.client = tk.Spotify(token=self.get_app_token(token),
                                 max_limits_on=True,
                                 chunked_on=True)

    # region Connection logic

    def get_app_token(self,
                      token=None) -> tk.RefreshingToken:
        if token is not None:
            cl_id = token[0].strip()
            cl_secret = token[1].strip()
            self.app_token = tk.request_client_token(cl_id,
                                                     cl_secret)

        return self.app_token

    def connect(self) -> None:
        self.client = tk.Spotify(token=self.get_app_token(),
                                 max_limits_on=True,
                                 chunked_on=True)

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

    def get_all_user_playlists(self, limit=50):
        self.validate_connection()

        response = self.client.playlists(user_id=self.client.current_user().id,
                                         limit=limit)
        results = response['items']
        # offset = limit

        while response.next is not None:
            results.extend(response.next())

        return results

    def playlist_get_all_tracks(self,
                                playlist_id: list,
                                limit=MAX_TRACKS_FOR_PLAYLIST_ITEMS):
        self.validate_connection()

        response = self.client.playlist_items(playlist_id, limit=limit)
        results = response['items']
        offset = limit
        while response['next'] is not None:
            response = self.client.playlist_items(playlist_id,
                                                  limit=limit,
                                                  offset=offset)
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
                                    types=('artist',),
                                    limit=1)

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

        albums_paging = self.client.artist_albums(artist_id=artist_id,
                                                  include_groups=[tk.model.AlbumGroup.album, ])
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

        tracks_paging = self.client.album_tracks(album_id=album_id)
        all_tracks = tracks_paging.items

        while tracks_paging.next is not None:
            tracks_paging = self.client.next(tracks_paging)
            all_tracks.extend(tracks_paging.items)

        for track in all_tracks:
            print(str(track.disc_number) + '-' + str(track.track_number) + '. ' + track.name)

        print('\n')

        return all_tracks

    # def get_all_recently_played_tracks(self,
    #                                    max_tracks_amount = MAX_TRACKS_AMOUNT_FOR_RECENTLY_PLAYED,
    #                                    batch_size = MAX_TRACKS_BATCH_SIZE_FOR_RECENTLY_PLAYED):
    #     """
    #     Returns a large number of the user's recently played tracks.
    #     :param max_tracks_amount:
    #     :param batch_size:
    #     :return:
    #     """
    #     self.validate_connection()
    #     response = self.client.current_user_recently_played(limit = batch_size)
    #
    #     results = response['items']
    #     next_batch_before_timestamp = response['cursors']['before']
    #
    #     while  (response['next'] is not None) \
    #         or (len(results) <= max_tracks_amount):
    #         response = self.client.current_user_recently_played(limit = batch_size,
    #                                                             before = next_batch_before_timestamp)
    #
    #         results.extend(response['items'])
    #         next_batch_before_timestamp = response['cursors']['before']
    #
    #     return results

    def get_tracks_audio_features(self,
                                  tracks_ids: list):
        '''
        :param tracks_ids: IDs of the desired tracks to get Audio Features of.
        :param limit:
        :return: List of
        '''
        self.validate_connection()

        all_features = self.client.tracks_audio_features(track_ids=tracks_ids)

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

    def create_tracks_data_frame(self, tracks_items: list,
                                 audio_features_names: list,
                                 limit=MAX_TRACKS_FOR_FEATURES):
        self.validate_connection()
        tracks_with_features = {'track_name': get_tracks_names(tracks_items)}

        test_artists_list = get_tracks_artists(tracks_items)

        # tracks_with_features['artist'] =

        for feat_name in audio_features_names:
            # Inserting a new column, titled feat_name, containing a list of all the audio features for all the tracks:
            tracks_with_features[feat_name] = self.get_specific_audio_feature(tracks_items,
                                                                              audio_feature=feat_name,
                                                                              limit=limit)

        return pd.DataFrame(data=tracks_with_features)
