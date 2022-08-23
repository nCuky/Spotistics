import tekore as tk


class SpotifyAPIClient:
    '''
    Spotify API Client logic. uses "Tekore" module to call Spotify API.
    '''
    MAX_TRACKS_FOR_FEATURES = 100
    MAX_TRACKS_FOR_PLAYLIST_ITEMS = 100
    AUTH_SCOPE = "user-library-read playlist-read-collaborative playlist-read-private user-read-recently-played"
    REDIRECT_URI = "http://localhost:8888/spotify/callback"

    client = tk.Spotify

    def __init__(self,
                 token):
        self.connect(token=token)


    def connect(self,
                token):

        cl_id = token[0].strip()
        cl_secret = token[1].strip()

        # app_token = tk.prompt_for_user_token(*conf,
        #                                  scope = auth_scope)  # scope=tk.scope.every)
        app_token = tk.request_client_token(cl_id,
                                            cl_secret)

        self.client = tk.Spotify(app_token)

    def disconnect(self):
        self.client.close()
        self.client = None

    def is_connected(self):
        if self.client is None:
            return False

        return True

    def validate_connection(self):
        if not self.is_connected():
            raise ValueError("Client is not connected to Spotify API")

    def get_all_user_playlists(self, limit=50):
        self.validate_connection()

        response = self.client.playlists(user_id = self.client.current_user().id,
                                         limit = limit)
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

    def album_get_all_tracks(self,
                             album_id: list,
                             limit=MAX_TRACKS_FOR_PLAYLIST_ITEMS):
        '''
        Returns all Tracks in a given Album.
        :param album_id: ID of the desired Album.
        :param limit:
        :return: All Tracks in the desired Album (list)
        '''
        self.validate_connection()

        response = list

        client1 = tek.Spotify(self.to)
        track_paging = client1.album_tracks(album_id)

        response = client1.all_items(track_paging).items

        result = self.client.playlist_items(album_id, limit=limit)
        tracks = result['items']

        while result['items']:
            result = sp.Spotify.next(result)
            tracks.extend(result['items'])

        return tracks

    def get_tracks_audio_features(self,
                                  tracks_ids: list,
                                  limit=MAX_TRACKS_FOR_FEATURES):
        '''
        :param tracks_ids: IDs of the desired tracks to get Audio Features of.
        :param limit:
        :return: List of
        '''
        self.validate_connection()

        result = self.client.audio_features(tracks_ids,
                                            limit=limit)
        features = result['items']
        offset = limit

        while result['next'] is not None:
            result = self.client.audio_features(tracks_ids,
                                                limit=limit,
                                                offset=offset)
            features.extend(result['items'])
            offset += limit

        # while result['items']:
        #     result = sp.Spotify.next(result)
        #     features.extend(result['items'])

        # while len(tracks_ids) > 0:
        #     result.extend(self.client.audio_features(tracks_ids[0:limit]))
        #
        #     # Removing the first {limit} items from the list:
        #     tracks_ids = tracks_ids[limit:]

        return features


    def get_specific_audio_feature(self,
                                   tracks_items: list,
                                   audio_feature: str,
                                   limit=MAX_TRACKS_FOR_FEATURES):
        '''
        Returns a list of a desired audio feature for all given tracks.
        :param tracks_items:
        :param audio_feature:
        :param limit:
        :return:
        '''
        self.validate_connection()
        tracks_audio_features = self.get_tracks_audio_features(tracks_items,
                                                               limit)
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

        result = list()

        for track_id in tracks_ids:
            result.append(self.client.audio_analysis(track_id))

        return result

    def find_artist(self,
                    name: str):
        '''
        :param artist_name: Name of the desired Artist to find
        '''
        self.validate_connection()

        result = self.client.search(name,
                                    types=('artist',),
                                    limit=1)

        return result[0].items[0]

    def artist_get_all_tracks(self,
                              artist_id):
        '''
        Returns a list of all the tracks by the given Artist.
        :param artist_id: ID of the desired Artist
        '''
        self.validate_connection()

        tracks = list

        for album in self.artist_get_all_albums(artist_id)['items']:
            tracks.extend(self.album_get_all_tracks(album))

        return tracks

    def artist_get_all_albums(self,
                              artist_id):
        self.validate_connection()

        result = self.client.artist_albums(artist_id = artist_id)
        albums = result.items

        while result.next is not None:
            result = tk.Spotify.next(result.next)
            albums.extend(result.items)

        return albums

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
