import sqlite3
import sys
from dataclasses import dataclass
from typing import Optional, Union

import pandas as pd
import tekore as tk
from deprecation import deprecated

import log
from spotify_data_set import ColNames as spdcol


class DB:
    """
    Manages the local DB for saving Spotify data for further calculations.
    """

    # region DB Names enums

    @dataclass(frozen = True)
    class TRACKS:
        TBL_NAME = 'tracks'

        ID = 'track_id'
        HREF = 'href'
        URI = 'uri'
        DISC_NUMBER = 'disc_number'
        DURATION_MS = 'duration_ms'
        EXPLICIT = 'explicit'
        NAME = 'name'
        PREVIEW_URL = 'preview_url'
        TRACK_NUMBER = 'track_number'
        IS_LOCAL = 'is_local'
        POPULARITY = 'popularity'
        IS_PLAYABLE = 'is_playable'

    @dataclass(frozen = True)
    class ALBUMS:
        TBL_NAME = 'albums'

        ID = 'album_id'
        HREF = 'href'
        URI = 'uri'
        NAME = 'name'
        ALBUM_TYPE = 'album_type'
        TOTAL_TRACKS = 'total_tracks'
        RELEASE_DATE = 'release_date'
        RELEASE_DATE_PRECISION = 'release_date_precision'

    @dataclass(frozen = True)
    class ARTISTS:
        TBL_NAME = 'artists'

        ID = 'artist_id'
        HREF = 'href'
        URI = 'uri'
        NAME = 'name'
        TOTAL_FOLLOWERS = 'total_followers'
        POPULARITY = 'popularity'

    @dataclass(frozen = True)
    class ARTISTS_ALBUMS:
        TBL_NAME = 'artists_albums'
        ARTIST_ID = 'artist_id'
        ALBUM_ID = 'album_id'

    @dataclass(frozen = True)
    class ALBUMS_TRACKS:
        TBL_NAME = 'albums_tracks'

        ALBUM_ID = 'album_id'
        TRACK_ID = 'track_id'

    @dataclass(frozen = True)
    class GENRES:
        TBL_NAME = 'genres'

        ID = 'genre_id'
        NAME = 'name'

    @dataclass(frozen = True)
    class ARTISTS_GENRES:
        TBL_NAME = 'artists_genres'

        ARTIST_ID = 'artist_id'
        GENRE_ID = 'genre_id'

    @dataclass(frozen = True)
    class TRACKS_LINKED_FROM:
        TBL_NAME = 'linked_tracks'

        FROM_ID = 'linked_from_id'
        RELINKED_ID = 'track_known_id'

    @dataclass(frozen = True)
    class TRACKS_LISTEN_HISTORY:
        TBL_NAME = 'tracks_listen_history'

        TIMESTAMP = 'timestamp'
        USERNAME = 'username'
        TRACK_ID = 'track_id'
        PLATFORM = 'platform'
        MS_PLAYED = 'ms_played'
        CONN_COUNTRY = 'conn_country'
        URI = 'uri'
        REASON_START = 'reason_start'
        REASON_END = 'reason_end'
        SHUFFLE = 'shuffle'
        OFFLINE = 'offline'
        INCOGNITO_MODE = 'incognito_mode'
        SKIPPED = 'skipped'

    # endregion DB Names enums

    @staticmethod
    def eprint(*args, **kwargs):
        """Print to stderr."""
        print(*args, file = sys.stderr, **kwargs)
        log.write(*args)

    # region Insertion Utilities

    @staticmethod
    def get_track_for_insert(track: tk.model.Track | tk.model.FullTrack | None = None) -> dict | None:
        """
        Turn a track into a structure that can be inserted into the **Tracks** DB-table.

        :param track: A Track (or FullTrack) object from which to take the track values.
        :return: Dictionary with the track's values."""
        values_out = None

        if track is not None:
            values_out = {DB.TRACKS.ID          : track.id,
                          DB.TRACKS.HREF        : track.href,
                          DB.TRACKS.URI         : track.uri,
                          DB.TRACKS.DISC_NUMBER : track.disc_number,
                          DB.TRACKS.DURATION_MS : track.duration_ms,
                          DB.TRACKS.EXPLICIT    : track.explicit,
                          DB.TRACKS.NAME        : track.name,
                          DB.TRACKS.PREVIEW_URL : str(track.preview_url),
                          DB.TRACKS.TRACK_NUMBER: track.track_number,
                          DB.TRACKS.IS_LOCAL    : track.is_local,
                          DB.TRACKS.POPULARITY  : track.popularity,
                          DB.TRACKS.IS_PLAYABLE : track.is_playable}

        return values_out

    @staticmethod
    def get_album_for_insert(track: tk.model.Track | tk.model.FullTrack | None = None) -> dict | None:
        """
        Turn a track's Album into a structure that can be inserted into the **Albums** DB-table.

        :param track: A Track (or FullTrack) object from which to take the album values.
        :return: Dictionary with the album's values.
        """
        album = track.album
        values_out = {DB.ALBUMS.ID                    : album.id,
                      DB.ALBUMS.HREF                  : album.href,
                      DB.ALBUMS.URI                   : album.uri,
                      DB.ALBUMS.NAME                  : album.name,
                      DB.ALBUMS.ALBUM_TYPE            : album.album_type,
                      DB.ALBUMS.TOTAL_TRACKS          : album.total_tracks,
                      DB.ALBUMS.RELEASE_DATE          : album.release_date,
                      DB.ALBUMS.RELEASE_DATE_PRECISION: str(album.release_date_precision)}
        # track.id)

        return values_out

    @staticmethod
    def get_linked_from_for_insert(track: tk.model.Track | tk.model.FullTrack | None = None) -> dict | None:
        """
        Turn a track's LinkedFrom value into a structure that can be inserted into the **TracksLinkedFrom** DB-table.

        :param track: A Track (or FullTrack) object from which to take the Linked Track values.
        :return: Dictionary with the Linked Track's values.
        """
        linked = track.linked_from

        if linked is None:
            values_out = {DB.TRACKS_LINKED_FROM.FROM_ID    : track.id,
                          DB.TRACKS_LINKED_FROM.RELINKED_ID: track.id}

        else:
            values_out = {DB.TRACKS_LINKED_FROM.FROM_ID    : linked.id,
                          DB.TRACKS_LINKED_FROM.RELINKED_ID: track.id}

        return values_out

    # @staticmethod
    # def get_albums_of_artists_for_insert(
    #     track: tk.model.Track | tk.model.FullTrack | None = None) -> list[dict] | None:
    #     """
    #     Turn a track's Artists' Albums into a structure that can be inserted into the **AlbumsOfArtists** DB-table.
    #
    #     :param track: A Track (or FullTrack) object from which to take the Artists' Albums.
    #     :return: List of dictionaries with each AlbumOfArtist.
    #     """
    #     artists = track.album.artists
    #     values_out = [None] * len(artists)
    #
    #     for (i, artist) in enumerate(artists):
    #         values_out[i] = (artist.id,
    #                          track.album.id)
    #
    #     return values_out

    # @staticmethod
    # @deprecated(details = "Logic moved to method save_full_tracks_to_db in class Logic.")
    # def get_artists_album_for_insert(track: dict | tk.model.Track | tk.model.FullTrack | None = None) -> list | None:
    #     """Turn a track into a tuple list insertable into the artists_album table."""
    #     match type(track):
    #         case dict():
    #             artists = track['album']['artists']
    #             values_out = [None] * len(artists)
    #
    #             for i, artist in enumerate(artists):
    #                 values_out[i] = (artist['id'],
    #                                  # artist['href'],
    #                                  # artist['uri'],
    #                                  # artist['name'],
    #                                  track['album']['id'])
    #
    #         case tk.model.Track | tk.model.FullTrack:
    #             artists = track.album.artists
    #             values_out = [None] * len(artists)
    #
    #             for (i, artist) in enumerate(artists):
    #                 values_out[i] = (artist.id,
    #                                  # artist.href,
    #                                  # artist.uri,
    #                                  # artist.name,
    #                                  track.album.id)
    #
    #         case _:
    #             values_out = None
    #
    #     return values_out

    # @staticmethod
    # @deprecated(details = """Please use ``get_tracks_of_albums_for_insert``.""")
    # def get_artists_track_for_insert(track: dict | tk.model.Track | tk.model.FullTrack | None = None) -> list | None:
    #     """Turn a track into a tuple list insertable into the artists_track table."""
    #     match type(track):
    #         case dict():
    #             artists = track['artists']
    #             values_out = [None] * len(artists)
    #
    #             for i, artist in enumerate(artists):
    #                 values_out[i] = (artist['id'],
    #                                  artist['href'],
    #                                  artist['uri'],
    #                                  artist['name'],
    #                                  track['id'])
    #
    #         case tk.model.Track | tk.model.FullTrack:
    #             artists = track.artists
    #             values_out = [None] * len(artists)
    #
    #             for i, artist in enumerate(artists):
    #                 values_out[i] = (artist.id,
    #                                  artist.href,
    #                                  artist.uri,
    #                                  artist.name,
    #                                  track.id)
    #
    #         case _:
    #             values_out = None
    #
    #     return values_out

    @staticmethod
    def get_listen_history_df_for_insert(listen_history_df: pd.DataFrame) -> pd.DataFrame:
        """Returns a DataFrame ready for insertion to the DB.

        :param listen_history_df: DataFrame to insert.
        :return: DataFrame ready for insertion."""
        df_to_insert = listen_history_df[[spdcol.TIMESTAMP, spdcol.USERNAME, spdcol.TRACK_ID, spdcol.PLATFORM,
                                          spdcol.MS_PLAYED, spdcol.CONN_COUNTRY, spdcol.TRACK_URI,
                                          spdcol.REASON_START, spdcol.REASON_END, spdcol.SHUFFLE, spdcol.OFFLINE,
                                          spdcol.INCOGNITO, spdcol.SKIPPED]].fillna(value = {spdcol.SKIPPED: ''},
                                                                                    inplace = False)

        df_to_insert = df_to_insert.rename(columns = {spdcol.TIMESTAMP: DB.TRACKS_LISTEN_HISTORY.TIMESTAMP,
                                                      spdcol.USERNAME : DB.TRACKS_LISTEN_HISTORY.USERNAME,
                                                      spdcol.TRACK_ID : DB.TRACKS_LISTEN_HISTORY.TRACK_ID,
                                                      spdcol.TRACK_URI: DB.TRACKS_LISTEN_HISTORY.URI})

        df_to_insert = df_to_insert.drop_duplicates(
            subset = [DB.TRACKS_LISTEN_HISTORY.TIMESTAMP,
                      DB.TRACKS_LISTEN_HISTORY.USERNAME,
                      DB.TRACKS_LISTEN_HISTORY.TRACK_ID])

        return df_to_insert

    # endregion Insertion Utilities

    # region Instantiation logic

    def __init__(self):
        """
        Initializes the DB Manager for working with the DB.
        """
        # Connect to DB
        self._db_filename_ = "my_spotify_data.db"
        self.connection = sqlite3.connect(self._db_filename_)
        self.cursor = self.connection.cursor()

    def commit(self) -> None:
        """Commits all changes to the DB."""
        self.connection.commit()

    def close(self):
        """Closes the connection and the cursor to the DB."""
        self.connection.close()
        self.cursor.close()

    # endregion Instantiation logic

    # region Insertion Logic

    # region Multiple records

    def insert_tracks(self, tracks_values: list[dict]) -> None:
        """
        Insert multiple Tracks' values to the **Tracks** DB-table.
        Does not commit.

        :param tracks_values: List of Dictionaries containing the desired Tracks' values to insert.
        :return: None.
        """
        if tracks_values is None:
            log.write("WARNING: " + log.EMPTY_VALUES.format('Tracks'))

        else:
            try:
                query = f"""INSERT OR REPLACE INTO {DB.TRACKS.TBL_NAME}
                ({DB.TRACKS.ID},
                {DB.TRACKS.HREF},
                {DB.TRACKS.URI},
                {DB.TRACKS.DISC_NUMBER},
                {DB.TRACKS.DURATION_MS},
                {DB.TRACKS.EXPLICIT},
                {DB.TRACKS.NAME},
                {DB.TRACKS.PREVIEW_URL},
                {DB.TRACKS.TRACK_NUMBER},
                {DB.TRACKS.IS_LOCAL},
                {DB.TRACKS.POPULARITY},
                {DB.TRACKS.IS_PLAYABLE})

                VALUES (:{DB.TRACKS.ID},
                :{DB.TRACKS.HREF},
                :{DB.TRACKS.URI},
                :{DB.TRACKS.DISC_NUMBER},
                :{DB.TRACKS.DURATION_MS},
                :{DB.TRACKS.EXPLICIT},
                :{DB.TRACKS.NAME},
                :{DB.TRACKS.PREVIEW_URL},
                :{DB.TRACKS.TRACK_NUMBER},
                :{DB.TRACKS.IS_LOCAL},
                :{DB.TRACKS.POPULARITY},
                :{DB.TRACKS.IS_PLAYABLE});"""

                self.cursor.executemany(query, tracks_values)

            except sqlite3.IntegrityError as e:
                # DB.eprint(log.CANNOT_INSERT.format(str(track_values)))
                DB.eprint(f"sqlite3.IntegrityError: {e}")

            except sqlite3.OperationalError as e:
                # DB.eprint(log.CANNOT_INSERT.format(str(track_values)))
                DB.eprint(f"sqlite3.OperationalError: {e}")

    def __insert_listen_history_df(self, listen_history_df: pd.DataFrame) -> None:
        """
        Insert values from a prepared Listen History DataFrame to DB.

        Does not commit.

        :param listen_history_df: DataFrame with the prepared Listen History data.
        :return: None.
        """
        if listen_history_df is None:
            log.write("WARNING: " + log.EMPTY_VALUES.format('Listen History'))

        else:
            for listened_track in listen_history_df.itertuples():
                track_str = (listened_track.pk_timestamp,
                             listened_track.pk_username,
                             listened_track.pk_track_id,
                             listened_track.platform,
                             listened_track.ms_played,
                             listened_track.conn_country,
                             listened_track.uri,
                             listened_track.reason_start,
                             listened_track.reason_end,
                             listened_track.shuffle,
                             listened_track.offline,
                             listened_track.incognito_mode,
                             str(listened_track.skipped))

                try:
                    query = f"""INSERT OR REPLACE INTO {DB.TRACKS_LISTEN_HISTORY.TBL_NAME} 
                    ({DB.TRACKS_LISTEN_HISTORY.TIMESTAMP},
                    {DB.TRACKS_LISTEN_HISTORY.USERNAME},
                    {DB.TRACKS_LISTEN_HISTORY.TRACK_ID},
                    {DB.TRACKS_LISTEN_HISTORY.PLATFORM},
                    {DB.TRACKS_LISTEN_HISTORY.MS_PLAYED},
                    {DB.TRACKS_LISTEN_HISTORY.CONN_COUNTRY},
                    {DB.TRACKS_LISTEN_HISTORY.URI},
                    {DB.TRACKS_LISTEN_HISTORY.REASON_START},
                    {DB.TRACKS_LISTEN_HISTORY.REASON_END},
                    {DB.TRACKS_LISTEN_HISTORY.SHUFFLE},
                    {DB.TRACKS_LISTEN_HISTORY.OFFLINE},
                    {DB.TRACKS_LISTEN_HISTORY.INCOGNITO_MODE},
                    {DB.TRACKS_LISTEN_HISTORY.SKIPPED})

                    VALUES (:{DB.TRACKS_LISTEN_HISTORY.TIMESTAMP},
                    :{DB.TRACKS_LISTEN_HISTORY.USERNAME},
                    :{DB.TRACKS_LISTEN_HISTORY.TRACK_ID},
                    :{DB.TRACKS_LISTEN_HISTORY.PLATFORM},
                    :{DB.TRACKS_LISTEN_HISTORY.MS_PLAYED},
                    :{DB.TRACKS_LISTEN_HISTORY.CONN_COUNTRY},
                    :{DB.TRACKS_LISTEN_HISTORY.URI},
                    :{DB.TRACKS_LISTEN_HISTORY.REASON_START},
                    :{DB.TRACKS_LISTEN_HISTORY.REASON_END},
                    :{DB.TRACKS_LISTEN_HISTORY.SHUFFLE},
                    :{DB.TRACKS_LISTEN_HISTORY.OFFLINE},
                    :{DB.TRACKS_LISTEN_HISTORY.INCOGNITO_MODE},
                    :{DB.TRACKS_LISTEN_HISTORY.SKIPPED});"""

                    self.cursor.execute(query, track_str)

                except sqlite3.IntegrityError as e:
                    DB.eprint(log.CANNOT_INSERT.format(track_str))
                    DB.eprint(f"sqlite3.IntegrityError: {e}")

                except sqlite3.OperationalError as e:
                    DB.eprint(log.CANNOT_INSERT.format(track_str))
                    DB.eprint(f"sqlite3.OperationalError: {e}")

    def insert_listen_history(self, df: pd.DataFrame) -> None:
        """Insert Listen History table to the DB.
        Cleans data and removes duplicate rows before inserting.

        :param df: DataFrame with Listen History records for inserting.
        """
        df_to_insert = DB.get_listen_history_df_for_insert(df)

        # df_to_insert.to_sql(DB.TABLES.TRACKS_LISTEN_HISTORY, con = self.connection, if_exists = "append",
        # index = False)b

        self.__insert_listen_history_df(df_to_insert)

    # endregion Multiple records

    # region Single record

    def insert_track_full(self, track: Optional[Union[dict, tk.model.FullTrack]] = None) -> None:
        """Insert full track record for all tables. Don't commit."""
        self.insert_track(DB.get_track_for_insert(track))
        self.insert_album(DB.get_album_for_insert(track))
        # self.insert_artists_album(DB.get_artists_album_for_insert(track))
        # self.insert_artists_track(DB.get_artists_track_for_insert(track))
        self.insert_linked_from(DB.get_linked_from_for_insert(track))

    def insert_track(self, track_values: dict) -> None:
        """
        Insert a Track's values to the **Tracks** DB-table.
        Does not commit.

        :param track_values: Dictionary with the desired Track's values to insert.
        :return: None.
        """
        if track_values is None:
            log.write("WARNING: " + log.EMPTY_VALUES.format('Track'))

        else:
            try:
                query = f"""INSERT OR REPLACE INTO {DB.TRACKS.TBL_NAME}
                ({DB.TRACKS.ID},
                {DB.TRACKS.HREF},
                {DB.TRACKS.URI},
                {DB.TRACKS.DISC_NUMBER},
                {DB.TRACKS.DURATION_MS},
                {DB.TRACKS.EXPLICIT},
                {DB.TRACKS.NAME},
                {DB.TRACKS.PREVIEW_URL},
                {DB.TRACKS.TRACK_NUMBER},
                {DB.TRACKS.IS_LOCAL},
                {DB.TRACKS.POPULARITY},
                {DB.TRACKS.IS_PLAYABLE})
                
                VALUES (:{DB.TRACKS.ID},
                :{DB.TRACKS.HREF},
                :{DB.TRACKS.URI},
                :{DB.TRACKS.DISC_NUMBER},
                :{DB.TRACKS.DURATION_MS},
                :{DB.TRACKS.EXPLICIT},
                :{DB.TRACKS.NAME},
                :{DB.TRACKS.PREVIEW_URL},
                :{DB.TRACKS.TRACK_NUMBER},
                :{DB.TRACKS.IS_LOCAL},
                :{DB.TRACKS.POPULARITY},
                :{DB.TRACKS.IS_PLAYABLE});"""

                self.cursor.execute(query, track_values)

            except sqlite3.IntegrityError as e:
                DB.eprint(log.CANNOT_INSERT.format(str(track_values)))
                DB.eprint(f"sqlite3.IntegrityError: {e}")

            except sqlite3.OperationalError as e:
                DB.eprint(log.CANNOT_INSERT.format(str(track_values)))
                DB.eprint(f"sqlite3.OperationalError: {e}")

    def insert_artist(self, artist_values: dict) -> None:
        """Insert Artist's values to DB. Does not commit."""
        if artist_values is None:
            log.write("WARNING: " + log.EMPTY_VALUES.format('Artist'))

        else:
            try:
                query = f"""INSERT OR REPLACE INTO {DB.ARTISTS.TBL_NAME} 
                ({DB.ARTISTS.ID},
                {DB.ARTISTS.HREF},
                {DB.ARTISTS.URI},
                {DB.ARTISTS.NAME},
                {DB.ARTISTS.TOTAL_FOLLOWERS},
                {DB.ARTISTS.POPULARITY})
                
                VALUES (:{DB.ARTISTS.ID},
                :{DB.ARTISTS.HREF},
                :{DB.ARTISTS.URI},
                :{DB.ARTISTS.NAME},
                :{DB.ARTISTS.TOTAL_FOLLOWERS},
                :{DB.ARTISTS.POPULARITY});"""

                self.cursor.execute(query, artist_values)

            except sqlite3.IntegrityError as e:
                DB.eprint(log.CANNOT_INSERT.format(str(artist_values)))
                DB.eprint(f"sqlite3.IntegrityError: {e}")

            except sqlite3.OperationalError as e:
                DB.eprint(log.CANNOT_INSERT.format(str(artist_values)))
                DB.eprint(f"sqlite3.OperationalError: {e}")

    def insert_album(self, album_values: dict) -> None:
        """Insert album values to DB. Does not commit."""
        if album_values is None:
            log.write("WARNING: " + log.EMPTY_VALUES.format('Album'))

        else:
            try:
                query = f"""INSERT OR REPLACE INTO {DB.ALBUMS.TBL_NAME} 
                ({DB.ALBUMS.ID},
                {DB.ALBUMS.HREF},
                {DB.ALBUMS.URI},
                {DB.ALBUMS.NAME},
                {DB.ALBUMS.ALBUM_TYPE},
                {DB.ALBUMS.TOTAL_TRACKS},
                {DB.ALBUMS.RELEASE_DATE},
                {DB.ALBUMS.RELEASE_DATE_PRECISION})
                
                VALUES (:{DB.ALBUMS.ID},
                :{DB.ALBUMS.HREF},
                :{DB.ALBUMS.URI},
                :{DB.ALBUMS.NAME},
                :{DB.ALBUMS.ALBUM_TYPE},
                :{DB.ALBUMS.TOTAL_TRACKS},
                :{DB.ALBUMS.RELEASE_DATE},
                :{DB.ALBUMS.RELEASE_DATE_PRECISION});"""

                self.cursor.execute(query, album_values)

            except sqlite3.IntegrityError as e:
                DB.eprint(log.CANNOT_INSERT.format(str(album_values)))
                DB.eprint(f"sqlite3.IntegrityError: {e}")

            except sqlite3.OperationalError as e:
                DB.eprint(log.CANNOT_INSERT.format(str(album_values)))
                DB.eprint(f"sqlite3.OperationalError: {e}")

    def insert_linked_from(self, linked_track_values: dict) -> None:
        """
        Insert ``linked_from`` values to DB.

        Does not commit.

        :param linked_track_values: Tuple of a Linked Track, with the original track's ID ('linked_from')
                                    and its Relinked Track ID.
        :return: None.
        """
        if linked_track_values is None:
            log.write("WARNING: " + log.EMPTY_VALUES.format('Linked Track'))

        else:
            try:
                # if len(linked_track_values) == 2:
                query = f"""INSERT OR REPLACE INTO {DB.TRACKS_LINKED_FROM.TBL_NAME} 
                ({DB.TRACKS_LINKED_FROM.FROM_ID},
                {DB.TRACKS_LINKED_FROM.RELINKED_ID})
                
                VALUES (:{DB.TRACKS_LINKED_FROM.FROM_ID},
                :{DB.TRACKS_LINKED_FROM.RELINKED_ID})"""

                self.cursor.execute(query, linked_track_values)

                # elif len(linked_track_values) == 4:
                #     self.cursor.execute(f"""INSERT OR REPLACE INTO {DB.TRACKS_LINKED_FROM.TBL_NAME}
                #     ({DB.TRACKS_LINKED_FROM.FROM_ID},
                #     {DB.TRACKS_LINKED_FROM.RELINKED_ID})
                #     VALUES (?, ?)""", __parameters = linked_track_values)

                # else:
                #     DB.eprint("WARNING: Wrong number of values to insert. Should be either 2 or 4. Skipping")

            except sqlite3.IntegrityError as e:
                DB.eprint(log.CANNOT_INSERT.format(str(linked_track_values)))
                DB.eprint(f"sqlite3.IntegrityError: {e}")

            except sqlite3.OperationalError as e:
                DB.eprint(log.CANNOT_INSERT.format(str(linked_track_values)))
                DB.eprint(f"sqlite3.OperationalError: {e}")

    def insert_albums_of_artists(self, artists_albums_values: list) -> None:
        """Insert artist values to DB. Does not commit."""
        if artists_albums_values is None:
            log.write("WARNING: " + log.EMPTY_VALUES.format('Artist Albums'))

        else:
            for artist_album in artists_albums_values:
                try:
                    query = f"""INSERT OR REPLACE INTO {DB.ARTISTS_ALBUMS.TBL_NAME} 
                    ({DB.ARTISTS_ALBUMS.ARTIST_ID},
                    {DB.ARTISTS_ALBUMS.ALBUM_ID})
                    
                    VALUES (:{DB.ARTISTS_ALBUMS.ARTIST_ID},
                    :{DB.ARTISTS_ALBUMS.ALBUM_ID})"""

                    self.cursor.execute(query, artist_album)

                except sqlite3.IntegrityError as e:
                    DB.eprint(log.CANNOT_INSERT.format(str(artist_album)))
                    DB.eprint(f"sqlite3.IntegrityError: {e}")

                except sqlite3.OperationalError as e:
                    DB.eprint(log.CANNOT_INSERT.format(str(artist_album)))
                    DB.eprint(f"sqlite3.OperationalError: {e}")

    def insert_tracks_of_albums(self, albums_tracks_values: list) -> None:
        """Insert artist values to DB. Does not commit."""
        if albums_tracks_values is None:
            log.write("WARNING: " + log.EMPTY_VALUES.format('Artist Tracks'))

        else:
            for album_track in albums_tracks_values:
                try:
                    query = f"""INSERT OR REPLACE INTO {DB.ALBUMS_TRACKS.TBL_NAME} 
                    ({DB.ALBUMS_TRACKS.ALBUM_ID},
                    {DB.ALBUMS_TRACKS.TRACK_ID})
                    
                    VALUES (:{DB.ALBUMS_TRACKS.ALBUM_ID},
                    :{DB.ALBUMS_TRACKS.TRACK_ID})"""

                    self.cursor.execute(query, album_track)

                except sqlite3.IntegrityError as e:
                    DB.eprint(log.CANNOT_INSERT.format(str(album_track)))
                    DB.eprint(f"sqlite3.IntegrityError: {e}")

                except sqlite3.OperationalError as e:
                    DB.eprint(log.CANNOT_INSERT.format(str(album_track)))
                    DB.eprint(f"sqlite3.OperationalError: {e}")

    @deprecated(details = """Tables were split into Model tables and Model-to-Model linkage tables.
    Please use method insert_albums_of_artists.""")
    def insert_artists_album(self, artists_albums_values: list) -> None:
        """Insert artist values to DB. Does not commit.
        Tables were split into Model tables and Model-to-Model linkage tables.
        Please use method ``insert_albums_of_artists``."""
        if artists_albums_values is None:
            log.write("WARNING: " + log.EMPTY_VALUES.format('Artist Albums'))

        else:
            for artist_album in artists_albums_values:
                try:
                    self.cursor.execute(f"""INSERT OR REPLACE INTO artists_albums (pk_id,
                    href,
                    uri,
                    name,
                    fk_album_id)
                    VALUES {str(artist_album)}""")

                except sqlite3.IntegrityError as e:
                    DB.eprint(f"ERROR: Could not insert the following: {str(artist_album)}")
                    DB.eprint(f"sqlite3.IntegrityError: {e}")

                except sqlite3.OperationalError as e:
                    DB.eprint(f"ERROR: Could not insert the following: {str(artist_album)}")
                    DB.eprint(f"sqlite3.OperationalError: {e}")

    @deprecated(details = """Tables were split into Model tables and Model-to-Model linkage tables.
        Please use method insert_tracks_of_artists.""")
    def insert_artists_track(self, artists_tracks_values: list) -> None:
        """Insert artist values to DB. Does not commit.
        Tables were split into Model tables and Model-to-Model linkage tables.
        Please use method ``insert_tracks_of_artists``."""
        if artists_tracks_values is None:
            log.write("WARNING: " + log.EMPTY_VALUES.format('Artist Tracks'))

        else:
            for artist_track in artists_tracks_values:
                try:
                    self.cursor.execute(f"""INSERT OR REPLACE INTO artists_tracks (pk_id,
                    href,
                    uri,
                    name,
                    fk_track_id)
                    VALUES {str(artist_track)}""")

                except sqlite3.IntegrityError as e:
                    DB.eprint(f"ERROR: Could not insert the following: {str(artist_track)}")
                    DB.eprint(f"sqlite3.IntegrityError: {e}")

                except sqlite3.OperationalError as e:
                    DB.eprint(f"ERROR: Could not insert the following: {str(artist_track)}")
                    DB.eprint(f"sqlite3.OperationalError: {e}")

    # endregion Single record

    # endregion Insertion Logic
