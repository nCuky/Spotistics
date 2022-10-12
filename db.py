import sqlite3
import sys
import pandas as pd
import tekore as tk
from deprecation import deprecated

import log
from names import Spdt as spdtnm
from names import Spdb as spdbnm


class DB:
    """
    Manages the local DB for saving Spotify data for further calculations.
    """

    @staticmethod
    def eprint(*args, **kwargs):
        """Print to stderr."""
        print(*args, file = sys.stderr, **kwargs)
        log.write(*args)

    # region Insertion Utilities

    @staticmethod
    def ___get_track_for_insert(track: tk.model.Track | tk.model.FullTrack | None = None) -> dict | None:
        """
        Turn a track into a structure that can be inserted into the **Tracks** DB-table.

        :param track: A Track (or FullTrack) object from which to take the track values.
        :return: Dictionary with the track's values."""
        values_out = None

        if track is not None:
            values_out = {spdbnm.TRACKS.ID          : track.id,
                          spdbnm.TRACKS.HREF        : track.href,
                          spdbnm.TRACKS.URI         : track.uri,
                          spdbnm.TRACKS.DISC_NUMBER : track.disc_number,
                          spdbnm.TRACKS.DURATION_MS : track.duration_ms,
                          spdbnm.TRACKS.EXPLICIT    : track.explicit,
                          spdbnm.TRACKS.NAME        : track.name,
                          spdbnm.TRACKS.PREVIEW_URL : str(track.preview_url),
                          spdbnm.TRACKS.TRACK_NUMBER: track.track_number,
                          spdbnm.TRACKS.IS_LOCAL    : track.is_local,
                          spdbnm.TRACKS.POPULARITY  : track.popularity,
                          spdbnm.TRACKS.IS_PLAYABLE : track.is_playable}

        return values_out

    @staticmethod
    def __get_album_for_insert(track: tk.model.Track | tk.model.FullTrack | None = None) -> dict | None:
        """
        Turn a track's Album into a structure that can be inserted into the **Albums** DB-table.

        :param track: A Track (or FullTrack) object from which to take the album values.
        :return: Dictionary with the album's values.
        """
        album = track.album
        values_out = {spdbnm.ALBUMS.ID                    : album.id,
                      spdbnm.ALBUMS.HREF                  : album.href,
                      spdbnm.ALBUMS.URI                   : album.uri,
                      spdbnm.ALBUMS.NAME                  : album.name,
                      spdbnm.ALBUMS.ALBUM_TYPE            : album.album_type,
                      spdbnm.ALBUMS.TOTAL_TRACKS          : album.total_tracks,
                      spdbnm.ALBUMS.RELEASE_DATE          : album.release_date,
                      spdbnm.ALBUMS.RELEASE_DATE_PRECISION: str(album.release_date_precision)}
        # track.id)

        return values_out

    @staticmethod
    def __get_linked_from_for_insert(track: tk.model.Track | tk.model.FullTrack | None = None) -> dict | None:
        """
        Turn a track's LinkedFrom value into a structure that can be inserted into the **TracksLinkedFrom** DB-table.

        Parameters:
            track: A Track (or FullTrack) object from which to take the Linked Track values.

        Returns:
            Dictionary with the Linked Track's values.
        """
        linked = track.linked_from

        if linked is None:
            values_out = {spdbnm.TRACKS_LINKED_FROM.FROM_ID    : track.id,
                          spdbnm.TRACKS_LINKED_FROM.RELINKED_ID: track.id}

        else:
            values_out = {spdbnm.TRACKS_LINKED_FROM.FROM_ID    : linked.id,
                          spdbnm.TRACKS_LINKED_FROM.RELINKED_ID: track.id}

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

    @staticmethod
    def __get_album_track_for_insert(track: tk.model.FullTrack = None) -> dict | None:
        """
        Returns an Album<->Track binding that can be inserted into the **AlbumsTracks** DB-table.

        Parameters:
            track: A FullTrack object with its Album.

        Returns:
            Dictionary with the Album<->Track binding.
        """
        linked = track.linked_from

        if linked is None:
            values_out = {spdbnm.ALBUMS_TRACKS.ALBUM_ID: track.album.id,
                          spdbnm.ALBUMS_TRACKS.TRACK_ID: track.id}

        else:
            values_out = {spdbnm.ALBUMS_TRACKS.ALBUM_ID: track.album.id,
                          spdbnm.ALBUMS_TRACKS.TRACK_ID: linked.id}

        return values_out

    @staticmethod
    def __get_listen_history_df_for_insert(listen_history_df: pd.DataFrame) -> pd.DataFrame:
        """
        Returns a DataFrame ready for insertion to the DB.

        Parameters:
            listen_history_df: DataFrame to insert.

        Returns:
            DataFrame ready for insertion.
        """
        df_to_insert = listen_history_df[[spdtnm.USERNAME,
                                          spdtnm.TIMESTAMP,
                                          spdtnm.TRACK_ID,
                                          spdtnm.MS_PLAYED,
                                          spdtnm.REASON_START,
                                          spdtnm.REASON_END,
                                          spdtnm.SKIPPED,
                                          spdtnm.PLATFORM,
                                          spdtnm.CONN_COUNTRY,
                                          spdtnm.TRACK_URI,
                                          spdtnm.SHUFFLE,
                                          spdtnm.OFFLINE,
                                          spdtnm.INCOGNITO]].fillna(value = {spdtnm.SKIPPED: ''},
                                                                    inplace = False)

        # df_to_insert = df_to_insert.rename(columns = {spdtnm.TIMESTAMP: spdbnm.TRACKS_LISTEN_HISTORY.TIMESTAMP,
        #                                               spdtnm.USERNAME : spdbnm.TRACKS_LISTEN_HISTORY.USERNAME,
        #                                               spdtnm.TRACK_ID : spdbnm.TRACKS_LISTEN_HISTORY.TRACK_ID,
        #                                               spdtnm.TRACK_URI: spdbnm.TRACKS_LISTEN_HISTORY.URI})

        df_to_insert = df_to_insert.drop_duplicates(
            subset = [spdbnm.TRACKS_LISTEN_HISTORY.USERNAME,
                      spdbnm.TRACKS_LISTEN_HISTORY.TIMESTAMP,
                      spdbnm.TRACKS_LISTEN_HISTORY.TRACK_ID])

        return df_to_insert

    # endregion Insertion Utilities

    # region Instantiation logic

    def __init__(self):
        """
        Initializes the DB Manager for working with the DB.
        """
        self._db_filename_ = "my_spotify_data.db"
        self._db_schema_filename = "my_spotify_data_db_scheme.sql"

        # Connect to DB
        self.connection = sqlite3.connect(self._db_filename_)
        self.cursor = self.connection.cursor()

        init_db_script = open(self._db_schema_filename, "rt").read()

        try:
            self.cursor.executescript(init_db_script)

        except sqlite3.OperationalError as e:
            DB.eprint(log.DB_OPERATIONAL_ERROR.format(e))
            log.write(message = log.DB_SCHEMA_ERROR)

    def commit(self) -> None:
        """Commits all changes to the DB."""
        self.connection.commit()

    def close(self):
        """Closes the connection and the cursor to the DB."""
        self.connection.close()
        self.cursor.close()

    # endregion Instantiation logic

    # region Insertion Logic

    def insert(self,
               table_name: str,
               values: dict | list[dict],
               columns_names: list[str],
               commit: bool = False) -> None:
        """
        Generic method to insert single or multiple values to a table.

        Parameters:
            table_name: Name of the DB-table, into which to insert the values.

            columns_names: A list of strings, containing the names of the columns into which to insert
                the values. The columns' names must be ordered the same way as the values in parameter 'values'.

            values: Dictionary, or a List of Dicts, each containing a desired value to insert.

            commit: Whether to commit the operation.

        Returns:
            None.
        """
        if values is None:
            log.write("WARNING: " + log.EMPTY_VALUES.format(table_name))

        else:
            try:
                query = f"""INSERT OR REPLACE INTO {table_name} 
                ({', '.join([name for name in columns_names])})

                VALUES 
                ({', '.join([f":{name}" for name in columns_names])});"""

                match values:
                    case dict() as values:
                        self.cursor.execute(query, values)

                    case list() as values:
                        self.cursor.executemany(query, values)

                if commit:
                    self.commit()

            except sqlite3.IntegrityError as e:
                DB.eprint(log.DB_INTEGRITY_ERROR.format(e))

            except sqlite3.OperationalError as e:
                DB.eprint(log.DB_OPERATIONAL_ERROR.format(e))

    def insert_tracks(self, tracks_values: dict | list[dict], commit: bool = False) -> None:
        """
        Insert multiple Tracks' values to the **Tracks** DB-table.

        Parameters:
            tracks_values: Dictionary, or a List of Dicts, each one containing the desired Tracks' values to insert.

            commit: Whether to commit the operation.

        Returns:
            None.
        """
        if tracks_values is None:
            log.write("WARNING: " + log.EMPTY_VALUES.format('Tracks'))

        else:
            try:
                query = f"""INSERT OR REPLACE INTO {spdbnm.TRACKS.TBL_NAME} (
                {spdbnm.TRACKS.ID},
                {spdbnm.TRACKS.NAME},                
                {spdbnm.TRACKS.DURATION_MS},
                {spdbnm.TRACKS.DISC_NUMBER},
                {spdbnm.TRACKS.TRACK_NUMBER},
                {spdbnm.TRACKS.EXPLICIT},
                {spdbnm.TRACKS.POPULARITY},
                {spdbnm.TRACKS.IS_LOCAL},
                {spdbnm.TRACKS.IS_PLAYABLE},
                {spdbnm.TRACKS.ISRC},
                {spdbnm.TRACKS.HREF},
                {spdbnm.TRACKS.URI},
                {spdbnm.TRACKS.PREVIEW_URL}
                )

                VALUES (
                :{spdbnm.TRACKS.ID},
                :{spdbnm.TRACKS.NAME},          
                :{spdbnm.TRACKS.DURATION_MS},
                :{spdbnm.TRACKS.DISC_NUMBER},
                :{spdbnm.TRACKS.TRACK_NUMBER},
                :{spdbnm.TRACKS.EXPLICIT},
                :{spdbnm.TRACKS.POPULARITY},
                :{spdbnm.TRACKS.IS_LOCAL},
                :{spdbnm.TRACKS.IS_PLAYABLE},
                :{spdbnm.TRACKS.ISRC},
                :{spdbnm.TRACKS.HREF},
                :{spdbnm.TRACKS.URI},
                :{spdbnm.TRACKS.PREVIEW_URL}
                );"""

                match tracks_values:
                    case dict() as tracks_values:
                        self.cursor.execute(query, tracks_values)

                    case list() as tracks_values:
                        self.cursor.executemany(query, tracks_values)

                if commit:
                    self.commit()

            except sqlite3.IntegrityError as e:
                # DB.eprint(log.CANNOT_INSERT.format(str(track_values)))
                DB.eprint(log.DB_INTEGRITY_ERROR.format(e))

            except sqlite3.OperationalError as e:
                # DB.eprint(log.CANNOT_INSERT.format(str(track_values)))
                DB.eprint(log.DB_OPERATIONAL_ERROR.format(e))

    def insert_artists(self, artists_values: dict | list[dict], commit: bool = False) -> None:
        """
        Insert single or multiple Artists' values to the **Artists** DB-table.

        Parameters:
            artists_values: Dictionary, or a List of Dicts, each one containing the desired Artists' values to insert.

            commit: Whether to commit the operation.

        Returns:
            None.
        """
        if artists_values is None:
            log.write("WARNING: " + log.EMPTY_VALUES.format('Artists'))

        else:
            try:
                query = f"""INSERT OR REPLACE INTO {spdbnm.ARTISTS.TBL_NAME} (
                {spdbnm.ARTISTS.ID},
                {spdbnm.ARTISTS.NAME},                
                {spdbnm.ARTISTS.TOTAL_FOLLOWERS},
                {spdbnm.ARTISTS.POPULARITY},
                {spdbnm.ARTISTS.HREF},
                {spdbnm.ARTISTS.URI}
                )

                VALUES (
                :{spdbnm.ARTISTS.ID},
                :{spdbnm.ARTISTS.NAME},             
                :{spdbnm.ARTISTS.TOTAL_FOLLOWERS},
                :{spdbnm.ARTISTS.POPULARITY},
                :{spdbnm.ARTISTS.HREF},
                :{spdbnm.ARTISTS.URI}
                );"""

                match artists_values:
                    case dict() as artists_values:
                        self.cursor.execute(query, artists_values)

                    case list() as artists_values:
                        self.cursor.executemany(query, artists_values)

                if commit:
                    self.commit()

            except sqlite3.IntegrityError as e:
                DB.eprint(log.DB_INTEGRITY_ERROR.format(e))

            except sqlite3.OperationalError as e:
                DB.eprint(log.DB_OPERATIONAL_ERROR.format(e))

    def insert_albums(self, albums_values: dict | list[dict], commit: bool = False) -> None:
        """
        Insert single or multiple Albums' values to the **Albums** DB-table.

        Parameters:
            albums_values: Dictionary, or a List of Dicts each containing the desired Albums' values to insert.

            commit: Whether to commit the operation.

        Returns:
            None.
        """
        self.insert(table_name = spdbnm.ALBUMS.TBL_NAME,
                    values = albums_values,
                    columns_names = [spdbnm.ALBUMS.ID,
                                     spdbnm.ALBUMS.NAME,
                                     spdbnm.ALBUMS.TOTAL_TRACKS,
                                     spdbnm.ALBUMS.RELEASE_DATE,
                                     spdbnm.ALBUMS.RELEASE_DATE_PRECISION,
                                     spdbnm.ALBUMS.ALBUM_TYPE,
                                     spdbnm.ALBUMS.IS_AVAILABLE,
                                     spdbnm.ALBUMS.HREF,
                                     spdbnm.ALBUMS.URI],
                    commit = commit)

        # if albums_values is None:
        #     log.write("WARNING: " + log.EMPTY_VALUES.format('Albums'))
        #
        # else:
        #     try:
        #         query = f"""INSERT OR REPLACE INTO {spdbnm.ALBUMS.TBL_NAME} (
        #         {spdbnm.ALBUMS.ID},
        #         {spdbnm.ALBUMS.NAME},
        #         {spdbnm.ALBUMS.TOTAL_TRACKS},
        #         {spdbnm.ALBUMS.RELEASE_DATE},
        #         {spdbnm.ALBUMS.RELEASE_DATE_PRECISION},
        #         {spdbnm.ALBUMS.ALBUM_TYPE},
        #         {spdbnm.ALBUMS.HREF},
        #         {spdbnm.ALBUMS.URI}
        #         )
        #
        #         VALUES (
        #         :{spdbnm.ALBUMS.ID},
        #         :{spdbnm.ALBUMS.NAME},
        #         :{spdbnm.ALBUMS.TOTAL_TRACKS},
        #         :{spdbnm.ALBUMS.RELEASE_DATE},
        #         :{spdbnm.ALBUMS.RELEASE_DATE_PRECISION},
        #         :{spdbnm.ALBUMS.ALBUM_TYPE},
        #         :{spdbnm.ALBUMS.HREF},
        #         :{spdbnm.ALBUMS.URI}
        #         );"""
        #
        #         match albums_values:
        #             case dict() as albums_values:
        #                 self.cursor.execute(query, albums_values)
        #
        #             case list() as albums_values:
        #                 self.cursor.executemany(query, albums_values)
        #
        #         if commit:
        #             self.commit()
        #
        #     except sqlite3.IntegrityError as e:
        #         DB.eprint(log.DB_INTEGRITY_ERROR.format(e))
        #
        #     except sqlite3.OperationalError as e:
        #         DB.eprint(log.DB_OPERATIONAL_ERROR.format(e))

    def __insert_listen_history_df(self, listen_history_df: pd.DataFrame, commit: bool = False) -> None:
        """
        Insert values from a prepared Listen History DataFrame to DB.

        Parameters:
            listen_history_df: DataFrame with the prepared Listen History data.

            commit: Whether to commit the operation.

        Returns:
            None.
        """
        listen_history_list = listen_history_df.to_dict('records')

        self.insert(table_name = spdbnm.TRACKS_LISTEN_HISTORY.TBL_NAME,
                    values = listen_history_list,
                    columns_names = [spdbnm.TRACKS_LISTEN_HISTORY.USERNAME,
                                     spdbnm.TRACKS_LISTEN_HISTORY.TIMESTAMP,
                                     spdbnm.TRACKS_LISTEN_HISTORY.TRACK_ID,
                                     spdbnm.TRACKS_LISTEN_HISTORY.MS_PLAYED,
                                     spdbnm.TRACKS_LISTEN_HISTORY.REASON_START,
                                     spdbnm.TRACKS_LISTEN_HISTORY.REASON_END,
                                     spdbnm.TRACKS_LISTEN_HISTORY.SKIPPED,
                                     spdbnm.TRACKS_LISTEN_HISTORY.PLATFORM,
                                     spdbnm.TRACKS_LISTEN_HISTORY.CONN_COUNTRY,
                                     spdbnm.TRACKS_LISTEN_HISTORY.URI,
                                     spdbnm.TRACKS_LISTEN_HISTORY.SHUFFLE,
                                     spdbnm.TRACKS_LISTEN_HISTORY.OFFLINE,
                                     spdbnm.TRACKS_LISTEN_HISTORY.INCOGNITO_MODE],
                    commit = commit)

    def insert_listen_history(self, df: pd.DataFrame, commit: bool = False) -> None:
        """
        Insert Listen History table to the DB.
        Cleans data and removes duplicate rows before inserting.

        Parameters:
            df: DataFrame with Listen History records for inserting.

            commit: Whether to commit the operation.

        Returns:
            None.
        """
        df_to_insert = DB.__get_listen_history_df_for_insert(df)

        self.__insert_listen_history_df(df_to_insert, commit)

    def insert_artists_albums(self, artists_albums_values: dict | list[dict], commit: bool = False) -> None:
        """Insert single or multiple Artists' Albums values to the **Artists' Albums** DB table.

        Parameters:
            artists_albums_values: Dictionary, or a List of Dicts, each containing a desired artist's album to insert.

            commit: Whether to commit the operation.

        Returns:
            None.
        """
        self.insert(table_name = spdbnm.ARTISTS_ALBUMS.TBL_NAME,
                    values = artists_albums_values,
                    columns_names = [spdbnm.ARTISTS_ALBUMS.ARTIST_ID,
                                     spdbnm.ARTISTS_ALBUMS.ALBUM_ID],
                    commit = commit)

    def insert_albums_tracks(self, albums_tracks_values: dict | list[dict], commit: bool = False) -> None:
        """
        Insert single or multiple Albums' Tracks' values to the **Albums' Tracks** DB-table.

        Parameters:
            albums_tracks_values: Dictionary, or a List of Dicts, each containing the desired Album's Tracks' values to insert.

            commit: Whether to commit the operation.

        Returns:
            None.
        """
        self.insert(table_name = spdbnm.ALBUMS_TRACKS.TBL_NAME,
                    values = albums_tracks_values,
                    columns_names = [spdbnm.ALBUMS_TRACKS.ALBUM_ID,
                                     spdbnm.ALBUMS_TRACKS.TRACK_ID],
                    commit = commit)

    def insert_linked_tracks(self, linked_track_values: dict | list[dict], commit: bool = False) -> None:
        """
        Insert single or multiple linked tracks' values to the **Linked Tracks** DB table.

        Parameters:
            linked_track_values: Dictionary, or a List of dicts, each dict containing a Linked Track, with its
                original track's ID ('linked_from') and its Relinked Track ID ("Track's Known ID").

            commit: Whether to commit the operation.

        Returns:
            None.
        """
        self.insert(table_name = spdbnm.TRACKS_LINKED_FROM.TBL_NAME,
                    values = linked_track_values,
                    columns_names = [spdbnm.TRACKS_LINKED_FROM.FROM_ID,
                                     spdbnm.TRACKS_LINKED_FROM.RELINKED_ID],
                    commit = commit)

    # endregion Insertion Logic

    # region Selection Logic

    def get_listen_history_df(self) -> pd.DataFrame:
        query = f"""SELECT
                {spdbnm.TRACKS_LISTEN_HISTORY.USERNAME},
                {spdbnm.TRACKS_LISTEN_HISTORY.TIMESTAMP},
                {spdbnm.TRACKS_LISTEN_HISTORY.TRACK_ID},
                {spdbnm.TRACKS_LISTEN_HISTORY.MS_PLAYED},
                {spdbnm.TRACKS_LISTEN_HISTORY.REASON_START},
                {spdbnm.TRACKS_LISTEN_HISTORY.REASON_END},
                {spdbnm.TRACKS_LISTEN_HISTORY.SKIPPED},
                {spdbnm.TRACKS_LISTEN_HISTORY.PLATFORM},
                {spdbnm.TRACKS_LISTEN_HISTORY.CONN_COUNTRY},
                {spdbnm.TRACKS_LISTEN_HISTORY.URI},
                {spdbnm.TRACKS_LISTEN_HISTORY.SHUFFLE},
                {spdbnm.TRACKS_LISTEN_HISTORY.OFFLINE},
                {spdbnm.TRACKS_LISTEN_HISTORY.INCOGNITO_MODE}
                FROM {spdbnm.TRACKS_LISTEN_HISTORY.VIEW_NAME}
                ORDER BY {spdbnm.TRACKS_LISTEN_HISTORY.TIMESTAMP} ASC;
                """

        listen_history_df = pd.read_sql_query(sql = query, con = self.connection)

        return listen_history_df

    # endregion Selection Logic
