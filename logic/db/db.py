import sqlite3
import sys
import pandas as pd
import tekore as tk
from logic.frontend import log
from logic.model.sp_data_set_names import SPDT as SPDTNM
from logic.db import db_names as SPDBNM


class DB:
    """
    Manages the local DB to save Spotify data into, for further calculations.
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
        Turns a track into a structure that can be inserted into the **Tracks** DB-table.

        Parameters:
            track: A Track (or FullTrack) object from which to take the track values.

        Returns:
            Dictionary with the track's values.
        """
        values_out = None

        if track is not None:
            values_out = {SPDBNM.TRACKS.ID          : track.id,
                          SPDBNM.TRACKS.HREF        : track.href,
                          SPDBNM.TRACKS.URI         : track.uri,
                          SPDBNM.TRACKS.DISC_NUMBER : track.disc_number,
                          SPDBNM.TRACKS.DURATION_MS : track.duration_ms,
                          SPDBNM.TRACKS.EXPLICIT    : track.explicit,
                          SPDBNM.TRACKS.NAME        : track.name,
                          SPDBNM.TRACKS.PREVIEW_URL : str(track.preview_url),
                          SPDBNM.TRACKS.TRACK_NUMBER: track.track_number,
                          SPDBNM.TRACKS.IS_LOCAL    : track.is_local,
                          SPDBNM.TRACKS.POPULARITY  : track.popularity,
                          SPDBNM.TRACKS.IS_PLAYABLE : track.is_playable}

        return values_out

    @staticmethod
    def __get_album_for_insert(track: tk.model.Track | tk.model.FullTrack | None = None) -> dict | None:
        """
        Turns a track's Album into a structure that can be inserted into the **Albums** DB-table.

        Parameters:
            track: A Track (or FullTrack) object from which to take the album values.

        Returns:
            Dictionary with the album's values.
        """
        album = track.album
        values_out = {SPDBNM.ALBUMS.ID                    : album.id,
                      SPDBNM.ALBUMS.HREF                  : album.href,
                      SPDBNM.ALBUMS.URI                   : album.uri,
                      SPDBNM.ALBUMS.NAME                  : album.name,
                      SPDBNM.ALBUMS.ALBUM_TYPE            : album.album_type,
                      SPDBNM.ALBUMS.TOTAL_TRACKS          : album.total_tracks,
                      SPDBNM.ALBUMS.RELEASE_DATE          : album.release_date,
                      SPDBNM.ALBUMS.RELEASE_DATE_PRECISION: str(album.release_date_precision)}
        # track.id)

        return values_out

    @staticmethod
    def __get_linked_from_for_insert(track: tk.model.Track | tk.model.FullTrack | None = None) -> dict | None:
        """
        Turns a track's LinkedFrom value into a structure that can be inserted into the **TracksLinkedFrom** DB-table.

        Parameters:
            track: A Track (or FullTrack) object from which to take the Linked Track values.

        Returns:
            Dictionary with the Linked Track's values.
        """
        linked = track.linked_from

        if linked is None:
            values_out = {SPDBNM.LINKED_TRACKS.FROM_ID    : track.id,
                          SPDBNM.LINKED_TRACKS.RELINKED_ID: track.id}

        else:
            values_out = {SPDBNM.LINKED_TRACKS.FROM_ID    : linked.id,
                          SPDBNM.LINKED_TRACKS.RELINKED_ID: track.id}

        return values_out

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
            values_out = {SPDBNM.ALBUMS_TRACKS.ALBUM_ID: track.album.id,
                          SPDBNM.ALBUMS_TRACKS.TRACK_ID: track.id}

        else:
            values_out = {SPDBNM.ALBUMS_TRACKS.ALBUM_ID: track.album.id,
                          SPDBNM.ALBUMS_TRACKS.TRACK_ID: linked.id}

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
        df_to_insert = listen_history_df[[SPDTNM.USERNAME,
                                          SPDTNM.TIMESTAMP,
                                          SPDTNM.TRACK_ID,
                                          SPDTNM.MS_PLAYED,
                                          SPDTNM.REASON_START,
                                          SPDTNM.REASON_END,
                                          SPDTNM.SKIPPED,
                                          SPDTNM.PLATFORM,
                                          SPDTNM.CONN_COUNTRY,
                                          SPDTNM.TRACK_URI,
                                          SPDTNM.SHUFFLE,
                                          SPDTNM.OFFLINE,
                                          SPDTNM.INCOGNITO]].fillna(value = {SPDTNM.SKIPPED: ''},
                                                                    inplace = False)

        df_to_insert = df_to_insert.drop_duplicates(
            subset = [SPDBNM.TRACKS_LISTEN_HISTORY.USERNAME,
                      SPDBNM.TRACKS_LISTEN_HISTORY.TIMESTAMP,
                      SPDBNM.TRACKS_LISTEN_HISTORY.TRACK_ID])

        return df_to_insert

    # endregion Insertion Utilities

    # region Instantiation logic

    def __init__(self):
        """
        Initializes the DB Manager for working with the DB.
        """
        self._db_filename_ = "data/personal_data/my_spotify_data.db"
        self._db_schema_filename = "logic/db/my_spotify_data_db_scheme.sql"

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

    def close(self) -> None:
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
                        log.write(log.INSERTING_RECORD.format(f"``{table_name}``"))
                        self.cursor.execute(query, values)

                        log.write(log.RECORD_INSERTED)

                    case list() as values:
                        log.write(log.INSERTING_RECORDS.format(f"``{table_name}``", len(values)))
                        self.cursor.executemany(query, values)

                        log.write(log.RECORDS_INSERTED)

                    case _:
                        log.write(log.ERROR_INVALID_RECORDS_TYPE.format(type(values)))

                if commit:
                    self.commit()

            except sqlite3.IntegrityError as e:
                DB.eprint(log.DB_INTEGRITY_ERROR.format(e))

            except sqlite3.OperationalError as e:
                DB.eprint(log.DB_OPERATIONAL_ERROR.format(e))

    def __insert_listen_history_df(self, listen_history_df: pd.DataFrame, commit: bool = False) -> None:
        """
        Inserts values from a prepared Listen History DataFrame to DB.

        Parameters:
            listen_history_df: DataFrame with the prepared Listen History data.

            commit: Whether to commit the operation.

        Returns:
            None.
        """
        listen_history_list = listen_history_df.to_dict('records')

        self.insert(table_name = SPDBNM.TRACKS_LISTEN_HISTORY.TBL_NAME,
                    values = listen_history_list,
                    columns_names = [SPDBNM.TRACKS_LISTEN_HISTORY.USERNAME,
                                     SPDBNM.TRACKS_LISTEN_HISTORY.TIMESTAMP,
                                     SPDBNM.TRACKS_LISTEN_HISTORY.TRACK_ID,
                                     SPDBNM.TRACKS_LISTEN_HISTORY.MS_PLAYED,
                                     SPDBNM.TRACKS_LISTEN_HISTORY.REASON_START,
                                     SPDBNM.TRACKS_LISTEN_HISTORY.REASON_END,
                                     SPDBNM.TRACKS_LISTEN_HISTORY.SKIPPED,
                                     SPDBNM.TRACKS_LISTEN_HISTORY.PLATFORM,
                                     SPDBNM.TRACKS_LISTEN_HISTORY.CONN_COUNTRY,
                                     SPDBNM.TRACKS_LISTEN_HISTORY.URI,
                                     SPDBNM.TRACKS_LISTEN_HISTORY.SHUFFLE,
                                     SPDBNM.TRACKS_LISTEN_HISTORY.OFFLINE,
                                     SPDBNM.TRACKS_LISTEN_HISTORY.INCOGNITO_MODE],
                    commit = commit)

    def insert_tracks(self, tracks_values: dict | list[dict], commit: bool = False) -> None:
        """
        Inserts multiple Tracks' values to the **Tracks** DB-table.

        Parameters:
            tracks_values: Dictionary, or a List of Dicts, each one containing the desired Tracks' values to insert.

            commit: Whether to commit the operation.

        Returns:
            None.
        """
        self.insert(table_name = SPDBNM.TRACKS.TBL_NAME,
                    values = tracks_values,
                    columns_names = [SPDBNM.TRACKS.ID,
                                     SPDBNM.TRACKS.NAME,
                                     SPDBNM.TRACKS.DURATION_MS,
                                     SPDBNM.TRACKS.DISC_NUMBER,
                                     SPDBNM.TRACKS.TRACK_NUMBER,
                                     SPDBNM.TRACKS.EXPLICIT,
                                     SPDBNM.TRACKS.POPULARITY,
                                     SPDBNM.TRACKS.IS_LOCAL,
                                     SPDBNM.TRACKS.IS_PLAYABLE,
                                     SPDBNM.TRACKS.ISRC,
                                     SPDBNM.TRACKS.HREF,
                                     SPDBNM.TRACKS.URI,
                                     SPDBNM.TRACKS.PREVIEW_URL],
                    commit = commit)

    def insert_artists(self, artists_values: dict | list[dict], commit: bool = False) -> None:
        """
        Inserts single or multiple Artists' values to the **Artists** DB-table.

        Parameters:
            artists_values: Dictionary, or a List of Dicts, each one containing the desired Artists' values to insert.

            commit: Whether to commit the operation.

        Returns:
            None.
        """
        self.insert(table_name = SPDBNM.ARTISTS.TBL_NAME,
                    values = artists_values,
                    columns_names = [SPDBNM.ARTISTS.ID,
                                     SPDBNM.ARTISTS.NAME,
                                     SPDBNM.ARTISTS.TOTAL_FOLLOWERS,
                                     SPDBNM.ARTISTS.POPULARITY,
                                     SPDBNM.ARTISTS.HREF,
                                     SPDBNM.ARTISTS.URI],
                    commit = commit)

    def insert_genres(self, genres_values: dict | list[dict], commit: bool = False) -> None:
        """
        Inserts single or multiple Genres to the **Genres** DB-table.

        Parameters:
            genres_values: Genre string, or a List of strings, to insert.

            commit: Whether to commit the operation.

        Returns:
            None.
        """
        self.insert(table_name = SPDBNM.GENRES.TBL_NAME,
                    values = genres_values,
                    columns_names = [SPDBNM.GENRES.GENRE_NAME],
                    commit = commit)

    def insert_artists_genres(self, artists_genres_values: dict | list[dict], commit: bool = False) -> None:
        """
        Inserts single or multiple Artists' Genres values to the **Artists' Genres** DB table.

        Parameters:
            artists_genres_values: Dictionary, or a List of Dicts, each containing a desired artist's genre to insert.

            commit: Whether to commit the operation.

        Returns:
            None.
        """
        self.insert(table_name = SPDBNM.ARTISTS_GENRES.TBL_NAME,
                    values = artists_genres_values,
                    columns_names = [SPDBNM.ARTISTS_GENRES.ARTIST_ID,
                                     SPDBNM.ARTISTS_GENRES.GENRE_NAME],
                    commit = commit)

    def insert_albums(self, albums_values: dict | list[dict], commit: bool = False) -> None:
        """
        Inserts single or multiple Albums' values to the **Albums** DB-table.

        Parameters:
            albums_values: Dictionary, or a List of Dicts each containing the desired Albums' values to insert.

            commit: Whether to commit the operation.

        Returns:
            None.
        """
        self.insert(table_name = SPDBNM.ALBUMS.TBL_NAME,
                    values = albums_values,
                    columns_names = [SPDBNM.ALBUMS.ID,
                                     SPDBNM.ALBUMS.NAME,
                                     SPDBNM.ALBUMS.TOTAL_TRACKS,
                                     SPDBNM.ALBUMS.RELEASE_DATE,
                                     SPDBNM.ALBUMS.RELEASE_DATE_PRECISION,
                                     SPDBNM.ALBUMS.ALBUM_TYPE,
                                     SPDBNM.ALBUMS.IS_AVAILABLE,
                                     SPDBNM.ALBUMS.HREF,
                                     SPDBNM.ALBUMS.URI],
                    commit = commit)

    def insert_artists_albums(self, artists_albums_values: dict | list[dict], commit: bool = False) -> None:
        """Inserts single or multiple Artists' Albums values to the **Artists' Albums** DB table.

        Parameters:
            artists_albums_values: Dictionary, or a List of Dicts, each containing a desired artist's album to insert.

            commit: Whether to commit the operation.

        Returns:
            None.
        """
        self.insert(table_name = SPDBNM.ARTISTS_ALBUMS.TBL_NAME,
                    values = artists_albums_values,
                    columns_names = [SPDBNM.ARTISTS_ALBUMS.ARTIST_ID,
                                     SPDBNM.ARTISTS_ALBUMS.ALBUM_ID,
                                     SPDBNM.ARTISTS_ALBUMS.ALBUM_GROUP],
                    commit = commit)

    def insert_albums_tracks(self, albums_tracks_values: dict | list[dict], commit: bool = False) -> None:
        """
        Inserts single or multiple Albums' Tracks' values to the **Albums' Tracks** DB-table.

        Parameters:
            albums_tracks_values: Dictionary, or a List of Dicts, each containing the desired Album's Tracks'
                values to insert.

            commit: Whether to commit the operation.

        Returns:
            None.
        """
        self.insert(table_name = SPDBNM.ALBUMS_TRACKS.TBL_NAME,
                    values = albums_tracks_values,
                    columns_names = [SPDBNM.ALBUMS_TRACKS.ALBUM_ID,
                                     SPDBNM.ALBUMS_TRACKS.TRACK_ID],
                    commit = commit)

    def insert_linked_tracks(self, linked_track_values: dict | list[dict], commit: bool = False) -> None:
        """
        Inserts single or multiple linked tracks' values to the **Linked Tracks** DB table.

        Parameters:
            linked_track_values: Dictionary, or a List of dicts, each dict containing a Linked Track, with its
                original track's ID ('linked_from') and its Relinked Track ID ("Track's Known ID").

            commit: Whether to commit the operation.

        Returns:
            None.
        """
        self.insert(table_name = SPDBNM.LINKED_TRACKS.TBL_NAME,
                    values = linked_track_values,
                    columns_names = [SPDBNM.LINKED_TRACKS.FROM_ID,
                                     SPDBNM.LINKED_TRACKS.RELINKED_ID],
                    commit = commit)

    def insert_linked_albums(self, linked_album_values: dict | list[dict], commit: bool = False) -> None:
        """
        Inserts single or multiple linked albums' values to the **Linked Albums** DB table.

        Parameters:
            linked_album_values: Dictionary, or a List of dicts, each dict containing a Linked Album, with its
                original album's ID ('linked_from') and its Relinked Album ID ("Album's Known ID").

            commit: Whether to commit the operation.

        Returns:
            None.
        """
        self.insert(table_name = SPDBNM.LINKED_ALBUMS.TBL_NAME,
                    values = linked_album_values,
                    columns_names = [SPDBNM.LINKED_ALBUMS.FROM_ID,
                                     SPDBNM.LINKED_ALBUMS.RELINKED_ID],
                    commit = commit)

    def insert_listen_history(self, df: pd.DataFrame, commit: bool = False) -> None:
        """
        Inserts Listen History table to the DB.
        Cleans data and removes duplicate rows before inserting.

        Parameters:
            df: DataFrame with Listen History records for inserting.

            commit: Whether to commit the operation.

        Returns:
            None.
        """
        df_to_insert = DB.__get_listen_history_df_for_insert(df)

        self.__insert_listen_history_df(df_to_insert, commit)

    # endregion Insertion Logic

    # region Selection Logic

    def get_listen_history_df(self) -> pd.DataFrame:
        query = f"""SELECT
                    {SPDBNM.V_KNOWN_LISTEN_HISTORY.USERNAME},
                    {SPDBNM.V_KNOWN_LISTEN_HISTORY.TIMESTAMP},
                    {SPDBNM.V_KNOWN_LISTEN_HISTORY.TRACK_LISTENED_ID},
                    {SPDBNM.V_KNOWN_LISTEN_HISTORY.TRACK_KNOWN_ID},
                    {SPDBNM.V_KNOWN_LISTEN_HISTORY.TRACK_NAME},
                    {SPDBNM.V_KNOWN_LISTEN_HISTORY.ALBUM_KNOWN_ID},
                    {SPDBNM.V_KNOWN_LISTEN_HISTORY.ALBUM_NAME},
                    {SPDBNM.V_KNOWN_LISTEN_HISTORY.ALBUM_ARTIST_ID},
                    {SPDBNM.V_KNOWN_LISTEN_HISTORY.ALBUM_ARTIST_NAME},
                    {SPDBNM.V_KNOWN_LISTEN_HISTORY.MS_PLAYED},
                    {SPDBNM.V_KNOWN_LISTEN_HISTORY.TRACK_DURATION_MS},
                    {SPDBNM.V_KNOWN_LISTEN_HISTORY.REASON_START},
                    {SPDBNM.V_KNOWN_LISTEN_HISTORY.REASON_END},
                    {SPDBNM.V_KNOWN_LISTEN_HISTORY.SKIPPED},
                    {SPDBNM.V_KNOWN_LISTEN_HISTORY.PLATFORM},
                    {SPDBNM.V_KNOWN_LISTEN_HISTORY.CONN_COUNTRY},
                    {SPDBNM.V_KNOWN_LISTEN_HISTORY.URI},
                    {SPDBNM.V_KNOWN_LISTEN_HISTORY.SHUFFLE},
                    {SPDBNM.V_KNOWN_LISTEN_HISTORY.OFFLINE},
                    {SPDBNM.V_KNOWN_LISTEN_HISTORY.INCOGNITO_MODE}
                    FROM {SPDBNM.V_KNOWN_LISTEN_HISTORY.VIEW_NAME};
                    """

        log.write(log.FETCHING_LISTEN_HISTORY)
        listen_history_df = pd.read_sql_query(sql = query, con = self.connection)

        log.write(log.LISTEN_HISTORY_FETCHED)

        return listen_history_df

    # endregion Selection Logic
