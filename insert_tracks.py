import json
import sqlite3
import sys
import pandas as pd
from dataclasses import dataclass

import log
from spotify_data_set import ColNames as spdcol
import tekore as tk
from typing import Optional, Union


class DB:
    """
    Manages the local DB for saving Spotify data for further calculations.
    """

    @dataclass(frozen = True)
    class TABLES:
        TRACKS = 'tracks'
        ALBUMS = 'albums'
        TRACKS_LISTEN_HISTORY = 'tracks_listen_history'
        TRACKS_LINKED_FROM = 'tracks_linked_from'
        ARTISTS_ALBUMS = 'artists_albums'
        ARTISTS_TRACKS = 'artists_track'

    # region Static Utilities

    @staticmethod
    def eprint(*args, **kwargs):
        """Print to stderr."""
        print(*args, file = sys.stderr, **kwargs)
        log.write(*args)

    @staticmethod
    def get_track_for_insert(track: dict | tk.model.Track | tk.model.FullTrack | None = None) -> tuple | None:
        """Turn a track into a tuple insertable into the tracks table."""
        match type(track):
            case dict():
                values_out = (track['id'],
                              track['href'],
                              track['uri'],
                              track['disc_number'],
                              track['duration_ms'],
                              track['explicit'],
                              track['name'],
                              track['preview_url'],
                              track['track_number'],
                              track['is_local'],
                              track['popularity'],
                              track['is_playable'])

            case tk.model.Track | tk.model.FullTrack:
                values_out = (track.id,
                              track.href,
                              track.uri,
                              track.disc_number,
                              track.duration_ms,
                              track.explicit,
                              track.name,
                              track.preview_url,
                              track.track_number,
                              track.is_local,
                              track.popularity,
                              track.is_playable)

            case _:
                values_out = None

        return values_out

    @staticmethod
    def get_album_for_insert(track: dict | tk.model.Track | tk.model.FullTrack | None = None) -> tuple | None:
        """Turn a track into a tuple insertable into the album table."""
        tk.mode
        match type(track):
            case dict():
                album = track['album']
                values_out = (album['id'],
                              album['href'],
                              album['uri'],
                              album['name'],
                              album['total_tracks'],
                              album['release_date'],
                              str(album['release_date_precision']),
                              track['id'])

            case tk.model.Track | tk.model.FullTrack:
                album = track.album
                values_out = (album.id,
                              album.href,
                              album.uri,
                              album.name,
                              album.total_tracks,
                              album.release_date,
                              str(album.release_date_precision),
                              track.id)

            case _:
                values_out = None

        return values_out

    @staticmethod
    def get_linked_from_for_insert(track: dict | tk.model.Track | tk.model.FullTrack | None = None) -> tuple | None:
        """Turn a track into a tuple insertable into the tracks_linked_from table."""
        match type(track):
            case dict():
                linked = track['linked_from']

                if linked is None:
                    # eprint("WARNING: linked_from attribute is null")
                    values_out = (track['id'], track['id'])

                else:
                    values_out = (linked['id'],
                                  linked['href'],
                                  linked['uri'],
                                  track['id'])

            case tk.model.Track | tk.model.FullTrack:
                linked = track.linked_from

                if linked is None:
                    # eprint("WARNING: linked_from attribute is null")
                    values_out = (track.id, track.id)

                else:
                    values_out = (linked.id,
                                  linked.href,
                                  linked.uri,
                                  track.id)

            case _:
                values_out = None

        return values_out

    @staticmethod
    def get_artists_album_for_insert(track: dict | tk.model.Track | tk.model.FullTrack | None = None) -> list | None:
        """Turn a track into a tuple list insertable into the artists_album table."""
        match type(track):
            case dict():
                artists = track['album']['artists']
                values_out = [None] * len(artists)

                for i, artist in enumerate(artists):
                    values_out[i] = (artist['id'],
                                     artist['href'],
                                     artist['uri'],
                                     artist['name'],
                                     track['album']['id'])

            case tk.model.Track | tk.model.FullTrack:
                artists = track.album.artists
                values_out = [None] * len(artists)

                for i, artist in enumerate(artists):
                    values_out[i] = (artist.id,
                                     artist.href,
                                     artist.uri,
                                     artist.name,
                                     track.album.id)

            case _:
                values_out = None

        return values_out

    @staticmethod
    def get_artists_track_for_insert(track: dict | tk.model.Track | tk.model.FullTrack | None = None) -> list | None:
        """Turn a track into a tuple list insertable into the artists_track table."""
        match type(track):
            case dict():
                artists = track['artists']
                values_out = [None] * len(artists)

                for i, artist in enumerate(artists):
                    values_out[i] = (artist['id'],
                                     artist['href'],
                                     artist['uri'],
                                     artist['name'],
                                     track['id'])

            case tk.model.Track | tk.model.FullTrack:
                artists = track.artists
                values_out = [None] * len(artists)

                for i, artist in enumerate(artists):
                    values_out[i] = (artist.id,
                                     artist.href,
                                     artist.uri,
                                     artist.name,
                                     track.id)

            case _:
                values_out = None

        return values_out

    # endregion Static Utilities

    def __init__(self):
        """
        Initializes the DB Manager for working with the DB.
        """
        # Connect to DB
        self.connection = sqlite3.connect("tracks.db")
        self.cursor = self.connection.cursor()

    def commit(self) -> None:
        """Commits all changes to the DB."""
        self.connection.commit()

    def close(self):
        """Closes the connection and the cursor to the DB."""
        self.connection.close()
        self.cursor.close()

    def insert_track_full(self, track: Optional[Union[dict, tk.model.FullTrack]] = None) -> None:
        """Insert full track record for all tables. Don't commit."""
        self.insert_track(DB.get_track_for_insert(track))
        self.insert_album(DB.get_album_for_insert(track))
        self.insert_artists_album(DB.get_artists_album_for_insert(track))
        self.insert_artists_track(DB.get_artists_track_for_insert(track))
        self.insert_linked_from(DB.get_linked_from_for_insert(track))

    def insert_track(self, track_values: tuple) -> None:
        """Insert track values to DB. Does not commit."""
        if track_values is None:
            log.write("WARNING: " + log.EMPTY_VALUES.format('Track'))

        else:
            try:
                self.cursor.execute(f"""INSERT OR REPLACE INTO {DB.TABLES.TRACKS} (pk_id,
                href,
                uri,
                disc_number,
                duration_ms,
                explicit,
                name,
                preview_url,
                track_number,
                is_local,
                popularity,
                is_playable)
                VALUES {str(track_values)}""")

            except sqlite3.IntegrityError as e:
                DB.eprint(f"ERROR: Could not insert the following: {str(track_values)}")
                DB.eprint(f"sqlite3.IntegrityError: {e}")

            except sqlite3.OperationalError as e:
                DB.eprint(f"ERROR: Could not insert the following: {str(track_values)}")
                DB.eprint(f"sqlite3.OperationalError: {e}")

    def insert_album(self, album_values: tuple) -> None:
        """Insert album values to DB. Does not commit."""
        if album_values is None:
            log.write("WARNING: " + log.EMPTY_VALUES.format('Album'))

        else:
            try:
                self.cursor.execute(f"""INSERT OR REPLACE INTO {DB.TABLES.ALBUMS} (pk_id,
                href,
                uri,
                name,
                total_tracks,
                release_date,
                release_date_precision,
                fk_track_id)
                VALUES {str(album_values)}""")

            except sqlite3.IntegrityError as e:
                DB.eprint(f"ERROR: Could not insert the following: {str(album_values)}")
                DB.eprint(f"sqlite3.IntegrityError: {e}")

            except sqlite3.OperationalError as e:
                DB.eprint(f"ERROR: Could not insert the following: {str(album_values)}")
                DB.eprint(f"sqlite3.OperationalError: {e}")

    def insert_linked_from(self, linked_track_values: tuple) -> None:
        """Insert linked_from values to DB. Does not commit."""
        if linked_track_values is None:
            log.write("WARNING: " + log.EMPTY_VALUES.format('Linked Track'))

        else:
            try:
                if len(linked_track_values) == 2:
                    self.cursor.execute(f"""INSERT OR REPLACE INTO {DB.TABLES.TRACKS_LINKED_FROM} (pk_id,
                    fk_track_id)
                    VALUES {str(linked_track_values)}""")

                elif len(linked_track_values) == 4:
                    self.cursor.execute(f"""INSERT OR REPLACE INTO {DB.TABLES.TRACKS_LINKED_FROM} (pk_id,
                    href,
                    uri,
                    fk_track_id)
                    VALUES {str(linked_track_values)}""")

                else:
                    DB.eprint("WARNING: Wrong number of values to insert. Should be either 2 or 4. Skipping")

            except sqlite3.IntegrityError as e:
                DB.eprint(f"ERROR: Could not insert the following: {str(linked_track_values)}")
                DB.eprint(f"sqlite3.IntegrityError: {e}")

            except sqlite3.OperationalError as e:
                DB.eprint(f"ERROR: Could not insert the following: {str(linked_track_values)}")
                DB.eprint(f"sqlite3.OperationalError: {e}")

    def insert_artists_album(self, artists_albums_values: list) -> None:
        """Insert artist values to DB. Does not commit."""
        if artists_albums_values is None:
            log.write("WARNING: " + log.EMPTY_VALUES.format('Artist Albums'))

        else:
            for artist_album in artists_albums_values:
                try:
                    self.cursor.execute(f"""INSERT OR REPLACE INTO {DB.TABLES.ARTISTS_ALBUMS} (pk_id,
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

    def insert_artists_track(self, artists_tracks_values: list) -> None:
        """Insert artist values to DB. Does not commit."""
        if artists_tracks_values is None:
            log.write("WARNING: " + log.EMPTY_VALUES.format('Artist Tracks'))

        else:
            for artist_track in artists_tracks_values:
                try:
                    self.cursor.execute(f"""INSERT OR REPLACE INTO {DB.TABLES.ARTISTS_TRACKS} (pk_id,
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

    def insert_listen_history(self, df: pd.DataFrame) -> None:
        """Insert to DB listen history table using pandas' .to_sql() method.
        Removes duplicate rows before inserting.
        :param df: DataFrame with Listen History records for inserting.
        """
        df_to_insert = df[[spdcol.TIMESTAMP, spdcol.USERNAME, spdcol.TRACK_ID, spdcol.PLATFORM,
                           spdcol.MS_PLAYED, spdcol.CONN_COUNTRY, spdcol.TRACK_URI,
                           spdcol.REASON_START, spdcol.REASON_END, spdcol.SHUFFLE, spdcol.OFFLINE,
                           spdcol.INCOGNITO, spdcol.SKIPPED]]

        df_to_insert = df_to_insert.rename(columns = {spdcol.TIMESTAMP: 'pk_timestamp',
                                                      spdcol.USERNAME : 'pk_username',
                                                      spdcol.TRACK_ID : 'pk_track_id',
                                                      spdcol.TRACK_URI: 'uri'})

        df_to_insert = df_to_insert.drop_duplicates(subset = ['pk_timestamp', 'pk_username', 'pk_track_id'])

        df_to_insert.to_sql(DB.TABLES.TRACKS_LISTEN_HISTORY, self.connection, if_exists = "append", index = False)


# Old logic before refactoring into a DB class:
if 0 == 1:
    # Load json data
    with open("full_tracks.json", "r") as f:
        tracks = json.load(f)

    # Connect to DB
    conn = sqlite3.connect("tracks.db")
    cur = conn.cursor()

    # Load data frame
    known_tracks = pd.read_csv("known_tracks_2022-09-17_16-50-57.csv")

    DB.insert_listen_history(known_tracks, conn)

    # Insert tracks from Json data
    for t in tracks:
        DB.insert_track_full(t, cur)

    conn.commit()
