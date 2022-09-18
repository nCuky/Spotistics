import json
import sqlite3
import sys
import pandas as pd


def eprint(*args, **kwargs):
    """Print to stderr."""
    print(*args, file=sys.stderr, **kwargs)


def get_track_for_insert(track: dict) -> tuple:
    """Turn a track into a tuple insertable into the tracks table."""
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

    return values_out


def insert_track(track_values: tuple, cursor: sqlite3.Cursor) -> None:
    """Insert track values to DB. Does not commit."""
    try:
        cursor.execute(f"""INSERT INTO tracks (pk_id,
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
        eprint(f"ERROR: Could not insert the following: {str(track_values)}")
        eprint(f"sqlite3.IntegrityError: {e}")
    except sqlite3.OperationalError as e:
        eprint(f"ERROR: Could not insert the following: {str(track_values)}")
        eprint(f"sqlite3.OperationalError: {e}")


def get_album_for_insert(track: dict) -> tuple:
    """Turn a track into a tuple insertable into the album table."""
    album = track['album']
    values_out = (album['id'],
                  album['href'],
                  album['uri'],
                  album['name'],
                  album['total_tracks'],
                  album['release_date'],
                  album['release_date_precision'],
                  track['id'])

    return values_out


def insert_album(track_values: tuple, cursor: sqlite3.Cursor) -> None:
    """Insert album values to DB. Does not commit."""
    try:
        cursor.execute(f"""INSERT INTO albums (pk_id,
        href,
        uri,
        name,
        total_tracks,
        release_date,
        release_date_precision,
        fk_track_id)
        VALUES {str(track_values)}""")
    except sqlite3.IntegrityError as e:
        eprint(f"ERROR: Could not insert the following: {str(track_values)}")
        eprint(f"sqlite3.IntegrityError: {e}")
    except sqlite3.OperationalError as e:
        eprint(f"ERROR: Could not insert the following: {str(track_values)}")
        eprint(f"sqlite3.OperationalError: {e}")


def get_linked_from_for_insert(track: dict) -> tuple:
    """Turn a track into a tuple insertable into the tracks_linked_from table."""
    linked = track['linked_from']

    if linked is None:
        # eprint("WARNING: linked_from attribute is null")
        values_out = (track['id'], track['id'])
    else:
        values_out = (linked['id'],
                      linked['href'],
                      linked['uri'],
                      track['id'])

    return values_out


def insert_linked_from(track_values: tuple, cursor: sqlite3.Cursor) -> None:
    """Insert linked_from values to DB. Does not commit."""
    try:
        if len(track_values) == 2:
            cursor.execute(f"""INSERT INTO tracks_linked_from (pk_id,
            fk_track_id)
            VALUES {str(track_values)}""")
        elif len(track_values) == 4:
            cursor.execute(f"""INSERT INTO tracks_linked_from (pk_id,
            href,
            uri,
            fk_track_id)
            VALUES {str(track_values)}""")
        else:
            eprint("WARNING: Wrong nubmer of values to insert. Should be either 2 or 4. Skipping")
    except sqlite3.IntegrityError as e:
        eprint(f"ERROR: Could not insert the following: {str(track_values)}")
        eprint(f"sqlite3.IntegrityError: {e}")
    except sqlite3.OperationalError as e:
        eprint(f"ERROR: Could not insert the following: {str(track_values)}")
        eprint(f"sqlite3.OperationalError: {e}")


def get_artists_album_for_insert(track: dict) -> list:
    """Turn a track into a tuple list insertable into the artists_album table."""
    artists = track['album']['artists']

    values_out = [None] * len(artists)
    for i, art in enumerate(artists):
        values_out[i] = (art['id'],
                         art['href'],
                         art['uri'],
                         art['name'],
                         track['album']['id'])

    return values_out


def insert_artists_album(artist_values: list, cursor: sqlite3.Cursor) -> None:
    """Insert artist values to DB. Does not commit."""
    for art in artist_values:
        try:
            cursor.execute(f"""INSERT INTO artists_album (pk_id,
            href,
            uri,
            name,
            fk_album_id)
            VALUES {str(art)}""")
        except sqlite3.IntegrityError as e:
            eprint(f"ERROR: Could not insert the following: {str(art)}")
            eprint(f"sqlite3.IntegrityError: {e}")
        except sqlite3.OperationalError as e:
            eprint(f"ERROR: Could not insert the following: {str(art)}")
            eprint(f"sqlite3.OperationalError: {e}")


def get_artists_track_for_insert(track: dict) -> list:
    """Turn a track into a tuple list insertable into the artists_track table."""
    artists = track['artists']

    values_out = [None] * len(artists)
    for i, art in enumerate(artists):
        values_out[i] = (art['id'],
                         art['href'],
                         art['uri'],
                         art['name'],
                         track['id'])

    return values_out


def insert_artists_track(artist_values: list, cursor: sqlite3.Cursor) -> None:
    """Insert artist values to DB. Does not commit."""
    for art in artist_values:
        try:
            cursor.execute(f"""INSERT INTO artists_track (pk_id,
            href,
            uri,
            name,
            fk_track_id)
            VALUES {str(art)}""")
        except sqlite3.IntegrityError as e:
            eprint(f"ERROR: Could not insert the following: {str(art)}")
            eprint(f"sqlite3.IntegrityError: {e}")
        except sqlite3.OperationalError as e:
            eprint(f"ERROR: Could not insert the following: {str(art)}")
            eprint(f"sqlite3.OperationalError: {e}")


def insert_track_full(track: dict, cursor: sqlite3.Cursor) -> None:
    """Insert full track record for all tables. Don't commit."""
    insert_track(get_track_for_insert(track), cursor)
    insert_album(get_album_for_insert(track), cursor)
    insert_artists_album(get_artists_album_for_insert(track), cursor)
    insert_artists_track(get_artists_track_for_insert(track), cursor)
    insert_linked_from(get_linked_from_for_insert(track), cursor)

def insert_listen_history(df: pd.DataFrame, sql_connection: sqlite3.Connection) -> None:
    """Insert to DB listen history table using pandas' .to_sql() method.
    Removes duplicate rows before inserting."""
    df_to_insert = df[['ts', 'username', 'track_id', 'platform',
                       'ms_played', 'conn_country', 'spotify_track_uri',
                       'reason_start', 'reason_end', 'shuffle', 'offline',
                       'incognito_mode', 'skipped']]

    df_to_insert = df_to_insert.rename(columns={'ts': 'pk_timestamp',
                                                'username': 'pk_username',
                                                'track_id': 'pk_track_id',
                                                'spotify_track_uri': 'uri'})

    df_to_insert = df_to_insert.drop_duplicates(subset=['pk_timestamp', 'pk_username', 'pk_track_id'])

    df_to_insert.to_sql("tracks_listen_history", conn, if_exists="append", index=False)


# Load json data
with open("full_tracks.json", "r") as f:
    tracks = json.load(f)

# Connect to DB
conn = sqlite3.connect("tracks.db")
cur = conn.cursor()

# Load data frame
known_tracks = pd.read_csv("known_tracks_2022-09-17_16-50-57.csv")

insert_listen_history(known_tracks, conn)

# Insert tracks from Json data
for t in tracks:
    insert_track_full(t, cur)

conn.commit()
