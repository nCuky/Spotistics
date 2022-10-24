import datetime as dt
import traceback

# File operations
READING_FILE = "Now reading file: {0}..."
NONEXISTENT_FILE = "File doesn't exist: {0}"
WRITING_FILE = "Now writing file: {0}..."
FILE_WRITTEN = "File written successfully: {0}"

# Fetching:
FETCHING_ATTRS_FOR = "Fetching {0} attributes for {1} {2} (might take a while)..."
ATTRS_FETCHED_FOR = "{0} attributes were successfully fetched for {1} {2}."

FETCHING_TRACKS_ATTRS = FETCHING_ATTRS_FOR.format('FullTrack', '{0}', 'tracks')
TRACKS_ATTRS_FETCHED = ATTRS_FETCHED_FOR.format('FullTrack', '{0}', 'tracks')
FETCHING_ARTISTS_ATTRS = FETCHING_ATTRS_FOR.format('FullArtist', '{0}', 'artists')
ARTISTS_ATTRS_FETCHED = ATTRS_FETCHED_FOR.format('FullArtist', '{0}', 'artists')
FETCHING_ALBUMS_ATTRS = FETCHING_ATTRS_FOR.format('FullAlbum', '{0}', 'albums')
ALBUMS_ATTRS_FETCHED = ATTRS_FETCHED_FOR.format('FullAlbum', '{0}', 'albums')
FETCHING_ARTISTS_ALBUMS_ATTRS = "Fetching all albums for {0} artists (might take a while)..."
ARTISTS_ALBUMS_ATTRS_FETCHED = "All albums were successfully fetched for {0} artists."
FETCHING_ARTISTS_TRACKS_ATTRS = "Fetching all tracks for {0} artists (might take a while)..."
ARTISTS_TRACKS_ATTRS_FETCHED = "All tracks were successfully fetched for {0} artists."
FETCHING_RECENTLY_PLAYED = "Fetching the current user's Recently Played Tracks..."
RECENTLY_PLAYED_FETCHED = "The current user's Recently Played Tracks were successfully fetched."
FETCHING_LISTEN_HISTORY = "Fetching the listen history..."
LISTEN_HISTORY_FETCHED = "Listen history was successfully fetched."

# Inserting:
INSERTING_RECORD = "Inserting a single record into DB-table {0}..."
RECORD_INSERTED = "The record was successfully inserted."
INSERTING_RECORDS = "Inserting {1} records into DB-table {0}..."
RECORDS_INSERTED = "All records were successfully inserted."

# API Errors:
API_SERVICE_UNAVAILABLE = 'Spotify API Service is unavailable. Original error: {0}'
API_SERVICE_UNAUTHORIZED = """You are not authorized for the desired API operation. Maybe your Token has expired, or 
the requested authorization scope is not sufficient. Original error: {0}"""

# DB Errors:
EMPTY_VALUES = 'No {0} values were given, so no DB-action was performed.'
CANNOT_INSERT = 'ERROR: Could not insert the following: {0}'
DB_SCHEMA_ERROR = 'ERROR in DB Schema script.'
DB_INTEGRITY_ERROR = 'sqlite3.IntegrityError: {0}'
DB_OPERATIONAL_ERROR = 'sqlite3.OperationalError: {0}'


def write(message: str) -> None:
    trace = traceback.extract_stack()
    last_call = next(item.name for item in trace if item.line.startswith('log.write('))

    formatted_msg = "LOG: {0}\t: \tin: {1}\t:\t{2}".format(str(dt.datetime.now()), last_call, message)

    print(formatted_msg)
