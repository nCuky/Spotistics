import datetime as dt
import traceback

# File operations
READING_FILE = "Now reading file: {0}..."
NONEXISTENT_FILE = "File doesn't exist: {0}"
WRITING_FILE = "Now writing file: {0}..."
FILE_WRITTEN = "File written successfully: {0}"

# Fetching:
FETCHING_TRACKS_ATTRS = "Fetching FullTrack attributes for {0} tracks (might take a while)..."
TRACKS_ATTRS_FETCHED = "FullTrack attributes were successfully fetched for {0} tracks."
FETCHING_ALBUMS_ATTRS = "Fetching FullAlbum attributes for {0} albums (might take a while)..."
ALBUMS_ATTRS_FETCHED = "FullAlbum attributes were successfully fetched for {0} albums."
FETCHING_ARTISTS_ALBUMS_ATTRS = "Fetching all albums for {0} artists (might take a while)..."
ARTISTS_ALBUMS_ATTRS_FETCHED = "All albums were successfully fetched for {0} artists."
FETCHING_RECENTLY_PLAYED = "Fetching the current user's Recently Played Tracks..."
RECENTLY_PLAYED_FETCHED = "The current user's Recently Played Tracks were successfully fetched."

# API Errors:
API_SERVICE_UNAVAILABLE = 'Spotify API Service is unavailable. Original error: {0}'

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
