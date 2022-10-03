import datetime as dt
import traceback

READING_FILE = "Now reading file: {0}..."
NONEXISTENT_FILE = "File doesn't exist: {0}"
WRITING_FILE = "Now writing file: {0}..."
FILE_WRITTEN = "File written successfully: {0}"
FETCHING_TRACKS_ATTRS = "Fetching FullTrack attributes for {0} tracks (might take a while)..."
TRACKS_ATTRS_FETCHED = "FullTrack attributes were successfully fetched for {0} tracks."
API_SERVICE_UNAVAILABLE = 'Spotify API Service is unavailable. Original error: {0}'
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
