import datetime as dt
import traceback

READING_FILE = "Now reading file: {0}"
NONEXISTENT_FILE = "File doesn't exist: {0}"
WRITING_FILE = "Now writing file: {0}"
FILE_WRITTEN = "File written successfully: {0}"


def write(message: str) -> None:
    trace = traceback.extract_stack()
    last_call = next(item.name for item in trace if item.line.startswith('log.write('))

    formatted_msg = "LOG: {0} --- in: {1} --- {2}".format(str(dt.datetime.now()), last_call, message)

    print(formatted_msg)
