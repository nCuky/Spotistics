import pandas as pd
import log
from datetime import datetime as dt


@staticmethod
def write_df_to_file(df: pd.DataFrame, file_name: str) -> None:
    """
    Writes a given DataFrame to a file.

    Parameters:
        df: DataFrame to write to a file.

        file_name: Name of the desired file to write (without preceding path).

    Returns:
        None.
    """
    file_path = 'data/personal_data/prepared/' + file_name.format(dt.now().strftime("%Y-%m-%d_%H-%M-%S"))
    log.write(log.WRITING_FILE.format(file_path))
    # Writing an Excel Spreadsheet doesn't work yet.
    df.to_csv(path_or_buf = file_path,
              encoding = 'utf-8-sig',  # UTF-8, explicitly signed with a BOM at the start of the file
              index = False)
    log.write(log.FILE_WRITTEN.format(file_path))


def get_unique_vals_list(values: pd.Series | set | list) -> list:
    """
    Keep only the unique values from a given collection.

    Parameters:
        values: Collection of values, from which to take the unique ones.

    Returns:
        List with the unique values.
    """
    match values:
        case pd.Series() as values:
            unique_values = values.dropna().unique().tolist()

        case set() as values:
            unique_values = list(values)

        case list() as values:
            unique_values = values

        case _:
            unique_values = []

    return unique_values


@staticmethod
def get_unique_dicts(dicts: list[dict]) -> list[dict] | None:
    """
    Keep only the unique **dictionaries** in a list of dicts. Each whole dict is taken as a single "value" to
    compare to others.

    Each dict in the given list **must be flat**, i.e. not an object or a nested dict, because
    they are not supported here.

    Parameters:
        dicts (list[dict]): List of flat dictionaries.

    Returns:
        list[dict] | None: List of only the unique dictionaries, compared by all the values of each dict,
            or None if any dict contains an object or a nested dict.
    """
    # Taken from https://stackoverflow.com/a/19804098/6202667
    unique_dict_list = list(map(dict, set(tuple(d.items()) for d in dicts)))

    return unique_dict_list
