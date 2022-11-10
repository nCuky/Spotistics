import pandas as pd
import log
from datetime import datetime as dt
# from typing import Set, List, Dict

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


def get_unique_vals_list(values: str | pd.Series | set | list) -> list:
    """
    Keep only the unique values from a given collection.

    Parameters:
        values: When a string is supplied, it is treated as a single value.
            Otherwise, ``values`` is treated as a collection of values, from which to take the unique ones.

    Returns:
        List with the unique values.
            When a single value is supplied, returns a list with the supplied value as its only item.
    """
    match values:
        case str() as values:
            unique_values = [values]

        case pd.Series() as values:
            unique_values = values.dropna().unique().tolist()

        case set() as values:
            unique_values = list(values)

        case list() as values:
            unique_values = list(set(values))

        case other:
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


@staticmethod
def add_to_mapping_of_sets(mapping: dict,
                           key: str,
                           value: str) -> None:
    """
    In a given dictionary of sets, searches for the required key, and adds the given string value as the next item
    to that key.
    If the key is not found, it is created, and for it a new set with the given value as its first item is put.

    This **changes the given mapping** (inplace = True).

    Parameters:
        mapping: Dictionary, mapping each key to a set of string values.

        key: String used as the key for the dict.

        value: String value to add to the dict - as the first value in a new set, or as another value in an existing
            set.

    Returns:
        None (the given mapping is changed).
    """
    if mapping.get(key) is None:
        mapping[key] = set()

    mapping[key].add(value)
