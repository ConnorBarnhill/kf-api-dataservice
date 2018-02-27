import time
import yaml
from pandas import notnull


def dropna_rows_cols(df_func):
    """
    Decorator to drop rows and cols w all nan values and replace
    any NaN values with None
    """

    def wrapper(*args, **kwargs):
        df = df_func(*args, **kwargs)

        # None or empty df
        try:
            if df.empty:
                print('Cannot perform dropna_rows_cols since df is empty')
                return df
        except AttributeError:
            print('Cannot perform dropna_rows_cols since df is None')
            return df

        # Rows
        df.dropna(how="all", inplace=True)
        # Cols
        df.dropna(how="all", axis=1, inplace=True)
        # Replace NaN values with None
        df = df.where((notnull(df)), None)

        return df

    return wrapper


def reformat_column_names(df_func):
    """
    Decorator to reformat DataFrame column names.

    Replace all column names having whitespace with underscore
    and make lowercase
    """

    def wrapper(*args, **kwargs):
        df = df_func(*args, **kwargs)
        # None or empty df
        try:
            if df.empty:
                print('Cannot perform reformat_column_names since df is empty')
                return df
        except AttributeError:
            print('Cannot perform reformat_column_names since df is None')
            return df

        cols_to_lower(df)

        return df

    return wrapper


def cols_to_lower(df):
    df.columns = map((lambda x: x.replace(" ", "_").lower()), df.columns)


def to_camel_case(snake_str):
    """
    Convert snake case str to camel case
    """
    words = snake_str.split('_')
    return ''.join([w.title() for w in words])


def time_it(func):
    """
    Decorator to time the function
    """

    def wrapper(*args, **kwargs):
        start = time.time()

        r = func(*args, **kwargs)

        end = time.time()

        delta_sec = end - start
        print("Time elapsed \nSec: {}\nMin: {}\nHours: {}".format(
            delta_sec, delta_sec / 60, delta_sec / 60 / 60))
        return r

    return wrapper


def read_yaml(filepath):
    with open(filepath, 'r') as yaml_file:
        return yaml.load(yaml_file)
