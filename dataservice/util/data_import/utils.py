import json
import time
import yaml
from pandas import notnull
from pathlib import Path

ARCHIVE_COMPRESSION_FORMATS = \
    {'.7z', '.??_', '.?Q?', '.?XF', '.?Z?', '.F', '.LBR', '.Z', '.a', '.ace',
     '.afa', '.alz', '.apk', '.ar', '.arc', '.arj', '.b1', '.b6z', '.ba',
     '.bh', '.bz2', '.cab', '.car', '.cfs', '.cpio', '.cpt', '.dar', '.dd',
     '.dgc', '.dmg', '.ear', '.gca', '.gz', '.ha', '.hki', '.ice', '.iso',
     '.jar', '.kgb', '.lbr', '.lha', '.lz', '.lzh', '.lzma', '.lzo', '.lzx',
     '.mar', '.pak', '.paq6', '.paq7', '.paq8', '.partimg', '.pea', '.pim',
     '.pit', '.qda', '.rar', '.rk', '.rz', '.s7z', '.sbx', '.sda', '.sea',
     '.sen', '.sfark', '.sfx', '.shar', '.shk', '.sit', '.sitx', '.sqx',
     '.sz', '.tar', '.tar.Z', '.tar.bz2', '.tar.gz', '.tar.lzma', '.tar.xz',
     '.tbz2', '.tgz', '.tlz', '.txz', '.uc', '.uc0', '.uc2', '.uca', '.ucn',
     '.ue2', '.uha', '.ur2', '.war', '.wim', '.xar', '.xp3', '.xz', '.yz1',
     '.z', '.zip', '.zipx', '.zoo', '.zpaq', '.zz'}


def extract_uncompressed_file_ext(filepath,
                                  ext_list=ARCHIVE_COMPRESSION_FORMATS):
    """
    Extract the file ext of a filepath with compression/archive extensions

    Example - myfile.ppt.tar.gz will result in the extension ppt
    """
    exts = Path(filepath).suffixes
    for ext in reversed(exts):
        if ext in ext_list:
            filepath = filepath.rstrip(ext)
        else:
            break
    return filepath.split('.')[-1]


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


def read_json(filepath):
    with open(filepath, 'r') as json_file:
        return json.load(json_file)


def write_json(data, filepath):
    with open(filepath, 'w') as json_file:
        json.dump(data, json_file, sort_keys=True, indent=4, separators=(',', ':'))
