
def dropna_rows_cols(df_func):
    """
    Decorator to drop rows and cols w all nan values
    """

    def wrapper(*args, **kwargs):
        df = df_func(*args, **kwargs)

        # None or empty df
        try:
            if df.empty:
                return df
        except AttributeError:
            return df

        # Rows
        df.dropna(how="all", inplace=True)
        # Cols
        df.dropna(how="all", axis=1, inplace=True)
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
                return df
        except AttributeError:
            return df
        df.columns = map((lambda x: x.replace(" ", "_").lower()),
                         df.columns)
        return df

    return wrapper
