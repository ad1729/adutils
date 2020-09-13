from pandas import DataFrame as _pandasDF
from pyspark.sql import DataFrame as _sparkDF
from typing import Union as _Union

__all__ = ["has_missing"]


def _has_missing_spark_df(df: _sparkDF,
                          return_missing: bool) -> _Union[bool, _sparkDF]:

    clean_df = df.dropna(how="any")
    nrows_clean = clean_df.count()
    nrows_all = df.count()

    if nrows_clean is not nrows_all:
        if return_missing:
            return df.subtract(clean_df)
        else:
            return True
    else:
        return False


def _has_missing_pandas_df(df: _pandasDF,
                           return_missing: bool) -> _Union[bool, _pandasDF]:

    missing = df[df.isnull().any(axis=1)]

    if missing.shape[0] is not 0:
        if return_missing:
            return missing
        else:
            return True
    else:
        return False


def has_missing(df: _Union[_pandasDF, _sparkDF],
                return_missing: bool = False) -> _Union[bool, _pandasDF, _sparkDF]:
    """Function
    1) to test whether a (pandas/spark) dataframe has missing values, and
    2) to return the rows with missing values for inspection.

    :param df: A pandas or spark dataframe.
    :param return_missing: Defaults to False. Whether to return subset of rows with missing values or not.
    :return: Either True/False if rows with missingness are not requested, or return a dataframe with rows that have
        missing values.
    :raises: TypeError: If the df is not a pandas or a spark dataframe or if return_missing is not a boolean.
    """
    # TODO: add support for checking missingness in a subset of columns
    if not isinstance(return_missing, bool):
        raise TypeError(f"return_missing should be True/False but is {type(return_missing)}")

    if isinstance(df, _pandasDF):
        return _has_missing_pandas_df(df, return_missing)
    elif isinstance(df, _sparkDF):
        return _has_missing_spark_df(df, return_missing)
    else:
        raise TypeError(f"Passed object should either be a pandas or a spark dataframe, not of type 'f{type(df)}'")
