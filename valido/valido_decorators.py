"""Decorators for VALIDO DataFrame Column Validator."""

import logging
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Union

from pyspark.sql import DataFrame

ColumnsDef = Union[List, Dict]


def _check_columns(df: DataFrame, columns: ColumnsDef, strict: bool) -> None:
    if isinstance(columns, list):
        for column in columns:
            assert column in df.columns, f"Column {column} missing from DataFrame. Got {_describe_df(df)}"
    if isinstance(columns, dict):
        col_dtype_dict = dict(df.dtypes)
        for column, dtype in columns.items():
            assert column in df.columns, f"Column {column} missing from DataFrame. Got {_describe_df(df)}"
            assert (
                    col_dtype_dict[column] == dtype
            ), f"Column {column} has wrong dtype. Was {col_dtype_dict[column]}, expected {dtype}"
    if strict:
        assert len(df.columns) == len(
            columns
        ), f"DataFrame contained unexpected column(s): {', '.join(set(df.columns) - set(columns))}"


def df_out(columns: Optional[ColumnsDef] = None, strict: bool = False) -> Callable:
    """Decorate a function that returns a Spark DataFrame.

    Document the return value of a function. The return value will be validated in runtime.

    Args:
        columns (ColumnsDef, optional): List or dict that describes expected columns of the DataFrame. Defaults to None.
        strict (bool, optional): If True, columns must match exactly with no extra columns. Defaults to False.

    Returns:
        Callable: Decorated function
    """

    def wrapper_df_out(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: str, **kwargs: Any) -> Any:
            result = func(*args, **kwargs)
            assert isinstance(result, DataFrame), f"Wrong return type. Expected spark dataframe, got {type(result)}"
            if columns:
                _check_columns(result, columns, strict)
            return result

        return wrapper

    return wrapper_df_out


def _get_parameter(name: Optional[str] = None, *args: str, **kwargs: Any) -> DataFrame:
    if not name:
        if len(args) == 0:
            return None
        return args[0]
    return kwargs[name]


def df_in(name: Optional[str] = None, columns: Optional[ColumnsDef] = None, strict: bool = False) -> Callable:
    """Decorate a function parameter that is a Spark DataFrame.

    Document the contents of an input parameter. The parameter will be validated in runtime.

    Args:
        name (Optional[str], optional): Name of the parameter that contains a DataFrame. Defaults to None.
        columns (ColumnsDef, optional): List or dict that describes expected columns of the DataFrame. Defaults to None.
        strict (bool, optional): If True, columns must match exactly with no extra columns. Defaults to False.

    Returns:
        Callable: Decorated function
    """

    def wrapper_df_out(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: str, **kwargs: Any) -> Any:
            df = _get_parameter(name, *args, **kwargs)
            assert isinstance(
                df, DataFrame
            ), f"Wrong parameter type. Expected Spark DataFrame, got {type(df).__name__} instead."
            if columns:
                _check_columns(df, columns, strict)
            return func(*args, **kwargs)

        return wrapper

    return wrapper_df_out


def _describe_df(df: DataFrame, include_dtypes: bool = False) -> str:
    result = f"columns: {list(df.columns)}"
    if include_dtypes:
        readable_dtypes = [element[1] for element in df.dtypes]
        result += f" with dtypes {readable_dtypes}"
    return result


def _log_input(func_name: str, df: Any, include_dtypes: bool) -> None:
    print("log_input\n", df)
    if isinstance(df, DataFrame):
        print(f"Function {func_name} parameters contained a DataFrame: {_describe_df(df, include_dtypes)}")


def _log_output(func_name: str, df: Any, include_dtypes: bool) -> None:
    if isinstance(df, DataFrame):
        print(f"Function {func_name} returned a DataFrame: {_describe_df(df, include_dtypes)}")


def df_log(name: Optional[str] = None, include_dtypes: bool = False) -> Callable:
    """Decorate a function that consumes or produces a Spark DataFrame or both.

    Logs the columns of the consumed and/or produced DataFrame.

    Args:
        include_dtypes (bool, optional): When set to True, will log also the dtypes of each column. Defaults to False.

    Returns:
        Callable: Decorated function.
    """

    def wrapper_df_log(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: str, **kwargs: Any) -> Any:
            _log_input(func.__name__, _get_parameter(name, *args, **kwargs), include_dtypes)
            result = func(*args, **kwargs)
            _log_output(func.__name__, result, include_dtypes)

        return wrapper

    return wrapper_df_log
