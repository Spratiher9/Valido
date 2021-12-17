import logging
from typing import Any
from unittest.mock import call
from pyspark.shell import spark
from pyspark.sql import DataFrame
import pandas as pd
import pytest
from pytest_mock import MockerFixture

from valido import df_in, df_log, df_out


@pytest.fixture
def basic_df() -> DataFrame:
    cars = [('Honda Civic', 22000), ('Toyota Corolla', 25000), ('Ford Focus', 27000), ('Audi A4', 35000)]
    schema = "Brand string, Price int"
    return spark.createDataFrame(cars, schema)


@pytest.fixture
def extended_df() -> DataFrame:
    cars = [('Honda Civic', 22000, 2020), ('Toyota Corolla', 25000, 1998), ('Ford Focus', 27000, 2001), ('Audi A4', 35000, 2021)]
    schema = "Brand string, Price int, Year int"
    return spark.createDataFrame(cars, schema)


def test_wrong_return_type() -> None:
    @df_out()
    def test_fn() -> int:
        return 1

    with pytest.raises(AssertionError) as excinfo:
        test_fn()

    assert "Wrong return type" in str(excinfo.value)


def test_correct_return_type_and_no_column_constraints(basic_df: DataFrame) -> None:
    @df_out()
    def test_fn() -> DataFrame:
        return basic_df

    test_fn()


def test_correct_return_type_and_columns(basic_df: DataFrame) -> None:
    @df_out(columns=["Brand", "Price"])
    def test_fn() -> DataFrame:
        return basic_df

    test_fn()


def test_allow_extra_columns_out(basic_df: DataFrame) -> None:
    @df_out(columns=["Brand"])
    def test_fn() -> DataFrame:
        return basic_df

    test_fn()


def test_correct_return_type_and_columns_strict(basic_df: DataFrame) -> None:
    @df_out(columns=["Brand", "Price"], strict=True)
    def test_fn() -> DataFrame:
        return basic_df

    test_fn()


def test_extra_column_in_return_strict(basic_df: DataFrame) -> None:
    @df_out(columns=["Brand"], strict=True)
    def test_fn() -> DataFrame:
        return basic_df

    with pytest.raises(AssertionError) as excinfo:
        test_fn()

    assert "DataFrame contained unexpected column(s): Price" in str(excinfo.value)


def test_missing_column_in_return(basic_df: DataFrame) -> None:
    @df_out(columns=["Brand", "FooColumn"])
    def test_fn() -> DataFrame:
        return basic_df

    with pytest.raises(AssertionError) as excinfo:
        test_fn()

    assert "Column FooColumn missing" in str(excinfo.value)


def test_wrong_input_type_unnamed() -> None:
    @df_in()
    def test_fn(my_input: Any) -> Any:
        return my_input

    with pytest.raises(AssertionError) as excinfo:
        test_fn("foobar")

    assert "Wrong parameter type" in str(excinfo.value)


def test_wrong_input_type_named() -> None:
    @df_in(name="my_input")
    def test_fn(my_input: Any) -> Any:
        return my_input

    with pytest.raises(AssertionError) as excinfo:
        test_fn(my_input="foobar")

    assert "Wrong parameter type. Expected Spark DataFrame, got str instead." in str(excinfo.value)


def test_correct_input_with_columns(basic_df: DataFrame) -> None:
    @df_in(columns=["Brand", "Price"])
    def test_fn(my_input: Any) -> Any:
        return my_input

    test_fn(basic_df)


def test_correct_input_with_no_column_constraints(basic_df: DataFrame) -> None:
    @df_in()
    def test_fn(my_input: Any) -> Any:
        return my_input

    test_fn(basic_df)


def test_dfin_with_no_inputs() -> None:
    @df_in()
    def test_fn() -> Any:
        return

    with pytest.raises(AssertionError) as excinfo:
        test_fn()

    assert "Wrong parameter type. Expected Spark DataFrame, got NoneType instead." in str(excinfo.value)


def test_correct_named_input_with_columns(basic_df: DataFrame) -> None:
    @df_in(name="df", columns=["Brand", "Price"])
    def test_fn(my_input: Any, df: DataFrame) -> DataFrame:
        return df

    test_fn("foo", df=basic_df)


def test_correct_named_input_with_columns_strict(basic_df: DataFrame) -> None:
    @df_in(name="df", columns=["Brand", "Price"], strict=True)
    def test_fn(my_input: Any, df: DataFrame) -> DataFrame:
        return df

    test_fn("foo", df=basic_df)


def test_in_allow_extra_columns(basic_df: DataFrame) -> None:
    @df_in(name="df", columns=["Brand"])
    def test_fn(my_input: Any, df: DataFrame) -> DataFrame:
        return df

    test_fn("foo", df=basic_df)


def test_in_strict_extra_columns(basic_df: DataFrame) -> None:
    @df_in(name="df", columns=["Brand"], strict=True)
    def test_fn(my_input: Any, df: DataFrame) -> DataFrame:
        return df

    with pytest.raises(AssertionError) as excinfo:
        test_fn("foo", df=basic_df)

    assert "DataFrame contained unexpected column(s): Price" in str(excinfo.value)


def test_correct_input_with_columns_and_dtypes(basic_df: DataFrame) -> None:
    @df_in(columns={"Brand": "string", "Price": "int"})
    def test_fn(my_input: Any) -> Any:
        return my_input

    test_fn(basic_df)


def test_dtype_mismatch(basic_df: DataFrame) -> None:
    @df_in(columns={"Brand": "string", "Price": "float"})
    def test_fn(my_input: Any) -> Any:
        return my_input

    with pytest.raises(AssertionError) as excinfo:
        test_fn(basic_df)

    assert "Column Price has wrong dtype. Was int, expected float" in str(excinfo.value)


def test_df_in_incorrect_input(basic_df: DataFrame) -> None:
    @df_in(columns=["Brand", "Price"])
    def test_fn(my_input: Any) -> Any:
        return my_input

    with pytest.raises(AssertionError) as excinfo:
        test_fn(basic_df[["Brand"]])
    assert "Column Price missing" in str(excinfo.value)


def test_df_out_with_df_modification(basic_df: DataFrame, extended_df: DataFrame) -> None:
    @df_out(columns=["Brand", "Price", "Year"])
    def test_fn(my_input: Any) -> Any:
        result = my_input.join(extended_df, ["Brand","Price"], "inner")
        return result

    assert list(basic_df.columns) == ["Brand", "Price"]  # For sanity
    pd.testing.assert_frame_equal(extended_df.orderBy("Brand","Price").toPandas(),
                                  test_fn(basic_df).orderBy("Brand","Price").toPandas())


def test_decorator_combinations(basic_df: DataFrame, extended_df: DataFrame) -> None:
    @df_in(columns=["Brand", "Price"])
    @df_out(columns=["Brand", "Price", "Year"])
    def test_fn(my_input: Any) -> Any:
        result = my_input.join(extended_df, ["Brand","Price"], "inner")
        return result

    pd.testing.assert_frame_equal(extended_df.orderBy("Brand","Price").toPandas(),
                                  test_fn(basic_df).orderBy("Brand","Price").toPandas())


def test_log_df(basic_df: DataFrame, mocker: MockerFixture) -> None:
    @df_log()
    def test_fn(foo_df: DataFrame) -> DataFrame:
        return basic_df

    mock_log = mocker.patch("builtins.print")
    test_fn(basic_df)

    mock_log.assert_has_calls(
        [
            call(
                ("Function test_fn parameters contained a DataFrame: columns: ['Brand', 'Price']"),
            ),
            call(
                "Function test_fn returned a DataFrame: columns: ['Brand', 'Price']",
            ),
        ]
    )
