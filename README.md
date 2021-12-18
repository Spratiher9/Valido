# Valido 🌩
PySpark ⚡ dataframe workflow ⚒ validator

![PyPI](https://img.shields.io/pypi/v/valido)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/valido)
![test](https://github.com/Spratiher9/Valido/workflows/Valido/badge.svg)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

## Description 

In projects using PySpark, it's very common to have functions that take Spark DataFrames as input or produce them as output.
It's hard to figure out quickly what these DataFrames contain. This library offers simple decorators to annotate your functions
so that they document themselves and that documentation is kept up-to-date by validating the input and output on runtime.

For example,

```python
@df_in(columns=["Brand", "Price"])     # the function expects a DataFrame as input parameter with columns Brand and Price
@df_out(columns=["Brand", "Price"])    # the function will return a DataFrame with columns Brand and Price
def filter_cars(car_df):
    # before this code is executed, the input DataFrame is validated according to the above decorator
    # filter some cars..
    return filtered_cars_df
```

## Table of Contents
* [Installation](#installation)
* [Usage](#usage)
* [Contributing](#contributing)
* [Tests](#tests)
* [License](#license)

## Installation

Install with your favorite Python dependency manager (_pip_) like

```sh
pip install valido
```

## Usage 

Start by importing the needed decorators:

```python
from valido import df_in, df_out
```

To check a DataFrame input to a function, annotate the function with `@df_in`. For example the following function expects to get
a DataFrame with columns `Brand` and `Price`:

```python
@df_in(columns=["Brand", "Price"])
def process_cars(car_df):
    # do stuff with cars
```

If your function takes multiple arguments, specify the field to be checked with it's `name`:

```python
@df_in(name="car_df", columns=["Brand", "Price"])
def process_cars(year, style, car_df):
    # do stuff with cars
```
_Note:_
Since this will evaluate it at runtime please use named arguments in the function call like this:
    ```
    process_cars(year = 2021, style = "Mazda", car_df = mydf) 
    ```

To check that a function returns a DataFrame with specific columns, use `@df_out` decorator:

```python
@df_out(columns=["Brand", "Price"])
def get_all_cars():
    # get those cars
    return all_cars_df
```

In case one of the listed columns is missing from the DataFrame, a helpful assertion error is thrown:

```shell
AssertionError("Column Price missing from DataFrame. Got columns: ['Brand']")
```

To check both input and output, just use both annotations on the same function:

```python
@df_in(columns=["Brand", "Price"])
@df_out(columns=["Brand", "Price"])
def filter_cars(car_df):
    # filter some cars
    return filtered_cars_df
```

If you want to also check the data types of each column, you can replace the column array:

```python
columns=["Brand", "Price"]
```

with a dict:

```python
columns={"Brand": "string", "Price": "int"}
```

This will not only check that the specified columns are found from the DataFrame but also that their `dtype` is the expected.
In case of a wrong `dtype`, an error message similar to following will explain the mismatch:

```shell
AssertionError("Column Price has wrong dtype. Was int, expected double")
```

You can enable strict-mode for both `@df_in` and `@df_out`. This will raise an error if the DataFrame contains columns
not defined in the annotation:

```python
@df_in(columns=["Brand"], strict=True)
def process_cars(car_df):
    # do stuff with cars
```

will raise an error when `car_df` contains columns `["Brand", "Price"]`:

```shell
AssertionError: DataFrame contained unexpected column(s): Price
```

To quickly check what the incoming and outgoing dataframes contain, you can add a `@df_log` annotation to the function. For
example adding `@df_log` to the above `filter_cars` function will product log lines:

```shell
Function filter_cars parameters contained a DataFrame: columns: ['Brand', 'Price']
Function filter_cars returned a DataFrame: columns: ['Brand', 'Price']
```

or with `@df_log(include_dtypes=True)` you get:

```shell
Function filter_cars parameters contained a DataFrame: columns: ['Brand', 'Price'] with dtypes ['object', 'int64']
Function filter_cars returned a DataFrame: columns: ['Brand', 'Price'] with dtypes ['object', 'int64']
```
_Note_:
    `@df_log(include_dtypes=True)` also takes the `name` parameter like `df_in` for the multi-param functions validation  

## Contributing

Contributions are accepted. Include tests in PR's.

## Development

To run the tests, clone the repository, install dependencies with _pip_ and run tests with _PyTest_:

```shell
python -m pytest --import-mode=append tests/
```

## License

BSD 3-Clause License
