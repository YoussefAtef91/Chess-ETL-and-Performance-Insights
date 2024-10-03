import pandas as pd
import numpy as np
from typing import Dict, List

def values_check(df: pd.DataFrame, col: str, minval: float = -np.inf, maxval: float = np.inf) -> None:
    """
    Check if column values fall within a specified range.
    """
    if df[(df[col] < minval) | (df[col] > maxval)].any().any():
        raise ValueError(f"Column {col} has a value error")

def nulls_check(df: pd.DataFrame) -> None:
    """
    Check if the DataFrame contains any null values.
    """
    if df.isnull().any().any():
        raise ValueError("DataFrame contains null values")

def datatypes_check(df: pd.DataFrame, col_dict: Dict[str, str]) -> None:
    """
    Check if the DataFrame columns have the expected data types.
    """
    for col, dtype in col_dict.items():
        if not pd.api.types.is_dtype_equal(df[col].dtype, dtype):
            raise TypeError(f"Column '{col}' expected dtype {dtype}, but got {df[col].dtype}")

def game_url_check(df: pd.DataFrame) -> None:
    """
    Check if all GameUrl values start with 'https://'.
    """
    if not df['GameUrl'].str.startswith('https://').all():
        raise ValueError("Bad GameUrl format")

def possible_values_check(df: pd.DataFrame, col: str, values: List[str]) -> None:
    """
    Check if column values belong to a set of valid values.
    """
    if not df[col].isin(values).all():
        raise ValueError(f"Column {col} contains invalid values")

def columns_check(actual_columns: List[str], desired_columns: List[str], strict: bool = True) -> None:
    """
    Check if the DataFrame columns match the expected set of columns.
    """
    if strict:
        if not sorted(set(actual_columns)) == sorted(set(desired_columns)):
            raise TypeError("Fields are not exactly the same")
    else:
        if not set(actual_columns) <= set(desired_columns):
            raise TypeError("Some fields don't exist")