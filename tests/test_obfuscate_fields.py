import pandas as pd
import pytest
import logging
from src.obfuscate_fields import obfuscate_fields


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {"id": ["1", "2"], "name": ["person1", "person2"], "country": ["UK", "India"]}
    )


def test_obfuscate_fields_returns_none_when_passed_an_empty_df_and_displays_a_message(
    caplog,
):
    df = obfuscate_fields(pd.DataFrame, ["name"])
    assert df is None
    with caplog.at_level(logging.ERROR):
        obfuscate_fields(pd.DataFrame, ["name", "email"])
    assert "No records present in the input file" in caplog.text


def test_obfuscate_fields_returns_a_dataframe(sample_df):
    df = obfuscate_fields(sample_df, ["name"])
    assert isinstance(df, pd.DataFrame)


def test_obfuscate_fields_returns_df_with_same_columns(sample_df):
    df = obfuscate_fields(sample_df, ["name"])
    assert list(df.columns) == ["id", "name", "country"]
    assert len(df) == 2


def test_obfuscate_fields_returns_df_with_one_col_replaced(sample_df):
    df = obfuscate_fields(sample_df, ["name"])
    assert (df["name"] == "***").all()


def test_obfuscate_fields_returns_df_with_more_cols_replaced(sample_df):
    df = obfuscate_fields(sample_df, ["name", "id"])
    assert (df["name"] == "***").all()
    assert (df["id"] == "***").all()


def test_obfuscate_fields_returns_df_with_some_cols_replaced(sample_df):
    df = obfuscate_fields(sample_df, ["name", "email"])
    assert (df["name"] == "***").all()


def test_obfuscate_fields_returns_df_with_some_cols_not_present_with_logs(
    sample_df, caplog
):
    with caplog.at_level(logging.ERROR):
        obfuscate_fields(sample_df, ["name", "email"])
    assert "Field 'email' not available in the input file" in caplog.text
