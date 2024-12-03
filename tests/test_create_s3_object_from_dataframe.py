import pytest
import pandas as pd
import io
from src.create_s3_object_from_dataframe import create_s3_object_from_dataframe

@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "id" : [1, 2],
        "name" : ["***", "***"],
        "country" : ["UK", "India"]
    })

def test_create_s3_object_from_dataframe_returns_error_for_empty_df():
    assert create_s3_object_from_dataframe(pd.DataFrame, "csv") is None

def test_create_s3_object_from_dataframe_raises_exception_for_unsupported_file_type(sample_df):
    with pytest.raises(TypeError):
        create_s3_object_from_dataframe(sample_df, "js")

def test_create_s3_object_from_dataframe_returns_string_for_csv_file(sample_df):
    response = create_s3_object_from_dataframe(sample_df, "csv")
    assert isinstance(response, str)

def test_create_s3_object_from_dataframe_returns_string_for_json_file(sample_df):
    response = create_s3_object_from_dataframe(sample_df, "json")
    assert isinstance(response, str)

def test_create_s3_object_from_dataframe_returns_string_for_json_file(sample_df):
    response = create_s3_object_from_dataframe(sample_df, "parquet")
    assert isinstance(response, bytes)

def test_create_s3_object_from_dataframe_returns_data_same_as_passed_df(sample_df):
    response = create_s3_object_from_dataframe(sample_df, "csv")
    new_df = pd.read_csv(io.StringIO(response), index_col=[0])
    assert new_df.equals(sample_df)