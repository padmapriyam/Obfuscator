import pytest
import logging
import pandas as pd
from unittest.mock import patch
from src.get_and_obfuscate_s3_file import (
    get_and_obfuscate_s3_file,
    InvalidKeyError,
    InvalidInputError,
    UnsupportedFileTypeError,
)


@pytest.fixture
def s3_details():
    s3_string = '{ "file_to_obfuscate": "s3://my_ingestion_bucket/new_data/file1.json", "pii_fields": ["name", "email"] }'
    return s3_string


def test_get_and_obfuscate_s3_file_returns_exception_when_file_not_passed():
    s3_string = '{"file_to_obfuscate": "", "pii_fields": ["name", "email"]}'
    with pytest.raises(InvalidInputError) as excinfo:
        get_and_obfuscate_s3_file(s3_string)
    assert str(excinfo.value) == "No file provided to obfuscate"


def test_get_and_obfuscate_s3_file_returns_exception_with_key_not_present():
    s3_string = '{ "file_to_obfuscate": "s3://my_ingestion_bucket/new_data/file1.csv" }'
    with pytest.raises(InvalidInputError) as excinfo:
        get_and_obfuscate_s3_file(s3_string)
    assert str(excinfo.value) == "No fields provided to obfuscate"


def test_get_and_obfuscate_s3_file_returns_exception_with_empty_list_for_fields():
    s3_string = '{ "file_to_obfuscate": "s3://my_ingestion_bucket/new_data/file1.csv", "pii_fields": [] }'
    with pytest.raises(InvalidInputError) as excinfo:
        get_and_obfuscate_s3_file(s3_string)
    assert str(excinfo.value) == "No fields provided to obfuscate"


def test_get_and_obfuscate_s3_file_returns_exception_when_file_format_not_supported():
    s3_string = '{ "file_to_obfuscate": "s3://my_ingestion_bucket/new_data/file1.jpeg", "pii_fields": ["name", "email"] }'
    with pytest.raises(UnsupportedFileTypeError) as excinfo:
        get_and_obfuscate_s3_file(s3_string)
    assert (
        str(excinfo.value)
        == "Only csv, json and parquet files supported for obfuscation"
    )


def test_get_and_obfuscate_s3_file_logs_error_read_data(caplog, s3_details):
    caplog.set_level(logging.ERROR)
    with patch(
        "src.get_and_obfuscate_s3_file.read_s3_object_into_dataframe"
    ) as mock_read_s3_object:
        mock_read_s3_object.side_effect = Exception("read data error")
        get_and_obfuscate_s3_file(s3_details)
    assert "Error reading file file1.json from bucket new_data" in caplog.text


def test_get_and_obfuscate_s3_file_returns_none_if_no_records_exist_in_file(s3_details):
    with patch(
        "src.get_and_obfuscate_s3_file.obfuscate_fields"
    ) as mock_obfuscate_fields:
        mock_obfuscate_fields.return_value = None
        assert get_and_obfuscate_s3_file(s3_details) is None


def test_get_and_obfuscate_s3_file_logs_error_df_to_s3(caplog, s3_details):
    caplog.set_level(logging.ERROR)
    with (
        patch(
            "src.get_and_obfuscate_s3_file.read_s3_object_into_dataframe"
        ) as mock_read_s3_object,
        patch(
            "src.get_and_obfuscate_s3_file.obfuscate_fields"
        ) as mock_obfuscate_fields,
        patch(
            "src.get_and_obfuscate_s3_file.create_s3_object_from_dataframe"
        ) as mock_create_s3_object,
    ):
        mock_read_s3_object.return_value = pd.DataFrame()
        mock_create_s3_object.side_effect = Exception("convert error")
        get_and_obfuscate_s3_file(s3_details)
    assert "Error converting file file1.json from bucket new_data" in caplog.text
