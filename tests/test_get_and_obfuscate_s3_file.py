import pytest
from src.get_and_obfuscate_s3_file import (
    get_and_obfuscate_s3_file,
    InvalidInputError,
    UnsupportedFileTypeError,
)


def test_get_and_obfuscate_s3_file_returns_exception_when_file_not_passed():
    s3_details = {"file_to_obfuscate": None, "pii_fields": ["name", "email"]}
    with pytest.raises(InvalidInputError) as excinfo:
        get_and_obfuscate_s3_file(s3_details)
    assert str(excinfo.value) == "No file/fields provided to obfuscate"


def test_get_and_obfuscate_s3_file_returns_exception_with_no_fields():
    s3_details = {
        "file_to_obfuscate": "s3://my_ingestion_bucket/new_data/file1.csv",
        "pii_fields": None,
    }
    with pytest.raises(InvalidInputError) as excinfo:
        get_and_obfuscate_s3_file(s3_details)
    assert str(excinfo.value) == "No file/fields provided to obfuscate"


def test_get_and_obfuscate_s3_file_returns_exception_with_empty_list_for_fields():
    s3_details = {
        "file_to_obfuscate": "s3://my_ingestion_bucket/new_data/file1.csv",
        "pii_fields": [],
    }
    with pytest.raises(InvalidInputError) as excinfo:
        get_and_obfuscate_s3_file(s3_details)
    assert str(excinfo.value) == "No file/fields provided to obfuscate"


def test_get_and_obfuscate_s3_file_returns_exception_when_file_format_not_supported():
    s3_details = {
        "file_to_obfuscate": "s3://my_ingestion_bucket/new_data/file1.jpeg",
        "pii_fields": ["name", "email"],
    }
    with pytest.raises(UnsupportedFileTypeError) as excinfo:
        get_and_obfuscate_s3_file(s3_details)
    assert (
        str(excinfo.value)
        == "Only csv, json and parquet files supported for obfuscation"
    )
