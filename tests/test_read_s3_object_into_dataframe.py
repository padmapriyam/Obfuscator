import pytest
import pandas as pd
import boto3
from moto import mock_aws
import os
from src.read_s3_object_into_dataframe import read_s3_object_into_dataframe


@pytest.fixture(scope="function")
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture
def s3_client(aws_creds):
    with mock_aws():
        yield boto3.client("s3")


@pytest.fixture
def s3_bucket(s3_client):
    bucket_name = "test-bucket"
    region = "eu-west-2"
    bucket = s3_client.create_bucket(
        Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": region}
    )
    return bucket_name


@pytest.fixture
def add_bucket_object_csv(s3_client, s3_bucket):

    df = pd.DataFrame(
        {"id": ["1", "2"], "name": ["person1", "person2"], "country": ["UK", "India"]}
    )

    df_csv = df.to_csv()

    key = "people.csv"
    s3_client.put_object(Bucket=s3_bucket, Key=key, Body=df_csv)
    return key


@pytest.fixture
def add_bucket_object_json(s3_client, s3_bucket):

    df = pd.DataFrame(
        {"id": ["1", "2"], "name": ["person1", "person2"], "country": ["UK", "India"]}
    )

    df_json = df.to_json()

    key = "people.json"
    s3_client.put_object(Bucket=s3_bucket, Key=key, Body=df_json)
    return key


@pytest.fixture
def add_bucket_object_parquet(s3_client, s3_bucket):

    df = pd.DataFrame(
        {"id": ["1", "2"], "name": ["person1", "person2"], "country": ["UK", "India"]}
    )

    df_parquet = df.to_parquet()

    key = "people.parquet"
    s3_client.put_object(Bucket=s3_bucket, Key=key, Body=df_parquet)
    return key


def test_read_s3_object_into_dataframe_returns_a_dataframe(
    add_bucket_object_csv, s3_bucket
):

    key = add_bucket_object_csv
    df = read_s3_object_into_dataframe(s3_bucket, key, "csv")

    assert isinstance(df, pd.DataFrame)


def test_read_s3_object_into_dataframe_returns_correct_columns_for_csv(
    add_bucket_object_csv, s3_bucket
):

    key = add_bucket_object_csv
    df = read_s3_object_into_dataframe(s3_bucket, key, "csv")

    assert list(df.columns) == ["id", "name", "country"]
    assert len(df) == 2


def test_read_s3_object_into_dataframe_returns_correct_columns_for_json(
    add_bucket_object_json, s3_bucket
):

    key = add_bucket_object_json
    df = read_s3_object_into_dataframe(s3_bucket, key, "json")

    assert list(df.columns) == ["id", "name", "country"]
    assert len(df) == 2


def test_read_s3_object_into_dataframe_returns_correct_columns_for_parquet(
    add_bucket_object_parquet, s3_bucket
):

    key = add_bucket_object_parquet
    df = read_s3_object_into_dataframe(s3_bucket, key, "parquet")

    assert list(df.columns) == ["id", "name", "country"]
    assert len(df) == 2


def test_read_s3_object_into_dataframe_returns_error_when_key_not_found(s3_bucket):

    with pytest.raises(ValueError):
        read_s3_object_into_dataframe(s3_bucket, "hello", "csv")


def test_read_s3_object_into_dataframe_returns_error_when_no_bucket():

    with pytest.raises(ValueError):
        read_s3_object_into_dataframe("1", "2", "csv")


def test_read_s3_object_into_dataframe_returns_message_when_not_str():

    with pytest.raises(TypeError):
        read_s3_object_into_dataframe(1, 2)


def test_read_s3_object_into_dataframe_returns_message_for_unsupported_file_type(
    add_bucket_object_csv, s3_bucket
):

    key = add_bucket_object_csv
    with pytest.raises(TypeError):
        read_s3_object_into_dataframe(s3_bucket, key, "txt")


def test_read_s3_object_into_dataframe_returns_message_for_unsupported_inputs():

    with pytest.raises(TypeError):
        read_s3_object_into_dataframe(s3_bucket, 2, "csv")

    with pytest.raises(TypeError):
        read_s3_object_into_dataframe(1, 2, "csv")
