import pandas as pd
import io


def create_s3_object_from_dataframe(
    df: pd.DataFrame, file_type: str
) -> str | bytes | None:
    """Creates a new object compatible with boto3 putObject of S3.

    This function uses the relevant functions in the pandas library
    to convert the passed dataframe into format suitable for uploading
    to S3 bucket

    Args:
        df: Dataframe of the object obtained from S3 and fields obfuscated
        file_type: the type of input file (csv, json or parquet)

    Returns:
        A string or bytes buffer depending on the file_type if successful
        None if df is empty

    Raises:
        TypeError when file_type not supported
    """
    if df.empty:
        return None

    if file_type == "csv":
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer)
        return csv_buffer.getvalue()
    elif file_type == "json":
        json_buffer = io.StringIO()
        df.to_json(json_buffer, orient="records", lines=True)
        return json_buffer.getvalue()
    elif file_type == "parquet":
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer)
        return parquet_buffer.getvalue()
    else:
        raise TypeError("Only csv, json and parquet files supported")
