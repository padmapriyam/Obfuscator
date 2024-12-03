import boto3
import pandas as pd
import io

def read_s3_object_into_dataframe(bucket_name: str, key: str, type_of_file: str) -> pd.DataFrame:
    """This function is used to read file in s3 bucket and convert into a dataframe, ready for obfuscating the fields.

    Args:
        bucket_name: the name of the bucket to access tables containing json lines data from, in this case, the imgestion s3 bucket.
        key: the filepath within the bucket to the json lines file to be read into a dataframe.
        type_of_file: the type of the input file retrieved from the key passed

    Returns:
        A dataframe of the records in the input file.

    Raises:
        TyperError is the bucket_name or key arguments passed are not strings, type of file not supported.
    """
    supported_file_types = ['csv', 'json', 'parquet']

    if not isinstance(bucket_name, str):
        raise TypeError("bucket_name must be a string")

    if not isinstance(key, str):
        raise TypeError("the specified key must be a string")
    
    if type_of_file not in supported_file_types:
        raise TypeError("Only csv, json and parquet files supported")

    try:
        s3_client = boto3.client("s3")
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        body = response["Body"].read().decode('utf-8')
        df = pd.read_csv(io.StringIO(body), index_col=[0])
        return df
    except Exception as e:
        raise ValueError(f"Error reading or processing the object from S3: {e}")
