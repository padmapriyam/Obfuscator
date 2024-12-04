import logging
import io
from src.read_s3_object_into_dataframe import read_s3_object_into_dataframe
from src.obfuscate_fields import obfuscate_fields
from src.create_s3_object_from_dataframe import create_s3_object_from_dataframe

supported_file_types = ["csv", "json", "parquet"]


class InvalidInputError(Exception):
    """
    Raised when the input file/fields are not valid
    """

    pass


class UnsupportedFileTypeError(Exception):
    """
    Raised when the input file type is not supported by obfuscator
    """

    pass


def get_and_obfuscate_s3_file(s3_details: dict) -> str | bytes | None:

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    if not s3_details["pii_fields"] or s3_details["file_to_obfuscate"] is None:
        raise InvalidInputError("No file/fields provided to obfuscate")

    bucket_name = s3_details["file_to_obfuscate"].split("/")[-2]
    s3_file = s3_details["file_to_obfuscate"].split("/")[-1]

    file_type = s3_file.split(".")[-1]

    if file_type not in supported_file_types:
        raise UnsupportedFileTypeError(
            "Only csv, json and parquet files supported for obfuscation"
        )

    logging.info(
        f"Obfuscating fields '{s3_details['pii_fields']}' in file '{s3_file}' in S3 bucket '{bucket_name}'  "
    )

    try:
        s3_data_frame = read_s3_object_into_dataframe(bucket_name, s3_file, file_type)
    except Exception as e:
        logger.error(f"Error reading file {s3_file} from bucket {bucket_name} {e}")
        return

    modified_df = obfuscate_fields(s3_data_frame, s3_details["pii_fields"])

    if modified_df is None:
        return None

    try:
        obfuscated_s3_object = create_s3_object_from_dataframe(modified_df, file_type)
        return obfuscated_s3_object
    except Exception as e:
        logger.error(f"Error converting file {s3_file} from bucket {bucket_name} {e}")
