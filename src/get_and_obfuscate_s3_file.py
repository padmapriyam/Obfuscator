import logging
from src.read_s3_object_into_dataframe import read_s3_object_into_dataframe

supported_file_types = ['csv', 'json', 'parquet']
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

def get_and_obfuscate_s3_file(s3_details: dict):

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    if not s3_details['pii_fields'] or s3_details['file_to_obfuscate'] is None:
        raise InvalidInputError("No file/fields provided to obfuscate")
 
    bucket_name = s3_details['file_to_obfuscate'].split('/')[-2]
    s3_file = s3_details['file_to_obfuscate'].split('/')[-1]

    file_type = s3_file.split('.')[-1]

    if file_type not in supported_file_types:
        raise UnsupportedFileTypeError("Only csv, json and parquet files supported for obfuscation")

    
    try:
        s3_data_frame = read_s3_object_into_dataframe(bucket_name, s3_file, file_type)
    except Exception as e:
        logger.error(f"Error reading file {s3_file} from bucket {bucket_name} into dataframe: {e}")
        return
    
    # for column in s3_details["pii_fields"]:
    #     if column in s3_data_frame.columns:
    #         logging.info(f"Obfuscated field {column} in file {s3_file}")
    #         s3_data_frame[column] = '***'
    #     else:
    #         logging.info(f"The passed field {column} not available in the file {s3_file}")

    
    # print(s3_data_frame)