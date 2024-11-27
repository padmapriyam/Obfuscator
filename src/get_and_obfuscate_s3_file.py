import boto3
from read_s3_object_to_dataframe import read_s3_object_into_dataframe

def get_and_obfuscate_s3_file(s3_details: dict):

    if s3_details['pii_fields'] is None:
        raise TypeError("No fields to obfuscate")
 
    bucket_name = s3_details['file_to_obfuscate'].split('/')[-2]
    s3_file = s3_details['file_to_obfuscate'].split('/')[-1]
    print(bucket_name)
    print(s3_file)
    print(s3_details['pii_fields'])
    
    # try:
    #     s3_data_frame = read_s3_object_into_dataframe(bucket_name, s3_file)
    # except Exception as e:
    #     return