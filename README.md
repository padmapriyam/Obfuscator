# GDPR Obfuscator Project


## Context
The purpose of this project is to create a general-purpose tool to process data being ingested to AWS and
intercept personally identifiable information (PII). 

## How to use
`get_and_obfuscate_s3_file` is the main function which needs to be invoked with the one parameter `s3_details`

`s3_details` is of type `str` (json string) which has keys

* `file_to_obfuscate` - `str` - the s3 location where the file to be obfuscated is stored,
* `pii_fields` - `list` of `str` - the names of the fields that are required to be obfuscated

For example, the input might be:
```json
{
    "file_to_obfuscate": "s3://my_ingestion_bucket/new_data/file1.csv",
    "pii_fields": ["name", "email_address"]
}
```
The target CSV file might look like this:
```csv
student_id,name,course,cohort,graduation_date,email_address
...
1234,'John Smith','Software','2024-03-31','j.smith@email.com'
...
```

The output will be a byte-stream representation of a file like this:
```csv
student_id,name,course,cohort,graduation_date,email_address
...
1234,'***','Software','2024-03-31','***'
...
```
The output file format will be the same format as the input file with the passed fields obfuscated with `*`

## Assumptions and Prerequisites
1. Data is stored in CSV-, JSON-, or parquet-formatted files in an AWS S3 bucket.
2. Fields containing GDPR-sensitive data are known and will be supplied in advance.

# Setup

1. Clone this repository to your local machine:
   https://github.com/padmapriyam/Obfuscator.git
2. Install Python, install dependencies and run unittests via Make commands (Make all)
3. Configure AWS's iam credentials using the AWS CLI: aws configure.

python src/run.py --s3FilePath s3://pm-gdpr-obfuscator/people-100.csv --obfuscateFields "First Name" "Sex"
python src/run.py --s3FilePath s3://pm-gdpr-obfuscator/generated.json --obfuscateFields "balance"
python src/run.py --s3FilePath s3://pm-gdpr-obfuscator/people.parquet --obfuscateFields "First Name" "Sex"