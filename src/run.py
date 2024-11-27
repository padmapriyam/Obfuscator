import argparse
from get_and_obfuscate_s3_file import get_and_obfuscate_s3_file

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Script that obfuscate the mentioned fields in the passed s3 object file"
    )
    parser.add_argument("--s3FilePath", type=str)
    parser.add_argument("--obfuscateFields", nargs='+', type=str)
    args = parser.parse_args()

    s3_details = {
        'file_to_obfuscate': args.s3FilePath,
        'pii_fields': args.obfuscateFields
    }

    get_and_obfuscate_s3_file(s3_details)