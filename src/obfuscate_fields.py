import pandas as pd
import logging


def obfuscate_fields(df: pd.DataFrame, fields: list) -> pd.DataFrame | None:
    """This function is used to obfuscate the passed fields in the provided dataframe
    Args:
        df: Dataframe of the object obtained from the S3 bucket
        fields: the name of the columns in the object which needs to be obfuscated

    Returns:
        A dataframe with the passed fields obfuscated
        None if the dataframe is empty (no records in the input object)
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    if df.empty:
        logging.error(f"No records present in the input file")
        return None

    for field in fields:
        if field in df.columns:
            df[field] = "***"
            logging.info(f"Obfuscated field '{field}' in the input file")
        else:
            logging.error(f"Field '{field}' not available in the input file")
    return df
