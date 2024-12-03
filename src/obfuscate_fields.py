import pandas as pd
import logging

def obfuscate_fields(df: pd.DataFrame, fields: list) -> pd.DataFrame | None:

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