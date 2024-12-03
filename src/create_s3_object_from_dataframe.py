import pandas as pd
import io

def create_s3_object_from_dataframe(df: pd.DataFrame, file_type: str) -> str | bytes | None:

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