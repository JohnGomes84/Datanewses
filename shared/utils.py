import pandas as pd
from datetime import datetime
from pathlib import Path

def safe_float_convert(series):
    return series.astype(str).str.replace(',', '.').astype(float, errors='ignore')

def save_parquet(df, path, filename=None):
    if filename is None:
        filename = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    full_path = Path(path) / filename
    full_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(full_path, index=False)
    return full_path

def load_latest_parquet(path):
    path = Path(path)
    if not path.exists():
        return None
    files = sorted(path.glob('*.parquet'))
    if not files:
        return None
    return pd.read_parquet(files[-1])
