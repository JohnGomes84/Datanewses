import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _resolve_runtime_path(env_value, default_container_path):
    raw_value = os.getenv(env_value, default_container_path)
    # Keep /app paths in containers, but map them to local project dirs outside Docker.
    if raw_value.startswith('/app/') and not Path('/app').exists():
        return str(PROJECT_ROOT / raw_value.replace('/app/', '', 1))
    return raw_value


DATA_DIR = _resolve_runtime_path('DATA_DIR', '/app/data')
MODEL_DIR = _resolve_runtime_path('MODEL_DIR', '/app/models')
SQLITE_DB = os.path.join(DATA_DIR, 'nowcasting.db')
MLFLOW_DB = os.path.join(MODEL_DIR, 'mlflow.db')
MLFLOW_ARTIFACT_DIR = os.path.join(MODEL_DIR, 'mlruns')
MLFLOW_TRACKING_URI = os.getenv('MLFLOW_TRACKING_URI', f"sqlite:///{MLFLOW_DB.replace(os.sep, '/')}")
MLFLOW_EXPERIMENT_NAME = os.getenv('MLFLOW_EXPERIMENT_NAME', 'workforce-forecast-local')

COMEX_API_URL = os.getenv('COMEX_API_URL', 'https://api.dados.gov.br/comex/exportacao')
BCB_API_URL = os.getenv('BCB_API_URL', 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.por/dados')
NEWS_SCRAPE_URL = os.getenv('NEWS_SCRAPE_URL', 'https://exemplo.com/noticias-economia')

RUN_INTERVAL = int(os.getenv('RUN_INTERVAL', '86400'))
SOURCE_RETRY_ATTEMPTS = int(os.getenv('SOURCE_RETRY_ATTEMPTS', '3'))
SOURCE_RETRY_BACKOFF_SECONDS = int(os.getenv('SOURCE_RETRY_BACKOFF_SECONDS', '8'))
HISTORY_RETENTION_DAYS = int(os.getenv('HISTORY_RETENTION_DAYS', '90'))
