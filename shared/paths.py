import os
from pathlib import Path
from .config import DATA_DIR, MODEL_DIR

def ensure_dirs():
    Path(DATA_DIR, 'bronze').mkdir(parents=True, exist_ok=True)
    Path(DATA_DIR, 'silver').mkdir(parents=True, exist_ok=True)
    Path(DATA_DIR, 'gold').mkdir(parents=True, exist_ok=True)
    Path(MODEL_DIR, 'saved').mkdir(parents=True, exist_ok=True)
