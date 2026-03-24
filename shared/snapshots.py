from datetime import datetime
import sqlite3

import pandas as pd

from . import config
from .logger import get_logger

logger = get_logger(__name__)


SNAPSHOT_TARGETS = {
    "regional_monitoring": "regional_monitoring_history",
    "workforce_forecasts": "workforce_forecasts_history",
    "alerts_operacionais": "alerts_operacionais_history",
}


def snapshot_run_outputs(run_id):
    captured_at = datetime.now().isoformat()
    conn = sqlite3.connect(config.SQLITE_DB)
    snapshot_counts = {}

    for source_table, target_table in SNAPSHOT_TARGETS.items():
        frame = pd.read_sql_query(f"SELECT * FROM {source_table}", conn)
        if frame.empty:
            snapshot_counts[target_table] = 0
            continue
        frame.insert(0, "snapshot_run_id", run_id)
        frame.insert(1, "snapshot_captured_at", captured_at)
        frame.to_sql(target_table, conn, if_exists="append", index=False)
        snapshot_counts[target_table] = int(len(frame))

    conn.close()
    logger.info(f"Snapshot summary: {snapshot_counts}")
    return snapshot_counts
