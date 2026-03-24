import sqlite3
from datetime import datetime, timedelta

import pandas as pd

from . import config
from .logger import get_logger

logger = get_logger(__name__)


RETENTION_TABLES = {
    "regional_monitoring_history": "snapshot_captured_at",
    "workforce_forecasts_history": "snapshot_captured_at",
    "alerts_operacionais_history": "snapshot_captured_at",
    "model_performance_history": "recorded_at",
    "model_backtest_folds": "recorded_at",
    "data_quality_checks": "checked_at",
}


def _connect():
    return sqlite3.connect(config.SQLITE_DB)


def cleanup_history(retention_days=None):
    retention_days = retention_days or config.HISTORY_RETENTION_DAYS
    cutoff = (datetime.now() - timedelta(days=retention_days)).isoformat()
    conn = _connect()
    deleted_rows = {}

    for table_name, timestamp_column in RETENTION_TABLES.items():
        before = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        conn.execute(f"DELETE FROM {table_name} WHERE {timestamp_column} < ?", (cutoff,))
        after = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        deleted_rows[table_name] = int(before - after)

    conn.commit()
    conn.close()
    logger.info(f"History cleanup summary: {deleted_rows}")
    return deleted_rows


def record_pipeline_run_summary(run_id):
    conn = _connect()
    run_row = conn.execute(
        """
        SELECT status, started_at, finished_at
        FROM ingestion_runs
        WHERE run_id = ?
        """,
        (run_id,),
    ).fetchone()
    if run_row is None:
        conn.close()
        return None

    source_summary = conn.execute(
        """
        SELECT
            SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END),
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END),
            SUM(CASE WHEN status = 'stale' THEN 1 ELSE 0 END)
        FROM source_registry
        WHERE enabled = 1
        """
    ).fetchone()
    cached_fallbacks = conn.execute(
        """
        SELECT COUNT(*)
        FROM pipeline_state
        WHERE state_key LIKE 'cached_fallback::%'
        """
    ).fetchone()[0]
    quality_summary = conn.execute(
        """
        SELECT
            SUM(CASE WHEN status = 'fail' AND severity = 'high' THEN 1 ELSE 0 END),
            SUM(CASE WHEN status = 'warn' THEN 1 ELSE 0 END)
        FROM data_quality_checks
        WHERE run_id = ?
        """,
        (run_id,),
    ).fetchone()

    started_at = pd.to_datetime(run_row[1]) if run_row[1] else None
    finished_at = pd.to_datetime(run_row[2]) if run_row[2] else None
    duration_seconds = None
    if started_at is not None and finished_at is not None:
        duration_seconds = int((finished_at - started_at).total_seconds())

    summary = pd.DataFrame(
        [
            {
                "run_id": run_id,
                "status": run_row[0],
                "started_at": run_row[1],
                "finished_at": run_row[2],
                "duration_seconds": duration_seconds,
                "healthy_sources": int(source_summary[0] or 0),
                "failed_sources": int(source_summary[1] or 0),
                "stale_sources": int(source_summary[2] or 0),
                "cached_fallbacks": int(cached_fallbacks or 0),
                "critical_quality_failures": int(quality_summary[0] or 0),
                "quality_warnings": int(quality_summary[1] or 0),
                "recorded_at": datetime.now().isoformat(),
            }
        ]
    )
    summary.to_sql("pipeline_run_summaries", conn, if_exists="append", index=False)
    conn.close()
    logger.info(f"Pipeline run summary recorded for {run_id}")
    return summary.iloc[0].to_dict()
