import json
import sqlite3
import uuid
from datetime import datetime

import pandas as pd

from . import config
from .logger import get_logger

logger = get_logger(__name__)


def _connect():
    return sqlite3.connect(config.SQLITE_DB)


def _record_check(records, run_id, check_group, table_name, check_name, status, severity, observed_value, expected_value, details):
    records.append(
        {
            "check_id": str(uuid.uuid4()),
            "run_id": run_id,
            "check_group": check_group,
            "table_name": table_name,
            "check_name": check_name,
            "status": status,
            "severity": severity,
            "observed_value": str(observed_value),
            "expected_value": str(expected_value),
            "details": details,
            "checked_at": datetime.now().isoformat(),
        }
    )


def _evaluate_min_rows(conn, records, run_id, table_name, min_rows, severity):
    count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    status = "pass" if count >= min_rows else "fail"
    _record_check(
        records,
        run_id,
        "volume",
        table_name,
        "min_rows",
        status,
        severity,
        count,
        f">={min_rows}",
        f"Tabela {table_name} com {count} linhas",
    )


def _evaluate_null_ratio(conn, records, run_id, table_name, column_name, max_ratio, severity):
    total = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    if total == 0:
        ratio = 1.0
    else:
        missing = conn.execute(
            f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} IS NULL OR TRIM(CAST({column_name} AS TEXT)) = ''"
        ).fetchone()[0]
        ratio = missing / total
    status = "pass" if ratio <= max_ratio else "fail"
    _record_check(
        records,
        run_id,
        "completeness",
        table_name,
        f"null_ratio::{column_name}",
        status,
        severity,
        f"{ratio:.4f}",
        f"<={max_ratio:.4f}",
        f"Coluna {column_name} com proporcao nula {ratio:.2%}",
    )


def _evaluate_freshness(conn, records, run_id, table_name, column_name, max_age_days, severity):
    row = conn.execute(f"SELECT MAX({column_name}) FROM {table_name}").fetchone()
    raw_value = row[0] if row else None
    if not raw_value:
        age_days = None
        status = "fail"
        observed = "None"
        details = f"Tabela {table_name} sem data em {column_name}"
    else:
        latest = pd.to_datetime(raw_value)
        age_days = (pd.Timestamp.now().normalize() - latest.normalize()).days
        status = "pass" if age_days <= max_age_days else "fail"
        observed = age_days
        details = f"Data mais recente em {table_name}.{column_name}: {latest.date()}"
    _record_check(
        records,
        run_id,
        "freshness",
        table_name,
        f"freshness::{column_name}",
        status,
        severity,
        observed,
        f"<={max_age_days} days",
        details,
    )


def _evaluate_forecast_horizon(conn, records, run_id):
    horizon = conn.execute("SELECT COUNT(DISTINCT data) FROM workforce_forecasts").fetchone()[0]
    status = "pass" if horizon == 14 else "fail"
    _record_check(
        records,
        run_id,
        "business",
        "workforce_forecasts",
        "forecast_horizon_days",
        status,
        "high",
        horizon,
        "14",
        f"Horizonte de previsao com {horizon} dias distintos",
    )


def _evaluate_alert_coverage(conn, records, run_id):
    alerts = conn.execute("SELECT COUNT(*) FROM alerts_operacionais").fetchone()[0]
    forecasts = conn.execute("SELECT COUNT(*) FROM workforce_forecasts").fetchone()[0]
    ratio = 0.0 if forecasts == 0 else alerts / forecasts
    status = "pass" if alerts > 0 else "warn"
    _record_check(
        records,
        run_id,
        "business",
        "alerts_operacionais",
        "alert_coverage",
        status,
        "medium",
        f"{ratio:.4f}",
        ">0 alerts",
        f"{alerts} alertas sobre {forecasts} previsoes",
    )


def _evaluate_source_health(conn, records, run_id):
    total = conn.execute("SELECT COUNT(*) FROM source_registry WHERE enabled = 1").fetchone()[0]
    healthy = conn.execute("SELECT COUNT(*) FROM source_registry WHERE enabled = 1 AND status = 'success'").fetchone()[0]
    ratio = 0.0 if total == 0 else healthy / total
    status = "pass" if ratio >= 0.6 else "warn"
    _record_check(
        records,
        run_id,
        "operations",
        "source_registry",
        "healthy_sources_ratio",
        status,
        "medium",
        f"{ratio:.4f}",
        ">=0.6000",
        f"{healthy} fontes saudaveis de {total} habilitadas",
    )


def run_data_quality_checks(run_id):
    conn = _connect()
    records = []

    critical_row_checks = [
        ("operations_daily", 500, "high"),
        ("contract_summary", 5, "high"),
        ("workforce_forecasts", 50, "high"),
        ("executive_insights", 3, "medium"),
        ("source_registry", 6, "medium"),
    ]
    for table_name, min_rows, severity in critical_row_checks:
        _evaluate_min_rows(conn, records, run_id, table_name, min_rows, severity)

    for table_name, column_name, max_ratio, severity in [
        ("operations_daily", "data", 0.0, "high"),
        ("operations_daily", "unidade", 0.0, "high"),
        ("operations_daily", "cliente", 0.0, "high"),
        ("workforce_forecasts", "data", 0.0, "high"),
        ("workforce_forecasts", "unidade", 0.0, "high"),
        ("workforce_forecasts", "cliente", 0.0, "high"),
        ("alerts_operacionais", "acao_recomendada", 0.0, "medium"),
    ]:
        _evaluate_null_ratio(conn, records, run_id, table_name, column_name, max_ratio, severity)

    for table_name, column_name, max_age_days, severity in [
        ("operations_daily", "data", 35, "high"),
        ("news_monitoring", "data", 14, "medium"),
        ("regional_monitoring", "data", 35, "medium"),
    ]:
        _evaluate_freshness(conn, records, run_id, table_name, column_name, max_age_days, severity)

    _evaluate_forecast_horizon(conn, records, run_id)
    _evaluate_alert_coverage(conn, records, run_id)
    _evaluate_source_health(conn, records, run_id)

    quality_df = pd.DataFrame(records)
    quality_df.to_sql("data_quality_checks", conn, if_exists="replace", index=False)

    failed_high = int(len(quality_df[(quality_df["status"] == "fail") & (quality_df["severity"] == "high")]))
    failed_total = int(len(quality_df[quality_df["status"] == "fail"]))
    warned_total = int(len(quality_df[quality_df["status"] == "warn"]))
    summary = {
        "run_id": run_id,
        "total_checks": int(len(quality_df)),
        "failed_high": failed_high,
        "failed_total": failed_total,
        "warned_total": warned_total,
        "passed_total": int(len(quality_df[quality_df["status"] == "pass"])),
    }
    conn.close()

    logger.info(f"Data quality summary: {summary}")
    if failed_high:
        raise RuntimeError(f"Data quality failed: {json.dumps(summary, ensure_ascii=True)}")
    return summary, quality_df
