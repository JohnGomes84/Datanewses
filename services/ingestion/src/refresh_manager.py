import sqlite3
import uuid
from datetime import datetime, timedelta
import json

from shared import config, get_logger

logger = get_logger(__name__)


SOURCE_DEFAULTS = [
    ("ANTT Rodovias", "rodovia", 360, "alta", "official", "api", "probe_fetch", "https://dados.antt.gov.br/api/3/action/package_search"),
    ("DNIT Dados Abertos", "rodovia", 720, "alta", "official", "api", "probe_fetch", "https://servicos.dnit.gov.br/dadosabertos/api/3/action/package_search"),
    ("ANTAQ Estatistica", "porto", 360, "alta", "official", "probe_fetch", "manual_portal", ""),
    ("ANAC Movimentacao Aeroportuaria", "aeroporto", 720, "alta", "official", "probe_fetch", "manual_portal", ""),
    ("SEFAZ-ES Documentos Fiscais", "fiscal", 360, "alta", "official", "probe_fetch", "manual_portal", ""),
    ("SEFAZ-ES NF-e Estatisticas", "fiscal", 1440, "media", "official", "probe_fetch", "manual_portal", ""),
    ("IBGE Localidades ES", "territorio", 10080, "alta", "official", "api", "cached_copy", "https://servicodados.ibge.gov.br/api/v1/localidades/estados/32/municipios"),
    ("MDIC Export Demand", "comex", 1440, "alta", "official", "file_download", "cached_copy", "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/mun/EXP_{ano}_MUN.csv"),
    ("BCB Market Indicators", "macro", 1440, "alta", "official", "api", "cached_copy", "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados?formato=json"),
    ("Regional Monitoring Derived", "regional", 360, "alta", "derived", "derived_official_signals", "cached_copy", ""),
    ("INMET Regional Forecast", "clima", 360, "alta", "official", "api", "cached_copy", "https://apiprevmet3.inmet.gov.br/previsao/{geocode}/3"),
    ("Official Transport News", "news", 720, "media", "official", "web_fetch", "cached_copy", ""),
]

SOURCE_POLICIES = [
    ("ANTT Rodovias", "intraday", 360, 540, 3, 1, "regional-intel"),
    ("DNIT Dados Abertos", "daily", 720, 1080, 2, 1, "regional-intel"),
    ("ANTAQ Estatistica", "intraday", 360, 540, 3, 1, "regional-intel"),
    ("ANAC Movimentacao Aeroportuaria", "daily", 720, 1080, 2, 1, "regional-intel"),
    ("SEFAZ-ES Documentos Fiscais", "intraday", 360, 540, 3, 1, "fiscal-intel"),
    ("SEFAZ-ES NF-e Estatisticas", "daily", 1440, 2160, 2, 1, "fiscal-intel"),
    ("IBGE Localidades ES", "weekly", 10080, 20160, 2, 0, "geo-intel"),
    ("MDIC Export Demand", "daily", 1440, 2160, 1, 0, "core-pipeline"),
    ("BCB Market Indicators", "daily", 1440, 2160, 1, 0, "macro-intel"),
    ("Regional Monitoring Derived", "intraday", 360, 540, 1, 0, "core-pipeline"),
    ("INMET Regional Forecast", "intraday", 360, 720, 1, 0, "core-pipeline"),
    ("Official Transport News", "daily", 720, 1080, 1, 0, "core-pipeline"),
]


def _connect():
    return sqlite3.connect(config.SQLITE_DB)


def ensure_source_registry():
    conn = _connect()
    cur = conn.cursor()
    now = datetime.now()
    for source_name, category, refresh_minutes, priority, source_type, preferred_ingestion_method, fallback_ingestion_method, api_url in SOURCE_DEFAULTS:
        cur.execute(
            """
            INSERT INTO source_registry (
                source_name, category, refresh_minutes, priority, enabled, status,
                last_success_at, last_attempt_at, next_run_at, reliability_score,
                source_type, preferred_ingestion_method, fallback_ingestion_method, api_url, last_ingestion_method
            )
            SELECT ?, ?, ?, ?, 1, 'pending', NULL, NULL, ?, 0.8, ?, ?, ?, ?, NULL
            WHERE NOT EXISTS (
                SELECT 1 FROM source_registry WHERE source_name = ?
            )
            """,
            (
                source_name,
                category,
                refresh_minutes,
                priority,
                (now + timedelta(minutes=refresh_minutes)).isoformat(),
                source_type,
                preferred_ingestion_method,
                fallback_ingestion_method,
                api_url,
                source_name,
            ),
        )
    conn.commit()
    conn.close()

    conn = _connect()
    cur = conn.cursor()
    for source_name, refresh_mode, refresh_minutes, stale_after_minutes, retry_limit, partial_refresh_enabled, owner in SOURCE_POLICIES:
        cur.execute(
            """
            INSERT INTO source_policy (
                source_name, refresh_mode, refresh_minutes, stale_after_minutes, retry_limit,
                partial_refresh_enabled, owner
            )
            SELECT ?, ?, ?, ?, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1 FROM source_policy WHERE source_name = ?
            )
            """,
            (
                source_name,
                refresh_mode,
                refresh_minutes,
                stale_after_minutes,
                retry_limit,
                partial_refresh_enabled,
                owner,
                source_name,
            ),
        )
    conn.commit()
    conn.close()


def start_pipeline_run(run_type="manual"):
    run_id = str(uuid.uuid4())
    started_at = datetime.now().isoformat()
    conn = _connect()
    conn.execute(
        """
        INSERT INTO ingestion_runs (run_id, run_type, status, started_at, finished_at, details)
        VALUES (?, ?, 'running', ?, NULL, '')
        """,
        (run_id, run_type, started_at),
    )
    conn.commit()
    conn.close()
    return run_id


def mark_source_attempt(source_name):
    conn = _connect()
    conn.execute(
        """
        UPDATE source_registry
        SET status = 'running', last_attempt_at = ?
        WHERE source_name = ?
        """,
        (datetime.now().isoformat(), source_name),
    )
    conn.commit()
    conn.close()


def mark_source_result(source_name, success, details="", ingestion_method=None):
    now = datetime.now()
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT refresh_minutes, reliability_score FROM source_registry WHERE source_name = ?",
        (source_name,),
    )
    row = cur.fetchone()
    refresh_minutes = row[0] if row else 1440
    current_reliability = row[1] if row else 0.8
    reliability = min(0.99, current_reliability + 0.02) if success else max(0.2, current_reliability - 0.08)
    status = "success" if success else "failed"
    last_success_at = now.isoformat() if success else None
    next_run_at = (now + timedelta(minutes=refresh_minutes)).isoformat()
    cur.execute(
        """
        UPDATE source_registry
        SET status = ?, last_attempt_at = ?, last_success_at = COALESCE(?, last_success_at),
            next_run_at = ?, reliability_score = ?, last_ingestion_method = COALESCE(?, last_ingestion_method)
        WHERE source_name = ?
        """,
        (status, now.isoformat(), last_success_at, next_run_at, reliability, ingestion_method, source_name),
    )
    conn.commit()
    conn.close()
    if details:
        logger.info(f"{source_name}: {details}")


def enqueue_refresh_job(source_name, job_type="refresh", priority="media", payload="{}"):
    job_id = str(uuid.uuid4())
    conn = _connect()
    conn.execute(
        """
        INSERT INTO refresh_queue (
            job_id, source_name, job_type, priority, status, attempts,
            enqueued_at, started_at, finished_at, payload
        ) VALUES (?, ?, ?, ?, 'queued', 0, ?, NULL, NULL, ?)
        """,
        (job_id, source_name, job_type, priority, datetime.now().isoformat(), payload),
    )
    conn.commit()
    conn.close()
    return job_id


def mark_job_started(job_id):
    conn = _connect()
    conn.execute(
        """
        UPDATE refresh_queue
        SET status = 'running', started_at = ?, attempts = attempts + 1
        WHERE job_id = ?
        """,
        (datetime.now().isoformat(), job_id),
    )
    conn.commit()
    conn.close()


def mark_job_finished(job_id, success):
    conn = _connect()
    conn.execute(
        """
        UPDATE refresh_queue
        SET status = ?, finished_at = ?
        WHERE job_id = ?
        """,
        ("success" if success else "failed", datetime.now().isoformat(), job_id),
    )
    conn.commit()
    conn.close()


def _execute_source_handler(source_name, payload):
    from services.ingestion.src import ingest_bcb, ingest_comex, ingest_ibge, ingest_inmet, ingest_regional, official_sources, scrape_news, source_catalog

    handlers = {
        "MDIC Export Demand": lambda: ingest_comex.fetch_comex_export(),
        "BCB Market Indicators": lambda: ingest_bcb.fetch_bcb_series(),
        "Regional Monitoring Derived": lambda: ingest_regional.fetch_regional_signals(payload),
        "INMET Regional Forecast": lambda: ingest_inmet.fetch_inmet_regional_forecast(),
        "Official Transport News": lambda: scrape_news.scrape_economic_news(),
        "IBGE Localidades ES": lambda: ingest_ibge.fetch_es_localities(),
        "ANTT Rodovias": lambda: _run_official_source("ANTT Rodovias", payload, source_catalog, official_sources),
        "DNIT Dados Abertos": lambda: _run_official_source("DNIT Dados Abertos", payload, source_catalog, official_sources),
        "ANTAQ Estatistica": lambda: _run_official_source("ANTAQ Estatistica", payload, source_catalog, official_sources),
        "ANAC Movimentacao Aeroportuaria": lambda: _run_official_source("ANAC Movimentacao Aeroportuaria", payload, source_catalog, official_sources),
        "SEFAZ-ES Documentos Fiscais": lambda: _run_official_source("SEFAZ-ES Documentos Fiscais", payload, source_catalog, official_sources),
        "SEFAZ-ES NF-e Estatisticas": lambda: _run_official_source("SEFAZ-ES NF-e Estatisticas", payload, source_catalog, official_sources),
    }
    handler = handlers.get(source_name)
    if handler is None:
        raise ValueError(f"No handler registered for source: {source_name}")
    result = handler()
    update_pipeline_state(f"last_payload::{source_name}", json.dumps(payload))
    return result


def _run_official_source(source_name, payload, source_catalog, official_sources):
    source_catalog.build_source_catalog()
    api_df = official_sources.fetch_ckan_catalog(source_name)
    probe_df = official_sources.probe_source_and_store(source_name)
    fetch_limit = 1 if payload.get("scope") == "entity" else 3
    asset_urls = payload.get("asset_urls") if isinstance(payload.get("asset_urls"), list) else None
    fetch_df, derived_df = official_sources.fetch_assets_from_probe(source_name, probe_df, limit=fetch_limit, asset_urls=asset_urls)
    return {
        "ingestion_method": "api" if not api_df.empty and "api_status" in api_df.columns and int(len(api_df[api_df["api_status"] == "success"])) > 0 else "probe_fetch",
        "api_records": int(len(api_df[api_df["api_status"] == "success"])) if not api_df.empty and "api_status" in api_df.columns else 0,
        "probe_records": int(len(probe_df)),
        "fetched_assets": int(len(fetch_df[fetch_df["fetch_status"] == "success"])) if not fetch_df.empty else 0,
        "failed_fetches": int(len(fetch_df[fetch_df["fetch_status"] != "success"])) if not fetch_df.empty else 0,
        "derived_candidates": int(len(derived_df)),
    }


def run_source_refresh(source_name, payload=None):
    payload = payload or {}
    mark_source_attempt(source_name)
    try:
        result = _execute_source_handler(source_name, payload)
        if result is None:
            result = {}
        if source_name in {
            "IBGE Localidades ES",
            "BCB Market Indicators",
            "INMET Regional Forecast",
            "ANTT Rodovias",
            "DNIT Dados Abertos",
            "ANTAQ Estatistica",
            "ANAC Movimentacao Aeroportuaria",
            "SEFAZ-ES Documentos Fiscais",
            "SEFAZ-ES NF-e Estatisticas",
        }:
            if source_name == "IBGE Localidades ES":
                api_records = int(len(result)) if hasattr(result, "__len__") else 0
                details = f"Fonte ingerida com {api_records} municipios oficiais do ES via API"
                mark_source_result(source_name, api_records > 0, details, "api")
                return api_records > 0
            if source_name == "BCB Market Indicators":
                api_records = int(len(result)) if hasattr(result, "__len__") else 0
                details = f"Fonte ingerida com {api_records} pontos diarios do BCB via API"
                mark_source_result(source_name, api_records > 0, details, "api")
                return api_records > 0
            if source_name == "INMET Regional Forecast":
                api_records = int(len(result)) if hasattr(result, "__len__") else 0
                details = f"Fonte ingerida com {api_records} sinais regionais oficiais de previsao do INMET"
                mark_source_result(source_name, api_records > 0, details, "api")
                return api_records > 0
            fetched_assets = int(result.get("fetched_assets", 0))
            probe_records = int(result.get("probe_records", 0))
            api_records = int(result.get("api_records", 0))
            if fetched_assets > 0 or api_records > 0:
                details = f"Fonte ingerida com {api_records} datasets via API, {fetched_assets} ativos baixados e {probe_records} ativos catalogados"
                mark_source_result(source_name, True, details, result.get("ingestion_method"))
                return True
            details = f"Probe realizado com {probe_records} ativos catalogados, mas sem API/fetch util"
            mark_source_result(source_name, False, details, result.get("ingestion_method"))
            return False
        else:
            details = "Refresh concluido com sucesso"
            method = "official_feed"
            if source_name == "MDIC Export Demand":
                method = "file_download"
            elif source_name == "Official Transport News":
                method = "web_fetch"
            elif source_name == "Regional Monitoring Derived":
                method = "derived_official_signals"
            mark_source_result(source_name, True, details, method)
            return True
    except Exception as exc:
        mark_source_result(source_name, False, str(exc))
        return False


def run_source_refresh_with_retry(source_name, payload=None, max_attempts=3, backoff_seconds=8):
    for attempt in range(1, max(1, max_attempts) + 1):
        success = run_source_refresh(source_name, payload)
        if success:
            return True
        if attempt < max_attempts:
            logger.warning(
                f"{source_name}: refresh failed on attempt {attempt}/{max_attempts}; retrying after {backoff_seconds * attempt}s"
            )
            from time import sleep

            sleep(backoff_seconds * attempt)
    return False


def enqueue_due_sources():
    conn = _connect()
    now = datetime.now().isoformat()
    rows = conn.execute(
        """
        SELECT sr.source_name, sr.priority
        FROM source_registry sr
        LEFT JOIN refresh_queue rq
            ON sr.source_name = rq.source_name
           AND rq.status IN ('queued', 'running')
        WHERE sr.enabled = 1
          AND (sr.next_run_at IS NULL OR sr.next_run_at <= ?)
          AND rq.job_id IS NULL
        """,
        (now,),
    ).fetchall()
    conn.close()
    for source_name, priority in rows:
        enqueue_refresh_job(source_name, "scheduled_refresh", priority)


def mark_stale_sources():
    conn = _connect()
    rows = conn.execute(
        """
        SELECT sr.source_name, sr.last_success_at, sp.stale_after_minutes
        FROM source_registry sr
        JOIN source_policy sp ON sp.source_name = sr.source_name
        WHERE sr.enabled = 1
        """
    ).fetchall()
    now = datetime.now()
    for source_name, last_success_at, stale_after_minutes in rows:
        if not last_success_at:
            continue
        age_minutes = (now - datetime.fromisoformat(last_success_at)).total_seconds() / 60
        if age_minutes > stale_after_minutes:
            conn.execute(
                "UPDATE source_registry SET status = 'stale' WHERE source_name = ?",
                (source_name,),
            )
    conn.commit()
    conn.close()


def process_refresh_queue(limit=20):
    conn = _connect()
    jobs = conn.execute(
        """
        SELECT rq.job_id, rq.source_name, rq.priority, rq.attempts, rq.payload, sp.retry_limit
        FROM refresh_queue rq
        LEFT JOIN source_policy sp ON sp.source_name = rq.source_name
        WHERE rq.status = 'queued'
        ORDER BY CASE priority WHEN 'alta' THEN 1 WHEN 'media' THEN 2 ELSE 3 END, enqueued_at
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()
    for job_id, source_name, priority, attempts, payload_raw, retry_limit in jobs:
        mark_job_started(job_id)
        payload = json.loads(payload_raw or "{}")
        success = run_source_refresh_with_retry(source_name, payload, max_attempts=retry_limit or 1)
        if success:
            mark_job_finished(job_id, True)
            update_pipeline_state(f"queue_last_processed::{source_name}", datetime.now().isoformat())
        else:
            if attempts + 1 < (retry_limit or 1):
                mark_job_finished(job_id, False)
                enqueue_refresh_job(source_name, "retry", priority, payload_raw or "{}")
            else:
                mark_job_finished(job_id, False)


def finish_pipeline_run(run_id, success, details=""):
    conn = _connect()
    conn.execute(
        """
        UPDATE ingestion_runs
        SET status = ?, finished_at = ?, details = ?
        WHERE run_id = ?
        """,
        ("success" if success else "failed", datetime.now().isoformat(), details, run_id),
    )
    conn.commit()
    conn.close()


def update_pipeline_state(key, value):
    conn = _connect()
    conn.execute(
        """
        INSERT INTO pipeline_state (state_key, state_value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(state_key) DO UPDATE SET state_value = excluded.state_value, updated_at = excluded.updated_at
        """,
        (key, value, datetime.now().isoformat()),
    )
    conn.commit()
    conn.close()
