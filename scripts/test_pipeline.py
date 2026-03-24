import os
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from services.ingestion.src import refresh_manager
from services.ml.src import predict, train
from services.processing.src import aggregate, transform
from shared import cleanup_history, config, ensure_dirs, get_logger, record_pipeline_run_summary, run_data_quality_checks, snapshot_run_outputs

logger = get_logger("OperationalPipeline")

SOURCE_CACHE_FILES = {
    "MDIC Export Demand": Path(config.DATA_DIR) / "bronze" / "comex_exportacao" / "operacoes_logisticas.parquet",
    "BCB Market Indicators": Path(config.DATA_DIR) / "bronze" / "bcb" / "indicadores_mercado.parquet",
    "Official Transport News": Path(config.DATA_DIR) / "bronze" / "news_raw" / "noticias_operacionais.parquet",
    "INMET Regional Forecast": Path(config.DATA_DIR) / "bronze" / "regional" / "regional_inmet_forecast.parquet",
    "Regional Monitoring Derived": Path(config.DATA_DIR) / "bronze" / "regional" / "regional_signals.parquet",
}


def _count(conn, table_name):
    return conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]


def _cached_source_is_usable(source_name):
    cache_path = SOURCE_CACHE_FILES.get(source_name)
    return bool(cache_path and cache_path.exists() and cache_path.stat().st_size > 0)


def _refresh_with_resilience(source_name, retries=None):
    max_attempts = retries or max(1, config.SOURCE_RETRY_ATTEMPTS)
    for attempt in range(1, max_attempts + 1):
        success = refresh_manager.run_source_refresh(source_name)
        if success:
            if attempt > 1:
                logger.info(f"{source_name}: refresh succeeded on retry {attempt}/{max_attempts}")
            return True
        if attempt < max_attempts:
            wait_seconds = config.SOURCE_RETRY_BACKOFF_SECONDS * attempt
            logger.warning(f"{source_name}: refresh failed on attempt {attempt}/{max_attempts}; retrying in {wait_seconds}s")
            time.sleep(wait_seconds)

    if _cached_source_is_usable(source_name):
        logger.warning(f"{source_name}: using cached local artifact after external refresh failure")
        refresh_manager.update_pipeline_state(f"cached_fallback::{source_name}", datetime.now().isoformat())
        return True
    return False


def run_test():
    ensure_dirs()
    refresh_manager.ensure_source_registry()
    run_id = refresh_manager.start_pipeline_run()
    logger.info("--- Iniciando pipeline operacional ---")
    try:
        logger.info("1. Ingestao de sinais operacionais")
        core_sources = [
            "MDIC Export Demand",
            "BCB Market Indicators",
            "Official Transport News",
            "INMET Regional Forecast",
            "Regional Monitoring Derived",
        ]
        official_sources = [
            "IBGE Localidades ES",
            "ANTT Rodovias",
            "DNIT Dados Abertos",
            "ANTAQ Estatistica",
            "ANAC Movimentacao Aeroportuaria",
            "SEFAZ-ES Documentos Fiscais",
            "SEFAZ-ES NF-e Estatisticas",
        ]
        for source_name in core_sources:
            if not _refresh_with_resilience(source_name):
                raise RuntimeError(f"Source refresh failed: {source_name}")
        official_results = {}
        for source_name in official_sources:
            official_results[source_name] = _refresh_with_resilience(source_name, retries=2)
            if not official_results[source_name]:
                logger.warning(f"Fonte oficial sem fetch util neste ciclo: {source_name}")

        logger.info("2. Transformacao")
        transform.process_comex()
        transform.process_bcb()
        transform.process_news()
        transform.process_regional()
        transform.process_catalog()
        transform.process_ibge_localities()
        transform.process_official_probes()
        transform.process_official_asset_fetches()
        transform.process_official_api_catalog()
        transform.process_official_asset_intelligence()

        logger.info("3. Agregacao e consolidacao")
        aggregate.build_operational_base()

        logger.info("4. Treinamento do modelo de escala")
        train.train_nowcasting(run_id)

        logger.info("5. Geracao de previsoes, alertas e insights")
        predict.predict_next()

        logger.info("6. Validacao de qualidade dos dados")
        quality_summary, _ = run_data_quality_checks(run_id)
        snapshot_summary = snapshot_run_outputs(run_id)
        cleanup_summary = cleanup_history()

        conn = sqlite3.connect(config.SQLITE_DB)
        summary = {
            "operations_daily": _count(conn, "operations_daily"),
            "contract_summary": _count(conn, "contract_summary"),
            "workforce_forecasts": _count(conn, "workforce_forecasts"),
            "workforce_forecasts_history": _count(conn, "workforce_forecasts_history"),
            "alerts_operacionais": _count(conn, "alerts_operacionais"),
            "alerts_operacionais_history": _count(conn, "alerts_operacionais_history"),
            "executive_insights": _count(conn, "executive_insights"),
            "model_performance_history": _count(conn, "model_performance_history"),
            "model_backtest_folds": _count(conn, "model_backtest_folds"),
            "news_monitoring": _count(conn, "news_monitoring"),
            "regional_monitoring": _count(conn, "regional_monitoring"),
            "direct_infrastructure_signals": _count(conn, "direct_infrastructure_signals"),
            "regional_monitoring_history": _count(conn, "regional_monitoring_history"),
            "source_catalog": _count(conn, "source_catalog"),
            "monitored_entities": _count(conn, "monitored_entities"),
            "municipality_catalog": _count(conn, "municipality_catalog"),
            "entity_registry": _count(conn, "entity_registry"),
            "source_probe": _count(conn, "source_probe"),
            "asset_registry": _count(conn, "asset_registry"),
            "fetched_assets": _count(conn, "fetched_assets"),
            "asset_fetch_log": _count(conn, "asset_fetch_log"),
            "official_api_catalog": _count(conn, "official_api_catalog"),
            "official_signal_intelligence": _count(conn, "official_signal_intelligence"),
            "download_candidates": _count(conn, "download_candidates"),
            "data_quality_checks": _count(conn, "data_quality_checks"),
            "pipeline_run_summaries": _count(conn, "pipeline_run_summaries"),
        }
        conn.close()
        summary["official_fetch_success"] = sum(1 for success in official_results.values() if success)
        summary["data_quality_failed_high"] = quality_summary["failed_high"]
        summary["data_quality_warned_total"] = quality_summary["warned_total"]
        summary["snapshot_summary"] = str(snapshot_summary)
        summary["cleanup_summary"] = str(cleanup_summary)
        refresh_manager.update_pipeline_state("last_success_summary", str(summary))
        refresh_manager.update_pipeline_state("last_success_at", datetime.now().isoformat())
        refresh_manager.update_pipeline_state("data_quality_summary", str(quality_summary))
        refresh_manager.mark_stale_sources()
        refresh_manager.enqueue_due_sources()
        refresh_manager.finish_pipeline_run(run_id, True, str(summary))
        run_summary = record_pipeline_run_summary(run_id)
        summary["run_summary"] = str(run_summary)
        logger.info(f"Resumo final: {summary}")
        logger.info("--- Pipeline operacional concluido ---")
    except Exception as exc:
        refresh_manager.finish_pipeline_run(run_id, False, str(exc))
        raise


def _enqueue_partial_refreshes():
    return


if __name__ == "__main__":
    run_test()
