import argparse
import os
import sqlite3
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from shared import config, ensure_dirs


TABLE_DEFINITIONS = {
    "operations_daily": "CREATE TABLE IF NOT EXISTS operations_daily (data TIMESTAMP, ano INTEGER, mes INTEGER, dia_semana TEXT, unidade TEXT, cliente TEXT, tipo_operacao TEXT, turno TEXT, corredor TEXT, municipio TEXT, modal_predominante TEXT, volume_toneladas REAL, cargas_previstas INTEGER, demanda_externa_index REAL, pressao_mao_obra_index REAL, combustivel_index REAL, chuva_mm REAL, news_risk_score REAL, rodovias_trafego_index REAL, porto_fila_index REAL, aeroporto_carga_index REAL, fiscal_emissao_index REAL, interdicao_prob REAL, infraestrutura_risk_index REAL, absenteismo_pct REAL, trabalhadores_planejados INTEGER, trabalhadores_presentes INTEGER, trabalhadores_necessarios INTEGER, gap_mao_obra INTEGER, horas_extras REAL, produtividade_estimada REAL, sla_meta REAL, sla_realizado REAL, receita_estimada REAL, custo_operacional REAL, margem_estimada REAL, risco_operacional REAL)",
    "contract_summary": "CREATE TABLE IF NOT EXISTS contract_summary (cliente TEXT, unidade TEXT, receita_total REAL, margem_total REAL, sla_medio REAL, absenteismo_medio REAL, risco_medio REAL, equipe_media REAL)",
    "workforce_forecasts": "CREATE TABLE IF NOT EXISTS workforce_forecasts (data TIMESTAMP, unidade TEXT, cliente TEXT, tipo_operacao TEXT, turno TEXT, volume_toneladas REAL, cargas_previstas INTEGER, trabalhadores_previstos INTEGER, capacidade_atual INTEGER, gap_previsto INTEGER, sla_previsto REAL, receita_prevista REAL, custo_previsto REAL, margem_prevista REAL, risk_score REAL, acao_recomendada TEXT)",
    "alerts_operacionais": "CREATE TABLE IF NOT EXISTS alerts_operacionais (data TIMESTAMP, unidade TEXT, cliente TEXT, tipo_operacao TEXT, turno TEXT, volume_toneladas REAL, cargas_previstas INTEGER, trabalhadores_previstos INTEGER, capacidade_atual INTEGER, gap_previsto INTEGER, sla_previsto REAL, receita_prevista REAL, custo_previsto REAL, margem_prevista REAL, risk_score REAL, acao_recomendada TEXT, alerta TEXT)",
    "executive_insights": "CREATE TABLE IF NOT EXISTS executive_insights (insight_type TEXT, priority TEXT, title TEXT, detail TEXT, generated_at TIMESTAMP)",
    "news_monitoring": "CREATE TABLE IF NOT EXISTS news_monitoring (data TIMESTAMP, titulo TEXT, tema TEXT, risk_score REAL, sentimento TEXT, origem TEXT)",
    "regional_monitoring": "CREATE TABLE IF NOT EXISTS regional_monitoring (data TIMESTAMP, corredor TEXT, municipio TEXT, municipio_id INTEGER, regiao_imediata TEXT, regiao_intermediaria TEXT, modal_predominante TEXT, rodovias_trafego_index REAL, porto_fila_index REAL, aeroporto_carga_index REAL, fiscal_emissao_index REAL, interdicao_prob REAL, source_support_index REAL, infraestrutura_risk_index_base REAL, infraestrutura_risk_index REAL, impacto_fontes_diretas REAL)",
    "direct_infrastructure_signals": "CREATE TABLE IF NOT EXISTS direct_infrastructure_signals (data TIMESTAMP, corredor TEXT, municipio TEXT, modal_predominante TEXT, rodovia_direct_index REAL, porto_direct_index REAL, aeroporto_direct_index REAL, fiscal_direct_index REAL, source_support_index REAL)",
    "regional_monitoring_history": "CREATE TABLE IF NOT EXISTS regional_monitoring_history (snapshot_run_id TEXT, snapshot_captured_at TIMESTAMP, data TIMESTAMP, corredor TEXT, municipio TEXT, municipio_id INTEGER, regiao_imediata TEXT, regiao_intermediaria TEXT, modal_predominante TEXT, rodovias_trafego_index REAL, porto_fila_index REAL, aeroporto_carga_index REAL, fiscal_emissao_index REAL, interdicao_prob REAL, source_support_index REAL, infraestrutura_risk_index_base REAL, infraestrutura_risk_index REAL, impacto_fontes_diretas REAL)",
    "workforce_forecasts_history": "CREATE TABLE IF NOT EXISTS workforce_forecasts_history (snapshot_run_id TEXT, snapshot_captured_at TIMESTAMP, data TIMESTAMP, unidade TEXT, cliente TEXT, tipo_operacao TEXT, turno TEXT, volume_toneladas REAL, cargas_previstas INTEGER, trabalhadores_previstos INTEGER, capacidade_atual INTEGER, gap_previsto INTEGER, sla_previsto REAL, receita_prevista REAL, custo_previsto REAL, margem_prevista REAL, risk_score REAL, acao_recomendada TEXT)",
    "alerts_operacionais_history": "CREATE TABLE IF NOT EXISTS alerts_operacionais_history (snapshot_run_id TEXT, snapshot_captured_at TIMESTAMP, data TIMESTAMP, unidade TEXT, cliente TEXT, tipo_operacao TEXT, turno TEXT, volume_toneladas REAL, cargas_previstas INTEGER, trabalhadores_previstos INTEGER, capacidade_atual INTEGER, gap_previsto INTEGER, sla_previsto REAL, receita_prevista REAL, custo_previsto REAL, margem_prevista REAL, risk_score REAL, acao_recomendada TEXT, alerta TEXT)",
    "model_performance_history": "CREATE TABLE IF NOT EXISTS model_performance_history (run_id TEXT, recorded_at TIMESTAMP, mae REAL, mape REAL, r2 REAL, baseline_mae REAL, baseline_mape REAL, baseline_r2 REAL, train_rows INTEGER, test_rows INTEGER, backtest_folds INTEGER, backtest_mae_mean REAL, backtest_mape_mean REAL, backtest_r2_mean REAL, backtest_baseline_mae_mean REAL, backtest_baseline_mape_mean REAL, backtest_baseline_r2_mean REAL)",
    "model_backtest_folds": "CREATE TABLE IF NOT EXISTS model_backtest_folds (run_id TEXT, fold_id INTEGER, train_end_date TEXT, test_start_date TEXT, test_end_date TEXT, train_rows INTEGER, test_rows INTEGER, mae REAL, mape REAL, r2 REAL, baseline_mae REAL, baseline_mape REAL, baseline_r2 REAL, recorded_at TIMESTAMP)",
    "municipality_catalog": "CREATE TABLE IF NOT EXISTS municipality_catalog (municipio_id INTEGER PRIMARY KEY, municipio TEXT, uf TEXT, microrregiao TEXT, mesorregiao TEXT, regiao_imediata TEXT, regiao_intermediaria TEXT, source_url TEXT, ingestion_method TEXT, collected_at TIMESTAMP)",
    "source_catalog": "CREATE TABLE IF NOT EXISTS source_catalog (source_name TEXT, category TEXT, scope TEXT, provider TEXT, url TEXT, format_hint TEXT, priority TEXT, status TEXT, source_type TEXT, preferred_ingestion_method TEXT, fallback_ingestion_method TEXT, api_url TEXT)",
    "monitored_entities": "CREATE TABLE IF NOT EXISTS monitored_entities (entity_name TEXT, entity_type TEXT, region TEXT, focus TEXT, source_url TEXT)",
    "entity_registry": "CREATE TABLE IF NOT EXISTS entity_registry (entity_name TEXT, entity_group TEXT, region TEXT, importance_score REAL, monitoring_reason TEXT, source_url TEXT)",
    "source_probe": "CREATE TABLE IF NOT EXISTS source_probe (source_name TEXT, source_url TEXT, asset_url TEXT, asset_hint TEXT, status TEXT, collected_at TIMESTAMP, ingestion_method TEXT)",
    "asset_registry": "CREATE TABLE IF NOT EXISTS asset_registry (asset_key TEXT PRIMARY KEY, source_name TEXT, asset_url TEXT, asset_hint TEXT, asset_status TEXT, last_seen_at TIMESTAMP, first_seen_at TIMESTAMP, fetch_ready INTEGER, fetch_priority TEXT, ingestion_method TEXT)",
    "fetched_assets": "CREATE TABLE IF NOT EXISTS fetched_assets (asset_key TEXT PRIMARY KEY, source_name TEXT, asset_url TEXT, asset_hint TEXT, stored_path TEXT, content_type TEXT, content_length INTEGER, checksum TEXT, fetched_at TIMESTAMP, fetch_status TEXT, http_status INTEGER, derived_candidates INTEGER, ingestion_method TEXT)",
    "asset_fetch_log": "CREATE TABLE IF NOT EXISTS asset_fetch_log (fetch_id TEXT PRIMARY KEY, asset_key TEXT, source_name TEXT, asset_url TEXT, fetch_status TEXT, http_status INTEGER, content_type TEXT, content_length INTEGER, stored_path TEXT, checksum TEXT, derived_candidates INTEGER, fetched_at TIMESTAMP, error_detail TEXT, ingestion_method TEXT)",
    "official_api_catalog": "CREATE TABLE IF NOT EXISTS official_api_catalog (source_name TEXT, api_url TEXT, dataset_id TEXT, dataset_name TEXT, dataset_title TEXT, dataset_state TEXT, dataset_url TEXT, organization TEXT, metadata_modified TIMESTAMP, resource_count INTEGER, groups TEXT, tags TEXT, notes_excerpt TEXT, api_status TEXT, fetched_at TIMESTAMP, ingestion_method TEXT)",
    "official_signal_intelligence": "CREATE TABLE IF NOT EXISTS official_signal_intelligence (asset_key TEXT PRIMARY KEY, source_name TEXT, asset_url TEXT, page_title TEXT, content_kind TEXT, link_count INTEGER, download_candidate_count INTEGER, es_mentions INTEGER, logistics_mentions INTEGER, signal_strength REAL, source_relevance TEXT, extracted_at TIMESTAMP)",
    "download_candidates": "CREATE TABLE IF NOT EXISTS download_candidates (candidate_key TEXT PRIMARY KEY, asset_key TEXT, source_name TEXT, asset_url TEXT, candidate_url TEXT, candidate_type TEXT, candidate_label TEXT, priority TEXT, discovered_at TIMESTAMP)",
    "source_registry": "CREATE TABLE IF NOT EXISTS source_registry (source_name TEXT PRIMARY KEY, category TEXT, refresh_minutes INTEGER, priority TEXT, enabled INTEGER, status TEXT, last_success_at TIMESTAMP, last_attempt_at TIMESTAMP, next_run_at TIMESTAMP, reliability_score REAL, source_type TEXT, preferred_ingestion_method TEXT, fallback_ingestion_method TEXT, api_url TEXT, last_ingestion_method TEXT)",
    "source_policy": "CREATE TABLE IF NOT EXISTS source_policy (source_name TEXT PRIMARY KEY, refresh_mode TEXT, refresh_minutes INTEGER, stale_after_minutes INTEGER, retry_limit INTEGER, partial_refresh_enabled INTEGER, owner TEXT)",
    "refresh_queue": "CREATE TABLE IF NOT EXISTS refresh_queue (job_id TEXT PRIMARY KEY, source_name TEXT, job_type TEXT, priority TEXT, status TEXT, attempts INTEGER, enqueued_at TIMESTAMP, started_at TIMESTAMP, finished_at TIMESTAMP, payload TEXT)",
    "ingestion_runs": "CREATE TABLE IF NOT EXISTS ingestion_runs (run_id TEXT PRIMARY KEY, run_type TEXT, status TEXT, started_at TIMESTAMP, finished_at TIMESTAMP, details TEXT)",
    "pipeline_run_summaries": "CREATE TABLE IF NOT EXISTS pipeline_run_summaries (run_id TEXT, status TEXT, started_at TIMESTAMP, finished_at TIMESTAMP, duration_seconds INTEGER, healthy_sources INTEGER, failed_sources INTEGER, stale_sources INTEGER, cached_fallbacks INTEGER, critical_quality_failures INTEGER, quality_warnings INTEGER, recorded_at TIMESTAMP)",
    "pipeline_state": "CREATE TABLE IF NOT EXISTS pipeline_state (state_key TEXT PRIMARY KEY, state_value TEXT, updated_at TIMESTAMP)",
    "data_quality_checks": "CREATE TABLE IF NOT EXISTS data_quality_checks (check_id TEXT PRIMARY KEY, run_id TEXT, check_group TEXT, table_name TEXT, check_name TEXT, status TEXT, severity TEXT, observed_value TEXT, expected_value TEXT, details TEXT, checked_at TIMESTAMP)",
}

TABLE_EXTRA_COLUMNS = {
    "model_performance_history": [
        ("run_id", "TEXT"),
        ("baseline_mae", "REAL"),
        ("baseline_mape", "REAL"),
        ("baseline_r2", "REAL"),
        ("backtest_baseline_mae_mean", "REAL"),
        ("backtest_baseline_mape_mean", "REAL"),
        ("backtest_baseline_r2_mean", "REAL"),
    ],
    "model_backtest_folds": [
        ("run_id", "TEXT"),
        ("baseline_mae", "REAL"),
        ("baseline_mape", "REAL"),
        ("baseline_r2", "REAL"),
    ],
    "regional_monitoring": [
        ("source_support_index", "REAL"),
        ("infraestrutura_risk_index_base", "REAL"),
        ("impacto_fontes_diretas", "REAL"),
    ],
    "regional_monitoring_history": [
        ("source_support_index", "REAL"),
        ("infraestrutura_risk_index_base", "REAL"),
        ("impacto_fontes_diretas", "REAL"),
    ],
}


def _ensure_missing_columns(cursor):
    for table_name, columns in TABLE_EXTRA_COLUMNS.items():
        existing_columns = {
            row[1]
            for row in cursor.execute(f"PRAGMA table_info({table_name})").fetchall()
        }
        for column_name, column_type in columns:
            if column_name not in existing_columns:
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--reset", action="store_true", help="Dropa e recria as tabelas do banco local.")
    args = parser.parse_args()

    ensure_dirs()
    os.makedirs(os.path.dirname(config.SQLITE_DB), exist_ok=True)

    conn = sqlite3.connect(config.SQLITE_DB)
    cursor = conn.cursor()

    if args.reset:
        for table_name in TABLE_DEFINITIONS:
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

    for ddl in TABLE_DEFINITIONS.values():
        cursor.execute(ddl)
    _ensure_missing_columns(cursor)

    conn.commit()
    conn.close()
    print("Database initialized." if args.reset else "Database ensured.")


if __name__ == "__main__":
    main()
