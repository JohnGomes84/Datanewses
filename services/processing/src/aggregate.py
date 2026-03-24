import math
import sqlite3
import hashlib
from pathlib import Path

import numpy as np
import pandas as pd

from shared import config, get_logger

logger = get_logger(__name__)


SHIFT_WEIGHTS = {"manha": 0.44, "tarde": 0.36, "noite": 0.20}
SHIFT_FACTORS = {"manha": 1.0, "tarde": 1.05, "noite": 1.18}
OPERATION_FACTORS = {"carga": 1.02, "descarga": 1.12, "crossdocking": 0.92, "movimentacao interna": 0.98}
UNIT_FACTORS = {"Porto de Vitoria": 1.08, "Terminal de Tubarao": 1.14, "CD Serra": 0.94, "Hub Vila Velha": 0.97, "Patio Aracruz": 1.01}
COST_PER_SHIFT = {"Porto de Vitoria": 198, "Terminal de Tubarao": 212, "CD Serra": 176, "Hub Vila Velha": 182, "Patio Aracruz": 188}
UNIT_CORRIDOR_MAP = {
    "Porto de Vitoria": "BR-101 Sul / Porto de Vitoria",
    "Terminal de Tubarao": "Acesso Tubarao / Praia Mole",
    "CD Serra": "BR-101 Norte / Serra",
    "Hub Vila Velha": "BR-262 / Cariacica-Viana",
    "Patio Aracruz": "Aracruz / Retroarea Industrial",
}
DIRECT_SOURCE_AXES = {
    "ANTT Rodovias": "rodovia",
    "DNIT Dados Abertos": "rodovia",
    "ANTAQ Estatistica": "porto",
    "ANAC Movimentacao Aeroportuaria": "aeroporto",
    "SEFAZ-ES Documentos Fiscais": "fiscal",
    "SEFAZ-ES NF-e Estatisticas": "fiscal",
}


def _build_news_daily_map(news):
    if news.empty:
        return {}
    news_daily = news.groupby("data", as_index=False).agg(news_risk_score=("risk_score", "mean"))
    return dict(zip(news_daily["data"], news_daily["news_risk_score"]))


def _append_entity(entity_rows, name, group, region, score, reason, source_url=""):
    entity_rows.append(
        {
            "entity_name": name,
            "entity_group": group,
            "region": region,
            "importance_score": round(float(score), 2),
            "monitoring_reason": reason,
            "source_url": source_url,
        }
    )


def _normalize_name(value):
    if not isinstance(value, str):
        return ""
    return (
        value.lower()
        .replace("á", "a")
        .replace("à", "a")
        .replace("â", "a")
        .replace("ã", "a")
        .replace("é", "e")
        .replace("ê", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ô", "o")
        .replace("õ", "o")
        .replace("ú", "u")
        .replace("ç", "c")
        .strip()
    )


def _safe_numeric(value, default):
    if pd.isna(value):
        return float(default)
    return float(value)


def _build_direct_infrastructure_signals(official_probes, official_fetches, official_api_catalog, official_intelligence, latest_regional):
    if latest_regional.empty:
        return pd.DataFrame()

    rows = []
    for source_name, axis in DIRECT_SOURCE_AXES.items():
        probe_count = int(len(official_probes[official_probes["source_name"] == source_name])) if not official_probes.empty else 0
        source_fetches = official_fetches[official_fetches["source_name"] == source_name] if not official_fetches.empty else pd.DataFrame()
        fetch_success_count = int(len(source_fetches[source_fetches["fetch_status"] == "success"])) if not source_fetches.empty else 0
        fetch_total_count = int(len(source_fetches)) if not source_fetches.empty else 0
        fetch_success_ratio = (fetch_success_count / fetch_total_count) if fetch_total_count else 0.0
        source_api = official_api_catalog[official_api_catalog["source_name"] == source_name] if not official_api_catalog.empty else pd.DataFrame()
        api_success_count = int(len(source_api[source_api["api_status"] == "success"])) if not source_api.empty and "api_status" in source_api.columns else 0
        source_intel = official_intelligence[official_intelligence["source_name"] == source_name] if not official_intelligence.empty else pd.DataFrame()
        signal_strength = float(source_intel["signal_strength"].mean() / 100) if not source_intel.empty else 0.0
        direct_signal_index = float(
            np.clip(
                fetch_success_ratio * 0.42
                + min(api_success_count, 20) / 20 * 0.18
                + min(probe_count, 20) / 20 * 0.15
                + signal_strength * 0.25,
                0.0,
                1.0,
            )
        )
        rows.append(
            {
                "source_name": source_name,
                "axis": axis,
                "probe_count": probe_count,
                "fetch_success_count": fetch_success_count,
                "api_success_count": api_success_count,
                "signal_strength": round(signal_strength * 100, 2),
                "direct_signal_index": round(direct_signal_index, 4),
            }
        )

    source_frame = pd.DataFrame(rows)
    corridor_rows = []
    latest_date = pd.to_datetime(latest_regional["data"]).max()
    for row in latest_regional.itertuples(index=False):
        road_direct = float(source_frame[source_frame["axis"] == "rodovia"]["direct_signal_index"].mean()) if not source_frame.empty else 0.0
        port_direct = float(source_frame[source_frame["axis"] == "porto"]["direct_signal_index"].mean()) if not source_frame.empty else 0.0
        air_direct = float(source_frame[source_frame["axis"] == "aeroporto"]["direct_signal_index"].mean()) if not source_frame.empty else 0.0
        fiscal_direct = float(source_frame[source_frame["axis"] == "fiscal"]["direct_signal_index"].mean()) if not source_frame.empty else 0.0
        source_support_index = float(
            np.clip(
                row.rodovias_trafego_index * 0.22 * road_direct
                + row.porto_fila_index * 0.22 * port_direct
                + row.aeroporto_carga_index * 0.18 * air_direct
                + row.fiscal_emissao_index * 0.22 * fiscal_direct
                + row.interdicao_prob * 0.16 * road_direct,
                0.0,
                1.0,
            )
        )
        corridor_rows.append(
            {
                "data": latest_date,
                "corredor": row.corredor,
                "municipio": row.municipio,
                "modal_predominante": row.modal_predominante,
                "rodovia_direct_index": round(road_direct, 4),
                "porto_direct_index": round(port_direct, 4),
                "aeroporto_direct_index": round(air_direct, 4),
                "fiscal_direct_index": round(fiscal_direct, 4),
                "source_support_index": round(source_support_index, 4),
            }
        )

    return pd.DataFrame(corridor_rows).sort_values(["corredor"]).reset_index(drop=True)


def _apply_direct_support_to_regional(regional_summary, direct_infrastructure_signals):
    if regional_summary.empty:
        return regional_summary

    regional_summary = regional_summary.copy()
    regional_summary["infraestrutura_risk_index_base"] = (
        regional_summary["rodovias_trafego_index"] * 0.32
        + regional_summary["porto_fila_index"] * 0.28
        + regional_summary["aeroporto_carga_index"] * 0.12
        + regional_summary["fiscal_emissao_index"] * 0.18
        + regional_summary["interdicao_prob"] * 0.10
    ).round(4)
    regional_summary["source_support_index"] = 0.0

    if direct_infrastructure_signals.empty:
        regional_summary["infraestrutura_risk_index"] = regional_summary["infraestrutura_risk_index_base"]
        regional_summary["impacto_fontes_diretas"] = 0.0
        return regional_summary

    regional_summary = regional_summary.merge(
        direct_infrastructure_signals[["data", "corredor", "source_support_index"]],
        on=["data", "corredor"],
        how="left",
        suffixes=("", "_direct"),
    )
    regional_summary["source_support_index"] = regional_summary["source_support_index_direct"].fillna(
        regional_summary["source_support_index"]
    )
    regional_summary = regional_summary.drop(columns=["source_support_index_direct"])
    regional_summary["infraestrutura_risk_index"] = (
        regional_summary["infraestrutura_risk_index_base"] * 0.88
        + regional_summary["source_support_index"] * 0.12
    ).clip(0.0, 1.0).round(4)
    regional_summary["impacto_fontes_diretas"] = (
        regional_summary["infraestrutura_risk_index"] - regional_summary["infraestrutura_risk_index_base"]
    ).round(4)
    return regional_summary


def build_operational_base():
    ops_file = Path(config.DATA_DIR) / "silver" / "operacoes" / "operacoes_logisticas.parquet"
    indicators_file = Path(config.DATA_DIR) / "silver" / "indicadores" / "indicadores_mercado.parquet"
    news_file = Path(config.DATA_DIR) / "silver" / "news" / "noticias_operacionais.parquet"
    regional_file = Path(config.DATA_DIR) / "silver" / "regional" / "regional_signals.parquet"
    source_catalog_file = Path(config.DATA_DIR) / "silver" / "catalog" / "source_catalog.parquet"
    monitored_entities_file = Path(config.DATA_DIR) / "silver" / "catalog" / "monitored_entities.parquet"
    official_probe_file = Path(config.DATA_DIR) / "silver" / "official_sources" / "official_source_probe.parquet"
    official_fetch_file = Path(config.DATA_DIR) / "silver" / "official_asset_fetches" / "official_asset_fetches.parquet"
    official_api_catalog_file = Path(config.DATA_DIR) / "silver" / "official_api" / "official_api_catalog.parquet"
    official_intelligence_file = Path(config.DATA_DIR) / "silver" / "official_asset_intelligence" / "official_signal_intelligence.parquet"
    download_candidates_file = Path(config.DATA_DIR) / "silver" / "official_asset_intelligence" / "download_candidates.parquet"
    ibge_municipios_file = Path(config.DATA_DIR) / "silver" / "ibge" / "municipios_es.parquet"
    gold_path = Path(config.DATA_DIR) / "gold" / "operacional"
    gold_path.mkdir(parents=True, exist_ok=True)

    if not ops_file.exists() or not indicators_file.exists():
        logger.warning("Missing operational source files for aggregation")
        return

    rng = np.random.default_rng(21)
    operations = pd.read_parquet(ops_file)
    indicators = pd.read_parquet(indicators_file)
    news = pd.read_parquet(news_file) if news_file.exists() else pd.DataFrame(columns=["data", "risk_score"])
    regional = pd.read_parquet(regional_file) if regional_file.exists() else pd.DataFrame(columns=["data", "corredor"])
    source_catalog = pd.read_parquet(source_catalog_file) if source_catalog_file.exists() else pd.DataFrame()
    monitored_entities = pd.read_parquet(monitored_entities_file) if monitored_entities_file.exists() else pd.DataFrame()
    official_probes = pd.read_parquet(official_probe_file) if official_probe_file.exists() else pd.DataFrame()
    official_fetches = pd.read_parquet(official_fetch_file) if official_fetch_file.exists() else pd.DataFrame()
    official_api_catalog = pd.read_parquet(official_api_catalog_file) if official_api_catalog_file.exists() else pd.DataFrame()
    official_intelligence = pd.read_parquet(official_intelligence_file) if official_intelligence_file.exists() else pd.DataFrame()
    download_candidates = pd.read_parquet(download_candidates_file) if download_candidates_file.exists() else pd.DataFrame()
    municipality_catalog = pd.read_parquet(ibge_municipios_file) if ibge_municipios_file.exists() else pd.DataFrame()

    operations["data"] = pd.to_datetime(operations["data"])
    indicators["data"] = pd.to_datetime(indicators["data"])
    news["data"] = pd.to_datetime(news["data"]) if not news.empty else pd.to_datetime(pd.Series([], dtype="datetime64[ns]"))
    if not regional.empty:
        regional["data"] = pd.to_datetime(regional["data"])
    if not municipality_catalog.empty:
        municipality_catalog["municipio_key"] = municipality_catalog["municipio"].map(_normalize_name)

    merged = operations.merge(indicators, on="data", how="left")
    merged["corredor"] = merged["unidade"].map(UNIT_CORRIDOR_MAP)
    if not regional.empty:
        merged = merged.merge(
            regional[
                [
                    "data",
                    "corredor",
                    "municipio",
                    "modal_predominante",
                    "rodovias_trafego_index",
                    "porto_fila_index",
                    "aeroporto_carga_index",
                    "fiscal_emissao_index",
                    "interdicao_prob",
                ]
            ],
            on=["data", "corredor"],
            how="left",
        )
    news_map = _build_news_daily_map(news)

    rows = []
    for row in merged.itertuples(index=False):
        news_risk = float(news_map.get(row.data, 0.24))
        road_index = _safe_numeric(getattr(row, "rodovias_trafego_index", None), 0.45)
        port_index = _safe_numeric(getattr(row, "porto_fila_index", None), 0.32)
        airport_index = _safe_numeric(getattr(row, "aeroporto_carga_index", None), 0.18)
        fiscal_index = _safe_numeric(getattr(row, "fiscal_emissao_index", None), 0.5)
        interdicao_prob = _safe_numeric(getattr(row, "interdicao_prob", None), 0.16)
        infra_risk = float(np.clip(road_index * 0.32 + port_index * 0.28 + airport_index * 0.12 + fiscal_index * 0.18 + interdicao_prob * 0.10, 0.05, 1.0))
        day_factor = 1.05 if row.data.day >= 25 else 1.0
        for turno, weight in SHIFT_WEIGHTS.items():
            shift_noise = rng.normal(1.0, 0.05)
            volume_turno = max(35.0, row.volume_toneladas * weight * shift_noise)
            cargas_turno = max(2, int(round(row.cargas_previstas * weight * (1 + fiscal_index * 0.08))))
            absenteismo = float(np.clip(0.028 + row.pressao_mao_obra_index * 0.12 + news_risk * 0.04 + infra_risk * 0.03 + (0.02 if turno == "noite" else 0), 0.02, 0.22))
            productivity = max(7.5, 17 - row.pressao_mao_obra_index * 5 - news_risk * 2 - infra_risk * 1.3 - (0.7 if turno == "noite" else 0))
            required = math.ceil(
                (volume_turno / 24.5) * OPERATION_FACTORS[row.tipo_operacao] * SHIFT_FACTORS[turno] * UNIT_FACTORS[row.unidade]
                + cargas_turno * 0.48
                + row.demanda_externa_index * 2.6
                + road_index * 1.6
                + port_index * (1.8 if "Porto" in row.unidade or "Terminal" in row.unidade else 0.8)
                + fiscal_index * 1.2
                + day_factor
            )
            planned = max(4, int(round(required + rng.integers(-3, 4))))
            presentes = max(3, int(round(planned * (1 - absenteismo))))
            gap = required - planned
            overtime = round(max(gap, 0) * 1.7 + rng.uniform(0, 1.2), 2)
            sla_meta = 0.96
            sla_realizado = float(np.clip(sla_meta - max(gap, 0) * 0.012 - max(absenteismo - 0.08, 0) * 0.35 - infra_risk * 0.05 - (0.02 if row.chuva_mm > 12 else 0), 0.76, 0.995))
            receita = round(volume_turno * row.preco_tonelada, 2)
            custo = round(planned * COST_PER_SHIFT[row.unidade] + overtime * 42 + row.combustivel_index * 110 + road_index * 70 + port_index * 90, 2)
            margem = round(receita - custo, 2)
            risco_operacional = float(np.clip(35 + max(gap, 0) * 9 + absenteismo * 100 + news_risk * 22 + row.chuva_mm * 0.6 + infra_risk * 22, 0, 100))

            rows.append(
                {
                    "data": row.data,
                    "ano": row.data.year,
                    "mes": row.data.month,
                    "dia_semana": row.data.day_name(),
                    "unidade": row.unidade,
                    "cliente": row.cliente,
                    "tipo_operacao": row.tipo_operacao,
                    "turno": turno,
                    "corredor": row.corredor,
                    "municipio": getattr(row, "municipio", None),
                    "modal_predominante": getattr(row, "modal_predominante", None),
                    "volume_toneladas": round(volume_turno, 2),
                    "cargas_previstas": cargas_turno,
                    "demanda_externa_index": round(float(row.demanda_externa_index), 4),
                    "pressao_mao_obra_index": round(float(row.pressao_mao_obra_index), 4),
                    "combustivel_index": round(float(row.combustivel_index), 4),
                    "chuva_mm": round(float(row.chuva_mm), 2),
                    "news_risk_score": round(news_risk, 2),
                    "rodovias_trafego_index": round(road_index, 4),
                    "porto_fila_index": round(port_index, 4),
                    "aeroporto_carga_index": round(airport_index, 4),
                    "fiscal_emissao_index": round(fiscal_index, 4),
                    "interdicao_prob": round(interdicao_prob, 4),
                    "infraestrutura_risk_index": round(infra_risk, 4),
                    "absenteismo_pct": round(absenteismo, 4),
                    "trabalhadores_planejados": planned,
                    "trabalhadores_presentes": presentes,
                    "trabalhadores_necessarios": required,
                    "gap_mao_obra": gap,
                    "horas_extras": overtime,
                    "produtividade_estimada": round(productivity, 2),
                    "sla_meta": sla_meta,
                    "sla_realizado": round(sla_realizado, 4),
                    "receita_estimada": receita,
                    "custo_operacional": custo,
                    "margem_estimada": margem,
                    "risco_operacional": round(risco_operacional, 2),
                }
            )

    operational = pd.DataFrame(rows).sort_values(["data", "unidade", "cliente", "turno"]).reset_index(drop=True)
    operational.to_parquet(gold_path / "operacoes_diarias.parquet", index=False)

    contract_summary = (
        operational.groupby(["cliente", "unidade"], as_index=False)
        .agg(
            receita_total=("receita_estimada", "sum"),
            margem_total=("margem_estimada", "sum"),
            sla_medio=("sla_realizado", "mean"),
            absenteismo_medio=("absenteismo_pct", "mean"),
            risco_medio=("risco_operacional", "mean"),
            equipe_media=("trabalhadores_necessarios", "mean"),
        )
        .sort_values("margem_total", ascending=False)
    )
    contract_summary.to_parquet(gold_path / "resumo_contratos.parquet", index=False)

    if not regional.empty:
        regional_summary = (
            regional.groupby(["data", "corredor", "municipio", "modal_predominante"], as_index=False)
            .agg(
                rodovias_trafego_index=("rodovias_trafego_index", "mean"),
                porto_fila_index=("porto_fila_index", "mean"),
                aeroporto_carga_index=("aeroporto_carga_index", "mean"),
                fiscal_emissao_index=("fiscal_emissao_index", "mean"),
                interdicao_prob=("interdicao_prob", "mean"),
            )
            .sort_values(["data", "corredor"])
        )
        direct_infrastructure_signals = _build_direct_infrastructure_signals(
            official_probes,
            official_fetches,
            official_api_catalog,
            official_intelligence,
            regional_summary[regional_summary["data"] == regional_summary["data"].max()].copy(),
        )
        regional_summary = _apply_direct_support_to_regional(regional_summary, direct_infrastructure_signals)
        if not municipality_catalog.empty:
            regional_summary["municipio_key"] = regional_summary["municipio"].map(_normalize_name)
            regional_summary = regional_summary.merge(
                municipality_catalog[
                    ["municipio_key", "municipio_id", "regiao_imediata", "regiao_intermediaria"]
                ].drop_duplicates(),
                on="municipio_key",
                how="left",
            ).drop(columns=["municipio_key"])
        else:
            regional_summary["municipio_id"] = None
            regional_summary["regiao_imediata"] = None
            regional_summary["regiao_intermediaria"] = None
        regional_summary.to_parquet(gold_path / "regional_monitoring.parquet", index=False)
        if not direct_infrastructure_signals.empty:
            direct_infrastructure_signals.to_parquet(gold_path / "direct_infrastructure_signals.parquet", index=False)
    else:
        direct_infrastructure_signals = pd.DataFrame()

    conn = sqlite3.connect(config.SQLITE_DB)
    operational.to_sql("operations_daily", conn, if_exists="replace", index=False)
    contract_summary.to_sql("contract_summary", conn, if_exists="replace", index=False)
    if not regional.empty:
        regional_summary.to_sql("regional_monitoring", conn, if_exists="replace", index=False)
    if not direct_infrastructure_signals.empty:
        direct_infrastructure_signals.to_sql("direct_infrastructure_signals", conn, if_exists="replace", index=False)
    if not municipality_catalog.empty:
        municipality_catalog.drop(columns=["municipio_key"], errors="ignore").to_sql("municipality_catalog", conn, if_exists="replace", index=False)
    if not source_catalog.empty:
        source_catalog.to_sql("source_catalog", conn, if_exists="replace", index=False)
    if not monitored_entities.empty:
        monitored_entities.to_sql("monitored_entities", conn, if_exists="replace", index=False)
        entity_rows = []
        for row in monitored_entities.itertuples(index=False):
            base_score = 88 if row.entity_type in {"infraestrutura", "municipio_logistico"} else 76
            _append_entity(entity_rows, row.entity_name, row.entity_type, row.region, base_score, row.focus, row.source_url)
    else:
        entity_rows = []

    if not operational.empty:
        operational_rollup = operational.groupby(["cliente", "unidade", "corredor", "municipio"], as_index=False).agg(
            volume_total=("volume_toneladas", "sum"),
            risco_medio=("risco_operacional", "mean"),
            sla_medio=("sla_realizado", "mean"),
        )
        for row in operational_rollup.itertuples(index=False):
            region = row.municipio or "ES"
            _append_entity(entity_rows, row.cliente, "cliente_operacional", region, min(99, 70 + row.volume_total / 1200 + row.risco_medio / 8), "Cliente com operacao recorrente e impacto direto em demanda, margem e SLA")
            _append_entity(entity_rows, row.unidade, "unidade_operacional", region, min(99, 78 + row.risco_medio / 6), "Unidade operacional com exposicao direta a escala, risco e infraestrutura regional")
            _append_entity(entity_rows, row.corredor, "corredor_logistico", region, min(99, 82 + row.risco_medio / 7), "Corredor logistico que condiciona acesso, fila, lead time e capacidade de armazem")

        for row in operational.groupby(["cliente", "turno"], as_index=False).agg(
            volume_total=("volume_toneladas", "sum"),
            risco_medio=("risco_operacional", "mean"),
        ).itertuples(index=False):
            _append_entity(
                entity_rows,
                f"{row.cliente} - turno {row.turno}",
                "cliente_turno",
                "ES",
                min(97, 68 + row.volume_total / 1800 + row.risco_medio / 9),
                "Combina cliente e turno para antecipar demanda de escala e cobertura operacional",
            )

        for row in operational.groupby(["unidade", "turno"], as_index=False).agg(
            risco_medio=("risco_operacional", "mean"),
            margem_media=("margem_estimada", "mean"),
        ).itertuples(index=False):
            _append_entity(
                entity_rows,
                f"{row.unidade} - turno {row.turno}",
                "unidade_turno",
                "ES",
                min(98, 74 + row.risco_medio / 7 + max(row.margem_media, 0) / 15000),
                "Faixa operacional por unidade e turno para monitorar deficit, fila e produtividade",
            )

        for row in operational.groupby(["unidade", "tipo_operacao"], as_index=False).agg(
            risco_medio=("risco_operacional", "mean"),
            volume_total=("volume_toneladas", "sum"),
        ).itertuples(index=False):
            _append_entity(
                entity_rows,
                f"{row.unidade} - {row.tipo_operacao}",
                "unidade_operacao",
                "ES",
                min(97, 73 + row.risco_medio / 8 + row.volume_total / 2500),
                "Faixa de operacao critica por unidade para planejar doca, equipe e janela",
            )

        for row in operational.groupby(["cliente", "tipo_operacao"], as_index=False).agg(
            risco_medio=("risco_operacional", "mean"),
            volume_total=("volume_toneladas", "sum"),
        ).itertuples(index=False):
            _append_entity(
                entity_rows,
                f"{row.cliente} - {row.tipo_operacao}",
                "cliente_operacao",
                "ES",
                min(96, 69 + row.risco_medio / 10 + row.volume_total / 3000),
                "Cliente segmentado por tipo de operacao para previsao de carga, descarga e crossdocking",
            )

        warehouse_kpis = [
            ("pressao_docas", "Pressao sobre docas e janelas operacionais"),
            ("fila_caminhoes", "Fila de caminhoes e lead time de patio"),
            ("ocupacao_armazem", "Ocupacao e pressao sobre armazenagem"),
            ("sla_turno", "Cumprimento de SLA e confiabilidade"),
            ("absenteismo", "Sensibilidade de equipe e disponibilidade"),
            ("throughput", "Ritmo de giro e capacidade de movimentacao do armazem"),
            ("janela_descarga", "Desempenho de descarga e absorcao de picos"),
            ("janela_carga", "Desempenho de carga e liberacao de expedicao"),
        ]
        for row in operational.groupby(["unidade", "municipio"], as_index=False).agg(
            risco_medio=("risco_operacional", "mean"),
            volume_total=("volume_toneladas", "sum"),
        ).itertuples(index=False):
            region = row.municipio or "ES"
            for kpi_name, kpi_reason in warehouse_kpis:
                _append_entity(
                    entity_rows,
                    f"{row.unidade} - {kpi_name}",
                    "kpi_armazem",
                    region,
                    min(95, 72 + row.risco_medio / 8 + row.volume_total / 4000),
                    kpi_reason,
                )

    if not regional.empty:
        regional_clusters = regional.groupby(["municipio", "modal_predominante"], as_index=False).agg(
            infra_risk=("interdicao_prob", "mean"),
            road=("rodovias_trafego_index", "mean"),
            port=("porto_fila_index", "mean"),
            air=("aeroporto_carga_index", "mean"),
            fiscal=("fiscal_emissao_index", "mean"),
        )
        for row in regional_clusters.itertuples(index=False):
            _append_entity(
                entity_rows,
                f"{row.municipio} - {row.modal_predominante}",
                "cluster_regional",
                row.municipio,
                min(99, 74 + row.road * 8 + row.port * 6 + row.air * 4 + row.fiscal * 5),
                "Cluster regional com potencial de pressionar acesso, expedicao e ocupacao de armazens",
            )

        municipality_signals = regional.groupby("municipio", as_index=False).agg(
            road=("rodovias_trafego_index", "mean"),
            port=("porto_fila_index", "mean"),
            air=("aeroporto_carga_index", "mean"),
            fiscal=("fiscal_emissao_index", "mean"),
            interd=("interdicao_prob", "mean"),
        )
        municipality_signal_defs = [
            ("trafego_rodoviario", "road", "Pressao de rodovias para abastecimento e escoamento"),
            ("fila_portuaria", "port", "Fila e saturacao portuaria com reflexo em armazens"),
            ("carga_aeroportuaria", "air", "Pressao de carga aerea e transferencias urgentes"),
            ("emissao_fiscal", "fiscal", "Pulso fiscal de expedicao, vendas e transporte"),
            ("interdicao", "interd", "Risco de restricao e quebra de fluxo logistico"),
            ("torre_controle", "road", "Visao agregada do municipio para acionar contingencia"),
            ("ocupacao_logistica", "fiscal", "Aquecimento logistico e potencial de saturacao regional"),
            ("risco_operacional", "interd", "Sinal composto para acionar reforco tatico"),
        ]
        for row in municipality_signals.itertuples(index=False):
            signal_map = {"road": row.road, "port": row.port, "air": row.air, "fiscal": row.fiscal, "interd": row.interd}
            for signal_name, key, reason in municipality_signal_defs:
                _append_entity(
                    entity_rows,
                    f"{row.municipio} - {signal_name}",
                    "municipio_sinal",
                    row.municipio,
                    min(96, 71 + signal_map[key] * 18),
                    reason,
                )

        corridor_signals = regional.groupby("corredor", as_index=False).agg(
            road=("rodovias_trafego_index", "mean"),
            port=("porto_fila_index", "mean"),
            air=("aeroporto_carga_index", "mean"),
            fiscal=("fiscal_emissao_index", "mean"),
            interd=("interdicao_prob", "mean"),
            municipality=("municipio", "first"),
        )
        corridor_signal_defs = [
            ("rodovia", "road", "Faixa de trafego e fluidez do corredor"),
            ("porto", "port", "Pressao portuaria conectada ao corredor"),
            ("aeroporto", "air", "Impacto de carga urgente e conexao aerea"),
            ("fiscal", "fiscal", "Sinal de expedicao e aquecimento economico"),
            ("interdicao", "interd", "Risco de bloqueio ou lentidao operacional"),
            ("sla", "road", "Impacto potencial em SLA de entrega e coleta"),
            ("capacidade", "port", "Capacidade efetiva do corredor para sustentar o fluxo"),
            ("contingencia", "interd", "Necessidade de rota alternativa ou reforco de janela"),
        ]
        for row in corridor_signals.itertuples(index=False):
            signal_map = {"road": row.road, "port": row.port, "air": row.air, "fiscal": row.fiscal, "interd": row.interd}
            for signal_name, key, reason in corridor_signal_defs:
                _append_entity(
                    entity_rows,
                    f"{row.corredor} - {signal_name}",
                    "corredor_sinal",
                    row.municipality or "ES",
                    min(97, 73 + signal_map[key] * 17),
                    reason,
                )

        logistics_cells = regional.groupby(["municipio", "modal_predominante"], as_index=False).agg(
            infra=("interdicao_prob", "mean"),
            fiscal=("fiscal_emissao_index", "mean"),
        )
        cell_types = [
            ("hub_distribuicao", "Celula regional de distribuicao e reabastecimento"),
            ("retroarea", "Celula de apoio portuario, armazenagem e staging"),
            ("zona_pressao", "Area de pressao operacional para planejamento de contingencia"),
            ("janela_expedicao", "Celula de expedicao para ajuste de janelas e equipes"),
        ]
        for row in logistics_cells.itertuples(index=False):
            for cell_name, reason in cell_types:
                _append_entity(
                    entity_rows,
                    f"{row.municipio} - {row.modal_predominante} - {cell_name}",
                    "celula_logistica",
                    row.municipio,
                    min(95, 70 + row.infra * 14 + row.fiscal * 10),
                    reason,
                )

    if entity_rows:
        entity_registry = pd.DataFrame(entity_rows).drop_duplicates(subset=["entity_name", "entity_group", "region"]).sort_values(
            ["importance_score", "entity_name"], ascending=[False, True]
        )
        entity_registry.to_sql("entity_registry", conn, if_exists="replace", index=False)
    if not official_probes.empty:
        official_probes.to_sql("source_probe", conn, if_exists="replace", index=False)
        asset_registry = official_probes.copy()
        asset_registry["asset_key"] = asset_registry.apply(
            lambda row: hashlib.sha1(f"{row['source_name']}|{row['asset_url']}".encode("utf-8")).hexdigest(),
            axis=1,
        )
        asset_registry["asset_status"] = asset_registry["status"]
        asset_registry["last_seen_at"] = asset_registry["collected_at"]
        asset_registry["first_seen_at"] = asset_registry["collected_at"]
        asset_registry["fetch_ready"] = asset_registry["asset_hint"].isin(["data_file", "dataset_page"]).astype(int)
        asset_registry["fetch_priority"] = asset_registry["asset_hint"].map(
            {"data_file": "alta", "dataset_page": "media", "data_candidate": "baixa"}
        ).fillna("baixa")
        if "ingestion_method" not in asset_registry.columns:
            asset_registry["ingestion_method"] = "probe_fetch"
        asset_registry = asset_registry[
            [
                "asset_key",
                "source_name",
                "asset_url",
                "asset_hint",
                "asset_status",
                "last_seen_at",
                "first_seen_at",
                "fetch_ready",
                "fetch_priority",
                "ingestion_method",
            ]
        ].drop_duplicates(subset=["asset_key"])
        if not official_fetches.empty:
            latest_fetches = official_fetches.sort_values("fetched_at", ascending=False).drop_duplicates(subset=["asset_key"], keep="first")
            asset_registry = asset_registry.merge(
                latest_fetches[["asset_key", "fetch_status", "fetched_at"]],
                on="asset_key",
                how="left",
            )
            asset_registry["asset_status"] = asset_registry["fetch_status"].fillna(asset_registry["asset_status"])
            asset_registry["last_seen_at"] = asset_registry["fetched_at"].fillna(asset_registry["last_seen_at"])
            asset_registry = asset_registry.drop(columns=["fetch_status", "fetched_at"])
        asset_registry.to_sql("asset_registry", conn, if_exists="replace", index=False)
    if not official_fetches.empty:
        official_fetches.to_sql("asset_fetch_log", conn, if_exists="replace", index=False)
        fetched_assets = (
            official_fetches.sort_values("fetched_at", ascending=False)
            .drop_duplicates(subset=["asset_key"], keep="first")
            [
                [
                    "asset_key",
                    "source_name",
                    "asset_url",
                    "stored_path",
                    "content_type",
                    "content_length",
                    "checksum",
                    "fetched_at",
                    "fetch_status",
                    "http_status",
                    "derived_candidates",
                    "ingestion_method",
                ]
            ]
            .copy()
        )
        if not official_probes.empty:
            fetched_assets = fetched_assets.merge(
                official_probes[["source_name", "asset_url", "asset_hint"]].drop_duplicates(),
                on=["source_name", "asset_url"],
                how="left",
            )
        else:
            fetched_assets["asset_hint"] = None
        if "ingestion_method" not in fetched_assets.columns:
            fetched_assets["ingestion_method"] = "probe_fetch"
        fetched_assets = fetched_assets[
            [
                "asset_key",
                "source_name",
                "asset_url",
                "asset_hint",
                "stored_path",
                "content_type",
                "content_length",
                "checksum",
                "fetched_at",
                "fetch_status",
                "http_status",
                "derived_candidates",
                "ingestion_method",
            ]
        ]
        fetched_assets.to_sql("fetched_assets", conn, if_exists="replace", index=False)
    if not official_api_catalog.empty:
        official_api_catalog.to_sql("official_api_catalog", conn, if_exists="replace", index=False)
    if not official_intelligence.empty:
        official_intelligence.to_sql("official_signal_intelligence", conn, if_exists="replace", index=False)
    if not download_candidates.empty:
        download_candidates.to_sql("download_candidates", conn, if_exists="replace", index=False)
    if not news.empty:
        news.sort_values("data", ascending=False).to_sql("news_monitoring", conn, if_exists="replace", index=False)
    conn.close()
    logger.info(f"Operational base aggregated: {len(operational)} shift records")


if __name__ == "__main__":
    build_operational_base()
