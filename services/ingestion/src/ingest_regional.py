from pathlib import Path

import numpy as np
import pandas as pd

from shared import config, get_logger, save_parquet

logger = get_logger(__name__)

UNIT_CORRIDOR_MAP = {
    "Porto de Vitoria": {"corredor": "BR-101 Sul / Porto de Vitoria", "municipio": "Vitoria", "modal": "porto", "road_bias": 0.62, "port_bias": 0.82, "air_bias": 0.08},
    "Terminal de Tubarao": {"corredor": "Acesso Tubarao / Praia Mole", "municipio": "Vitoria", "modal": "porto", "road_bias": 0.58, "port_bias": 0.88, "air_bias": 0.06},
    "CD Serra": {"corredor": "BR-101 Norte / Serra", "municipio": "Serra", "modal": "rodovia", "road_bias": 0.86, "port_bias": 0.24, "air_bias": 0.12},
    "Hub Vila Velha": {"corredor": "BR-262 / Cariacica-Viana", "municipio": "Viana", "modal": "rodovia", "road_bias": 0.78, "port_bias": 0.34, "air_bias": 0.1},
    "Patio Aracruz": {"corredor": "Aracruz / Retroarea Industrial", "municipio": "Aracruz", "modal": "porto", "road_bias": 0.56, "port_bias": 0.74, "air_bias": 0.05},
}


def _resolve_partial_corridors(payload):
    if not isinstance(payload, dict) or payload.get("scope") != "entity":
        return None
    corridors = set()
    unidade = payload.get("unidade")
    corredor = payload.get("corredor")
    if unidade and unidade in UNIT_CORRIDOR_MAP:
        corridors.add(UNIT_CORRIDOR_MAP[unidade]["corredor"])
    if corredor:
        corridors.add(corredor)
    return corridors or None


def _normalize(series, lower=0.12, upper=1.0):
    min_val = series.min()
    max_val = series.max()
    if pd.isna(min_val) or pd.isna(max_val) or max_val == min_val:
        return pd.Series(np.full(len(series), lower), index=series.index)
    scaled = (series - min_val) / (max_val - min_val)
    return lower + scaled * (upper - lower)


def fetch_regional_signals(payload=None):
    demand_file = Path(config.DATA_DIR) / "bronze" / "comex_exportacao" / "operacoes_logisticas.parquet"
    news_file = Path(config.DATA_DIR) / "bronze" / "news_raw" / "noticias_operacionais.parquet"
    inmet_file = Path(config.DATA_DIR) / "bronze" / "regional" / "regional_inmet_forecast.parquet"
    if not demand_file.exists():
        raise FileNotFoundError("Operational demand base not found for regional monitoring")

    demand = pd.read_parquet(demand_file)
    demand["data"] = pd.to_datetime(demand["data"])
    demand["volume_toneladas"] = pd.to_numeric(demand["volume_toneladas"], errors="coerce").fillna(0.0)
    demand["cargas_previstas"] = pd.to_numeric(demand["cargas_previstas"], errors="coerce").fillna(0.0)
    demand["corredor"] = demand["unidade"].map(lambda value: UNIT_CORRIDOR_MAP[value]["corredor"])
    demand["municipio"] = demand["unidade"].map(lambda value: UNIT_CORRIDOR_MAP[value]["municipio"])
    demand["modal_predominante"] = demand["unidade"].map(lambda value: UNIT_CORRIDOR_MAP[value]["modal"])

    daily = (
        demand.groupby(["data", "corredor", "municipio", "modal_predominante"], as_index=False)
        .agg(
            volume_toneladas=("volume_toneladas", "sum"),
            cargas_previstas=("cargas_previstas", "sum"),
        )
        .sort_values(["corredor", "data"])
    )

    if news_file.exists():
        news = pd.read_parquet(news_file)
        news["data"] = pd.to_datetime(news["data"])
        theme_daily = news.groupby(["data", "tema"], as_index=False)["risk_score"].mean()
        news_map = {
            tema: theme_daily[theme_daily["tema"] == tema].set_index("data")["risk_score"]
            for tema in theme_daily["tema"].unique()
        }
    else:
        news_map = {}

    if inmet_file.exists():
        inmet = pd.read_parquet(inmet_file)
        inmet["data"] = pd.to_datetime(inmet["data"])
        inmet = inmet.sort_values(["data", "corredor"]).drop_duplicates(subset=["data", "corredor"], keep="last")
        inmet = inmet[
            [
                "data",
                "corredor",
                "rodovias_trafego_index",
                "porto_fila_index",
                "aeroporto_carga_index",
                "interdicao_prob",
                "temp_max_c",
                "forecast_summary",
            ]
        ].rename(
            columns={
                "rodovias_trafego_index": "inmet_rodovias_index",
                "porto_fila_index": "inmet_porto_index",
                "aeroporto_carga_index": "inmet_aeroporto_index",
                "interdicao_prob": "inmet_interdicao_prob",
            }
        )
    else:
        inmet = pd.DataFrame()

    rows = []
    for corridor_name, frame in daily.groupby("corredor"):
        frame = frame.sort_values("data").copy()
        meta = next(value for value in UNIT_CORRIDOR_MAP.values() if value["corredor"] == corridor_name)
        if not inmet.empty:
            frame = frame.merge(inmet[inmet["corredor"] == corridor_name], on=["data", "corredor"], how="left")
        frame["volume_norm"] = _normalize(frame["volume_toneladas"])
        frame["cargas_norm"] = _normalize(frame["cargas_previstas"], lower=0.08, upper=0.95)
        frame["porto_news"] = frame["data"].map(news_map.get("porto", pd.Series(dtype=float))).fillna(0.0)
        frame["aero_news"] = frame["data"].map(news_map.get("aeroporto", pd.Series(dtype=float))).fillna(0.0)
        frame["infra_news"] = frame["data"].map(news_map.get("infraestrutura", pd.Series(dtype=float))).fillna(0.0)
        frame["demanda_news"] = frame["data"].map(news_map.get("demanda", pd.Series(dtype=float))).fillna(0.0)
        climate_road = frame.get("inmet_rodovias_index", pd.Series(0.0, index=frame.index)).fillna(0.0)
        climate_port = frame.get("inmet_porto_index", pd.Series(0.0, index=frame.index)).fillna(0.0)
        climate_air = frame.get("inmet_aeroporto_index", pd.Series(0.0, index=frame.index)).fillna(0.0)
        climate_interd = frame.get("inmet_interdicao_prob", pd.Series(0.0, index=frame.index)).fillna(0.0)
        temp_max = frame.get("temp_max_c", pd.Series(0.0, index=frame.index)).fillna(0.0)

        frame["rodovias_trafego_index"] = np.clip(meta["road_bias"] * 0.55 + frame["cargas_norm"] * 0.35 + frame["infra_news"] * 0.18 + climate_road * 0.12, 0.08, 1.0)
        frame["porto_fila_index"] = np.clip(meta["port_bias"] * 0.5 + frame["volume_norm"] * 0.32 + frame["porto_news"] * 0.26 + climate_port * 0.14, 0.05, 1.0)
        frame["aeroporto_carga_index"] = np.clip(meta["air_bias"] * 0.4 + frame["aero_news"] * 0.6 + frame["demanda_news"] * 0.12 + climate_air * 0.1, 0.02, 1.0)
        frame["fiscal_emissao_index"] = np.clip(frame["volume_norm"] * 0.72 + frame["demanda_news"] * 0.2 - (climate_interd * 0.06) + np.clip((temp_max - 30) / 20, 0, 0.08), 0.08, 1.0)
        frame["interdicao_prob"] = np.clip(frame["infra_news"] * 0.52 + frame["porto_news"] * 0.18 + frame["aero_news"] * 0.08 + climate_interd * 0.22, 0.03, 0.95)
        frame["origem"] = "operational_signals_with_inmet"
        rows.append(frame)

    df = pd.concat(rows, ignore_index=True)[
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
            "origem",
        ]
    ].copy()

    scoped_corridors = _resolve_partial_corridors(payload)
    if scoped_corridors:
        cutoff_date = pd.Timestamp.today().normalize() - pd.Timedelta(days=21)
        df = df[(df["corredor"].isin(scoped_corridors)) & (pd.to_datetime(df["data"]) >= cutoff_date)].copy()

    if df.empty:
        logger.info("Regional monitoring partial refresh skipped because no scoped records were available")
        return df

    df["data"] = pd.to_datetime(df["data"]).dt.strftime("%Y-%m-%d")
    save_parquet(df, f"{config.DATA_DIR}/bronze/regional", filename="regional_signals.parquet")
    logger.info(f"Saved {len(df)} regional monitoring records derived from real signals")
    return df
