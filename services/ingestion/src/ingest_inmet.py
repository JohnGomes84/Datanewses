from pathlib import Path

import pandas as pd
import requests

from shared import config, get_logger, save_parquet

logger = get_logger(__name__)


INMET_FORECAST_URL = "https://apiprevmet3.inmet.gov.br/previsao/{geocode}/3"
CORRIDOR_MAP = [
    {
        "corredor": "BR-101 Sul / Porto de Vitoria",
        "municipio": "Vitoria",
        "modal_predominante": "porto",
        "geocode": 3205309,
        "road_exposure": 0.68,
        "port_exposure": 0.92,
        "air_exposure": 0.18,
    },
    {
        "corredor": "Acesso Tubarao / Praia Mole",
        "municipio": "Vitoria",
        "modal_predominante": "porto",
        "geocode": 3205309,
        "road_exposure": 0.62,
        "port_exposure": 0.96,
        "air_exposure": 0.12,
    },
    {
        "corredor": "BR-101 Norte / Serra",
        "municipio": "Serra",
        "modal_predominante": "rodovia",
        "geocode": 3205002,
        "road_exposure": 0.94,
        "port_exposure": 0.26,
        "air_exposure": 0.14,
    },
    {
        "corredor": "BR-262 / Cariacica-Viana",
        "municipio": "Viana",
        "modal_predominante": "rodovia",
        "geocode": 3205101,
        "road_exposure": 0.88,
        "port_exposure": 0.38,
        "air_exposure": 0.10,
    },
    {
        "corredor": "Aracruz / Retroarea Industrial",
        "municipio": "Aracruz",
        "modal_predominante": "porto",
        "geocode": 3200607,
        "road_exposure": 0.58,
        "port_exposure": 0.86,
        "air_exposure": 0.08,
    },
]
TURN_ORDER = ("manha", "tarde", "noite")
WEATHER_KEYWORDS = {
    "chuva forte": 0.85,
    "tempestade": 0.95,
    "trovoada": 0.9,
    "pancadas de chuva": 0.72,
    "chuva": 0.6,
    "nublado": 0.28,
    "muitas nuvens": 0.24,
    "encoberto": 0.34,
    "nevoeiro": 0.4,
}
WIND_SCALE = {
    "fracos": 0.18,
    "fraco": 0.18,
    "moderados": 0.42,
    "moderado": 0.42,
    "fortes": 0.76,
    "forte": 0.76,
    "muito fortes": 0.92,
    "muito forte": 0.92,
}


def _keyword_score(text):
    normalized = (text or "").strip().lower()
    score = 0.0
    for token, value in WEATHER_KEYWORDS.items():
        if token in normalized:
            score = max(score, value)
    return score


def _wind_score(text):
    normalized = (text or "").strip().lower()
    for token, value in WIND_SCALE.items():
        if token in normalized:
            return value
    return 0.2


def _clip(value, lower=0.02, upper=1.0):
    return round(float(min(max(value, lower), upper)), 4)


def _parse_forecast_days(payload):
    city_payload = next(iter(payload.values()), {})
    rows = []
    for raw_date, turn_payload in city_payload.items():
        date_value = pd.to_datetime(raw_date, format="%d/%m/%Y", errors="coerce")
        if pd.isna(date_value):
            continue
        turns = [turn_payload.get(turn_name, {}) for turn_name in TURN_ORDER if isinstance(turn_payload.get(turn_name), dict)]
        if not turns:
            continue

        summaries = [turn.get("resumo", "") for turn in turns]
        weather_score = max(_keyword_score(summary) for summary in summaries)
        cloud_candidates = [_keyword_score(summary) for summary in summaries if "nuv" in summary.lower()]
        storm_candidates = [_keyword_score(summary) for summary in summaries if any(token in summary.lower() for token in ("trovoada", "tempestade", "chuva forte"))]
        cloud_score = max(cloud_candidates) if cloud_candidates else 0.0
        storm_score = max(storm_candidates) if storm_candidates else 0.0
        wind_score = max(_wind_score(turn.get("int_vento", "")) for turn in turns)
        humidity_max = pd.to_numeric([turn.get("umidade_max", 0) for turn in turns], errors="coerce")
        humidity_min = pd.to_numeric([turn.get("umidade_min", 0) for turn in turns], errors="coerce")
        temp_max = pd.to_numeric([turn.get("temp_max", 0) for turn in turns], errors="coerce")
        temp_min = pd.to_numeric([turn.get("temp_min", 0) for turn in turns], errors="coerce")

        humidity_factor = (pd.Series(humidity_max).fillna(0).mean() or 0) / 100
        heat_factor = max((pd.Series(temp_max).fillna(0).mean() or 0) - 28, 0) / 10

        rows.append(
            {
                "data": date_value.normalize(),
                "forecast_summary": " | ".join(filter(None, summaries)),
                "weather_score": weather_score,
                "cloud_score": cloud_score,
                "storm_score": storm_score,
                "wind_score": wind_score,
                "humidity_factor": round(float(humidity_factor), 4),
                "heat_factor": round(float(min(heat_factor, 1.0)), 4),
                "temp_max_c": round(float(pd.Series(temp_max).fillna(0).mean()), 2),
                "temp_min_c": round(float(pd.Series(temp_min).fillna(0).mean()), 2),
            }
        )
    return rows


def fetch_inmet_regional_forecast():
    session = requests.Session()
    rows = []

    for corridor in CORRIDOR_MAP:
        response = session.get(INMET_FORECAST_URL.format(geocode=corridor["geocode"]), timeout=30)
        response.raise_for_status()
        for day in _parse_forecast_days(response.json()):
            weather_score = day["weather_score"]
            cloud_score = day["cloud_score"]
            storm_score = day["storm_score"]
            wind_score = day["wind_score"]
            humidity_factor = day["humidity_factor"]
            heat_factor = day["heat_factor"]

            road_index = _clip(
                0.12
                + corridor["road_exposure"] * 0.34
                + weather_score * 0.28
                + wind_score * 0.16
                + humidity_factor * 0.1
            )
            port_index = _clip(
                0.08
                + corridor["port_exposure"] * 0.38
                + weather_score * 0.24
                + wind_score * 0.18
                + storm_score * 0.12
            )
            airport_index = _clip(
                0.05
                + corridor["air_exposure"] * 0.32
                + wind_score * 0.3
                + cloud_score * 0.18
                + storm_score * 0.1
            )
            fiscal_index = _clip(0.42 + heat_factor * 0.06 - weather_score * 0.08, lower=0.1, upper=0.8)
            interdicao_prob = _clip(storm_score * 0.46 + weather_score * 0.22 + wind_score * 0.2 + cloud_score * 0.08, lower=0.03, upper=0.95)

            rows.append(
                {
                    "data": day["data"].strftime("%Y-%m-%d"),
                    "corredor": corridor["corredor"],
                    "municipio": corridor["municipio"],
                    "modal_predominante": corridor["modal_predominante"],
                    "rodovias_trafego_index": road_index,
                    "porto_fila_index": port_index,
                    "aeroporto_carga_index": airport_index,
                    "fiscal_emissao_index": fiscal_index,
                    "interdicao_prob": interdicao_prob,
                    "forecast_summary": day["forecast_summary"],
                    "temp_max_c": day["temp_max_c"],
                    "temp_min_c": day["temp_min_c"],
                    "origem": "inmet_municipal_forecast",
                    "source_url": INMET_FORECAST_URL.format(geocode=corridor["geocode"]),
                    "ingestion_method": "api",
                }
            )

    df = pd.DataFrame(rows).sort_values(["data", "corredor"]).reset_index(drop=True)
    save_parquet(df, f"{config.DATA_DIR}/bronze/regional", filename="regional_inmet_forecast.parquet")
    logger.info(f"Saved {len(df)} official INMET regional climate records")
    return df


if __name__ == "__main__":
    fetch_inmet_regional_forecast()
