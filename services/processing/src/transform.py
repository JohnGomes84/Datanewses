import json
from pathlib import Path
import re
from html import unescape
from urllib.parse import urljoin, urlparse

import pandas as pd

from shared import config, get_logger

logger = get_logger(__name__)

NOISE_EXTENSIONS = (".css", ".js", ".png", ".jpg", ".jpeg", ".svg", ".gif", ".ico", ".woff", ".woff2")
DATA_EXTENSIONS = (".csv", ".json", ".xlsx", ".xls", ".zip", ".parquet", ".xml")
ES_TOKENS = ("espirito santo", "vitoria", "vitória", "serra", "vila velha", "cariacica", "viana", "aracruz", "linhares", "colatina", "cachoeiro")
LOGISTICS_TOKENS = ("porto", "rodovia", "aeroporto", "carga", "descarga", "logistica", "armaz", "container", "trafego", "fiscal", "cte", "nfe")


def _normalize_probe_asset(url):
    if not isinstance(url, str):
        return None
    normalized = url.split("#", 1)[0].rstrip("/")
    parsed = urlparse(normalized)
    if parsed.scheme not in {"http", "https"}:
        return None
    lower = normalized.lower()
    if any(token in lower for token in ["whatsapp://", "mailto:", "javascript:", "/portal_css/", "/@@"]):
        return None
    if lower.endswith(NOISE_EXTENSIONS):
        return None
    return normalized


def _classify_probe_asset(url):
    lower = url.lower()
    if lower.endswith(DATA_EXTENSIONS):
        return "data_file"
    if "/dataset/" in lower or "/resource/" in lower:
        return "dataset_page"
    if any(token in lower for token in ["download", "dadosabertos", "dados-abertos", "arquivo", "planilha", "estatistica"]):
        return "data_candidate"
    return "generic_page"


def _strip_html(html):
    return re.sub(r"<[^>]+>", " ", html)


def _extract_title(html):
    match = re.search(r"<title[^>]*>(.*?)</title>", html, flags=re.I | re.S)
    if not match:
        return ""
    return unescape(re.sub(r"\s+", " ", match.group(1)).strip())[:240]


def _extract_links_with_labels(html):
    return re.findall(r"<a[^>]+href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>", html, flags=re.I | re.S)


def _classify_download_candidate(candidate_url):
    lower = candidate_url.lower()
    if any(lower.endswith(ext) for ext in DATA_EXTENSIONS):
        return "download_file", "alta"
    if any(token in lower for token in ["download", "/resource/", "arquivo", "planilha"]):
        return "download_page", "alta"
    if any(token in lower for token in ["dataset", "estatistica", "cte", "nfe", "contagem-trafego", "rodovias"]):
        return "dataset_reference", "media"
    return "generic_reference", "baixa"


def _score_signal(es_mentions, logistics_mentions, link_count, download_candidate_count):
    score = 25 + es_mentions * 4 + logistics_mentions * 2 + min(download_candidate_count, 12) * 3 + min(link_count, 400) / 40
    return round(float(min(score, 100)), 2)


def process_comex():
    bronze_file = Path(config.DATA_DIR) / "bronze" / "comex_exportacao" / "operacoes_logisticas.parquet"
    silver_path = Path(config.DATA_DIR) / "silver" / "operacoes"
    silver_path.mkdir(parents=True, exist_ok=True)

    if not bronze_file.exists():
        logger.warning("No operational demand file found")
        return

    df = pd.read_parquet(bronze_file)
    df["data"] = pd.to_datetime(df["data"])
    df["volume_toneladas"] = pd.to_numeric(df["volume_toneladas"], errors="coerce")
    df["cargas_previstas"] = pd.to_numeric(df["cargas_previstas"], errors="coerce").fillna(0).astype(int)
    df["preco_tonelada"] = pd.to_numeric(df["preco_tonelada"], errors="coerce")
    df = df.dropna(subset=["data", "cliente", "unidade", "tipo_operacao", "volume_toneladas", "preco_tonelada"])
    df = df.sort_values(["data", "unidade", "cliente"]).reset_index(drop=True)
    df.to_parquet(silver_path / "operacoes_logisticas.parquet", index=False)
    logger.info(f"Operational demand transformed: {len(df)} records")


def process_bcb():
    bronze_file = Path(config.DATA_DIR) / "bronze" / "bcb" / "indicadores_mercado.parquet"
    silver_path = Path(config.DATA_DIR) / "silver" / "indicadores"
    silver_path.mkdir(parents=True, exist_ok=True)

    if not bronze_file.exists():
        logger.warning("No market indicators file found")
        return

    df = pd.read_parquet(bronze_file)
    df["data"] = pd.to_datetime(df["data"])
    numeric_cols = ["pressao_mao_obra_index", "demanda_externa_index", "combustivel_index", "chuva_mm"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["data"]).sort_values("data").reset_index(drop=True)
    df.to_parquet(silver_path / "indicadores_mercado.parquet", index=False)
    logger.info(f"Market indicators transformed: {len(df)} records")


def process_news():
    bronze_file = Path(config.DATA_DIR) / "bronze" / "news_raw" / "noticias_operacionais.parquet"
    silver_path = Path(config.DATA_DIR) / "silver" / "news"
    silver_path.mkdir(parents=True, exist_ok=True)

    if not bronze_file.exists():
        logger.warning("No operational news file found")
        return

    df = pd.read_parquet(bronze_file)
    df["data"] = pd.to_datetime(df["data"])
    df["risk_score"] = pd.to_numeric(df["risk_score"], errors="coerce").fillna(0.3)
    df = df.dropna(subset=["data", "titulo"]).sort_values("data", ascending=False).reset_index(drop=True)
    df.to_parquet(silver_path / "noticias_operacionais.parquet", index=False)
    logger.info(f"Operational news transformed: {len(df)} records")


def process_regional():
    bronze_file = Path(config.DATA_DIR) / "bronze" / "regional" / "regional_signals.parquet"
    silver_path = Path(config.DATA_DIR) / "silver" / "regional"
    silver_path.mkdir(parents=True, exist_ok=True)

    if not bronze_file.exists():
        logger.warning("No regional signals file found")
        return

    df = pd.read_parquet(bronze_file)
    df["data"] = pd.to_datetime(df["data"])
    numeric_cols = [
        "rodovias_trafego_index",
        "porto_fila_index",
        "aeroporto_carga_index",
        "fiscal_emissao_index",
        "interdicao_prob",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["data", "corredor", "municipio"]).sort_values(["data", "corredor"]).reset_index(drop=True)
    df.to_parquet(silver_path / "regional_signals.parquet", index=False)
    logger.info(f"Regional signals transformed: {len(df)} records")


def process_catalog():
    bronze_path = Path(config.DATA_DIR) / "bronze" / "catalog"
    silver_path = Path(config.DATA_DIR) / "silver" / "catalog"
    silver_path.mkdir(parents=True, exist_ok=True)

    for filename in ["source_catalog.parquet", "monitored_entities.parquet"]:
        source_file = bronze_path / filename
        if not source_file.exists():
            logger.warning(f"Missing catalog file: {filename}")
            continue
        df = pd.read_parquet(source_file)
        df.to_parquet(silver_path / filename, index=False)
    logger.info("Catalog transformed")


def process_ibge_localities():
    bronze_file = Path(config.DATA_DIR) / "bronze" / "ibge" / "municipios_es.parquet"
    silver_path = Path(config.DATA_DIR) / "silver" / "ibge"
    silver_path.mkdir(parents=True, exist_ok=True)

    if not bronze_file.exists():
        logger.warning("Missing IBGE locality file")
        return

    df = pd.read_parquet(bronze_file)
    df["municipio_id"] = pd.to_numeric(df["municipio_id"], errors="coerce").astype("Int64")
    df["collected_at"] = pd.to_datetime(df["collected_at"], format="ISO8601", errors="coerce")
    df = df.dropna(subset=["municipio_id", "municipio"]).sort_values("municipio").reset_index(drop=True)
    df.to_parquet(silver_path / "municipios_es.parquet", index=False)
    logger.info(f"IBGE localities transformed: {len(df)} records")


def process_official_probes():
    bronze_path = Path(config.DATA_DIR) / "bronze" / "official_sources"
    silver_path = Path(config.DATA_DIR) / "silver" / "official_sources"
    silver_path.mkdir(parents=True, exist_ok=True)

    files = list(bronze_path.glob("*.parquet"))
    if not files:
        logger.warning("Missing official source probe file")
        return

    df = pd.concat([pd.read_parquet(file) for file in files], ignore_index=True)
    df["collected_at"] = pd.to_datetime(df["collected_at"], format="ISO8601", errors="coerce")
    df["asset_url"] = df["asset_url"].map(_normalize_probe_asset)
    df = df.dropna(subset=["asset_url"])
    df["asset_hint"] = df["asset_url"].map(_classify_probe_asset)
    df = (
        df.dropna(subset=["source_name", "source_url", "asset_url"])
        .sort_values(["source_name", "asset_url", "collected_at"], ascending=[True, True, False])
        .drop_duplicates(subset=["source_name", "asset_url"], keep="first")
        .query("asset_hint != 'generic_page'")
        .reset_index(drop=True)
    )
    df.to_parquet(silver_path / "official_source_probe.parquet", index=False)
    logger.info(f"Official source probes transformed: {len(df)} records")


def process_official_asset_fetches():
    bronze_path = Path(config.DATA_DIR) / "bronze" / "official_asset_fetches"
    silver_path = Path(config.DATA_DIR) / "silver" / "official_asset_fetches"
    silver_path.mkdir(parents=True, exist_ok=True)

    files = list(bronze_path.glob("*.parquet"))
    if not files:
        logger.warning("Missing official asset fetch files")
        return

    df = pd.concat([pd.read_parquet(file) for file in files], ignore_index=True)
    df["fetched_at"] = pd.to_datetime(df["fetched_at"], format="ISO8601", errors="coerce")
    df["content_length"] = pd.to_numeric(df["content_length"], errors="coerce").fillna(0).astype(int)
    df["http_status"] = pd.to_numeric(df["http_status"], errors="coerce")
    df["derived_candidates"] = pd.to_numeric(df["derived_candidates"], errors="coerce").fillna(0).astype(int)
    df = (
        df.dropna(subset=["fetch_id", "asset_key", "source_name", "asset_url", "fetched_at"])
        .sort_values(["asset_key", "fetched_at"], ascending=[True, False])
        .drop_duplicates(subset=["asset_key"], keep="first")
        .reset_index(drop=True)
    )
    df.to_parquet(silver_path / "official_asset_fetches.parquet", index=False)
    logger.info(f"Official asset fetches transformed: {len(df)} records")


def process_official_api_catalog():
    bronze_path = Path(config.DATA_DIR) / "bronze" / "official_api"
    silver_path = Path(config.DATA_DIR) / "silver" / "official_api"
    silver_path.mkdir(parents=True, exist_ok=True)

    files = list(bronze_path.glob("*.parquet"))
    if not files:
        logger.warning("Missing official API catalog files")
        return

    df = pd.concat([pd.read_parquet(file) for file in files], ignore_index=True)
    df["fetched_at"] = pd.to_datetime(df["fetched_at"], format="ISO8601", errors="coerce")
    df["resource_count"] = pd.to_numeric(df["resource_count"], errors="coerce").fillna(0).astype(int)
    df = (
        df.dropna(subset=["source_name", "api_url", "fetched_at"])
        .sort_values(["source_name", "dataset_id", "fetched_at"], ascending=[True, True, False])
        .drop_duplicates(subset=["source_name", "dataset_id"], keep="first")
        .reset_index(drop=True)
    )
    df.to_parquet(silver_path / "official_api_catalog.parquet", index=False)
    logger.info(f"Official API catalog transformed: {len(df)} records")


def process_official_asset_intelligence():
    fetch_file = Path(config.DATA_DIR) / "silver" / "official_asset_fetches" / "official_asset_fetches.parquet"
    silver_path = Path(config.DATA_DIR) / "silver" / "official_asset_intelligence"
    silver_path.mkdir(parents=True, exist_ok=True)

    if not fetch_file.exists():
        logger.warning("Missing official asset fetches for intelligence extraction")
        return

    fetches = pd.read_parquet(fetch_file)
    success_fetches = fetches[fetches["fetch_status"] == "success"].copy()
    if success_fetches.empty:
        logger.warning("No successful official asset fetches for intelligence extraction")
        return

    intel_rows = []
    candidate_rows = []
    extracted_at = pd.Timestamp.now()

    for row in success_fetches.itertuples(index=False):
        stored_path = Path(row.stored_path)
        if not stored_path.exists():
            continue

        raw_bytes = stored_path.read_bytes()
        text = raw_bytes.decode("utf-8", errors="ignore")
        content_type = (row.content_type or "").lower()
        link_count = 0
        download_candidate_count = 0
        page_title = ""
        content_kind = "binary"

        if "html" in content_type or stored_path.suffix.lower() in {".html", ".php", ".br"}:
            content_kind = "html"
            page_title = _extract_title(text)
            hrefs = _extract_links_with_labels(text)
            link_count = len(hrefs)
            seen_candidates = set()
            for href, label in hrefs:
                candidate_url = urljoin(row.asset_url, unescape(href))
                parsed = urlparse(candidate_url)
                if parsed.scheme not in {"http", "https"}:
                    continue
                if candidate_url in seen_candidates:
                    continue
                candidate_type, priority = _classify_download_candidate(candidate_url)
                if candidate_type == "generic_reference":
                    continue
                seen_candidates.add(candidate_url)
                clean_label = unescape(re.sub(r"<[^>]+>", " ", label))
                clean_label = re.sub(r"\s+", " ", clean_label).strip()[:180]
                candidate_rows.append(
                    {
                        "candidate_key": f"{row.asset_key}:{len(seen_candidates)}",
                        "asset_key": row.asset_key,
                        "source_name": row.source_name,
                        "asset_url": row.asset_url,
                        "candidate_url": candidate_url,
                        "candidate_type": candidate_type,
                        "candidate_label": clean_label,
                        "priority": priority,
                        "discovered_at": extracted_at,
                    }
                )
            download_candidate_count = len(seen_candidates)
            lowered_text = _strip_html(text).lower()
        elif "json" in content_type or stored_path.suffix.lower() == ".json":
            content_kind = "json"
            try:
                parsed_json = json.loads(text)
                lowered_text = json.dumps(parsed_json, ensure_ascii=False).lower()
                link_count = len(parsed_json) if isinstance(parsed_json, (dict, list)) else 0
            except Exception:
                lowered_text = text.lower()
        else:
            lowered_text = text.lower()

        es_mentions = sum(lowered_text.count(token) for token in ES_TOKENS)
        logistics_mentions = sum(lowered_text.count(token) for token in LOGISTICS_TOKENS)
        signal_strength = _score_signal(es_mentions, logistics_mentions, link_count, download_candidate_count)
        source_relevance = "alta" if signal_strength >= 70 else "media" if signal_strength >= 45 else "baixa"

        intel_rows.append(
            {
                "asset_key": row.asset_key,
                "source_name": row.source_name,
                "asset_url": row.asset_url,
                "page_title": page_title,
                "content_kind": content_kind,
                "link_count": int(link_count),
                "download_candidate_count": int(download_candidate_count),
                "es_mentions": int(es_mentions),
                "logistics_mentions": int(logistics_mentions),
                "signal_strength": signal_strength,
                "source_relevance": source_relevance,
                "extracted_at": extracted_at,
            }
        )

    intel_df = pd.DataFrame(intel_rows)
    if not intel_df.empty:
        intel_df = intel_df.sort_values(["signal_strength", "source_name"], ascending=[False, True]).drop_duplicates(subset=["asset_key"])
        intel_df.to_parquet(silver_path / "official_signal_intelligence.parquet", index=False)
        logger.info(f"Official asset intelligence transformed: {len(intel_df)} records")
    else:
        logger.warning("No official asset intelligence extracted")

    candidate_df = pd.DataFrame(candidate_rows)
    if not candidate_df.empty:
        candidate_df = candidate_df.drop_duplicates(subset=["candidate_key"]).sort_values(["priority", "source_name", "candidate_url"])
        candidate_df.to_parquet(silver_path / "download_candidates.parquet", index=False)
        logger.info(f"Download candidates transformed: {len(candidate_df)} records")
    else:
        logger.warning("No download candidates extracted from official assets")


if __name__ == "__main__":
    process_comex()
    process_bcb()
    process_news()
    process_regional()
    process_catalog()
    process_official_probes()
    process_official_asset_fetches()
    process_official_api_catalog()
    process_official_asset_intelligence()
