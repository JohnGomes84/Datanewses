from datetime import datetime
import hashlib
import mimetypes
from pathlib import Path
import re
import uuid
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests

from shared import config, get_logger, save_parquet

try:
    from bs4 import BeautifulSoup
except ImportError:  # pragma: no cover
    BeautifulSoup = None

logger = get_logger(__name__)


SOURCE_URLS = {
    "ANTT Rodovias": "https://dados.antt.gov.br/group/rodovias",
    "DNIT Dados Abertos": "https://servicos.dnit.gov.br/dadosabertos/",
    "ANTAQ Estatistica": "https://www.gov.br/antaq/pt-br/assuntos/estatistica",
    "ANAC Movimentacao Aeroportuaria": "https://www.anac.gov.br/acesso-a-informacao/dados-abertos/areas-de-atuacao/operador-aeroportuario/dados-de-movimentacao-aeroportuaria",
    "SEFAZ-ES Documentos Fiscais": "https://sefaz.es.gov.br/GrupodeArquivos/base-de-dados-documentos-fiscais",
    "SEFAZ-ES NF-e Estatisticas": "https://internet.sefaz.es.gov.br/informacoes/nfe/estatisticas.php",
}

CKAN_API_SOURCES = {
    "ANTT Rodovias": {
        "api_url": "https://dados.antt.gov.br/api/3/action/package_search",
        "params": {"rows": 100, "fq": "groups:rodovias"},
    },
    "DNIT Dados Abertos": {
        "api_url": "https://servicos.dnit.gov.br/dadosabertos/api/3/action/package_search",
        "params": {"rows": 100, "fq": "organization:dnit"},
    },
}


NOISE_EXTENSIONS = {".css", ".js", ".png", ".jpg", ".jpeg", ".svg", ".gif", ".ico", ".woff", ".woff2"}
DATA_EXTENSIONS = {".csv", ".json", ".xlsx", ".xls", ".zip", ".parquet", ".xml"}
DATASET_HINTS = ["dataset", "resource", "download", "arquivo", "dados-abertos", "dadosabertos", "planilha", "estatistica"]
FETCHABLE_HINTS = {"data_file", "dataset_page", "data_candidate"}
MAX_FETCH_BYTES = 2 * 1024 * 1024


def _extract_links(html):
    if BeautifulSoup is None:
        hrefs = re.findall(r'href=["\']([^"\']+)["\']', html, flags=re.IGNORECASE)
        return hrefs[:50]
    soup = BeautifulSoup(html, "html.parser")
    hrefs = []
    for link in soup.find_all("a", href=True):
        hrefs.append(link["href"])
    return hrefs[:100]


def _classify_link(base_url, href):
    absolute_url = urljoin(base_url, href)
    parsed = urlparse(absolute_url)
    if parsed.scheme not in {"http", "https"}:
        return absolute_url, "noise_scheme", False

    absolute_url = absolute_url.split("#", 1)[0].rstrip("/")
    parsed = urlparse(absolute_url)
    path_lower = parsed.path.lower()
    if any(token in absolute_url.lower() for token in ["whatsapp://", "mailto:", "javascript:", "/portal_css/", "/@@"]):
        return absolute_url, "noise_link", False

    for ext in NOISE_EXTENSIONS:
        if path_lower.endswith(ext):
            return absolute_url, "noise_static", False

    for ext in DATA_EXTENSIONS:
        if path_lower.endswith(ext):
            return absolute_url, "data_file", True

    if any(token in absolute_url.lower() for token in DATASET_HINTS):
        if "/dataset/" in path_lower or "/resource/" in path_lower:
            return absolute_url, "dataset_page", True
        return absolute_url, "data_candidate", True

    return absolute_url, "generic_page", False


def _detect_extension(asset_url, content_type):
    parsed = urlparse(asset_url)
    suffix = Path(parsed.path).suffix.lower()
    if suffix:
        return suffix
    guessed = mimetypes.guess_extension((content_type or "").split(";")[0].strip())
    if guessed:
        return guessed
    if "html" in (content_type or "").lower():
        return ".html"
    if "json" in (content_type or "").lower():
        return ".json"
    return ".bin"


def _extract_nested_candidates(base_url, content_type, raw_bytes):
    if "html" not in (content_type or "").lower():
        return []
    try:
        html = raw_bytes.decode("utf-8", errors="ignore")
    except Exception:
        return []
    nested = []
    for href in _extract_links(html):
        asset_url, asset_hint, keep = _classify_link(base_url, href)
        if keep:
            nested.append({"asset_url": asset_url, "asset_hint": asset_hint})
    unique = []
    seen = set()
    for row in nested:
        if row["asset_url"] in seen:
            continue
        seen.add(row["asset_url"])
        unique.append(row)
    return unique[:25]


def probe_source(source_name):
    url = SOURCE_URLS[source_name]
    collected_at = datetime.now().isoformat()
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        html = response.text
        links = _extract_links(html)
        records = []
        seen = set()
        for href in links:
            asset_url, asset_hint, keep = _classify_link(url, href)
            if not keep or asset_url in seen:
                continue
            seen.add(asset_url)
            records.append(
                {
                    "source_name": source_name,
                    "source_url": url,
                    "asset_url": asset_url,
                    "asset_hint": asset_hint,
                    "status": "reachable",
                    "collected_at": collected_at,
                    "ingestion_method": "probe_fetch",
                }
            )
        if not records:
            records.append(
                {
                    "source_name": source_name,
                    "source_url": url,
                    "asset_url": url,
                    "asset_hint": "page_only",
                    "status": "reachable",
                    "collected_at": collected_at,
                    "ingestion_method": "probe_fetch",
                }
            )
    except Exception as exc:
        records = [
            {
                "source_name": source_name,
                "source_url": url,
                "asset_url": url,
                "asset_hint": "fallback_catalog",
                "status": f"unreachable:{exc.__class__.__name__}",
                "collected_at": collected_at,
                "ingestion_method": "probe_fetch",
            }
        ]
    return pd.DataFrame(records)


def probe_source_and_store(source_name):
    df = probe_source(source_name)
    save_parquet(df, f"{config.DATA_DIR}/bronze/official_sources", filename=f"official_probe_{source_name.lower().replace(' ', '_').replace('/', '_')}.parquet")
    logger.info(f"Stored official probe for {source_name}: {len(df)} records")
    return df


def probe_all_official_sources():
    frames = [probe_source(source_name) for source_name in SOURCE_URLS]
    df = pd.concat(frames, ignore_index=True)
    save_parquet(df, f"{config.DATA_DIR}/bronze/official_sources", filename="official_source_probe.parquet")
    logger.info(f"Saved {len(df)} official source probe records")
    return df


def fetch_assets_from_probe(source_name, probe_df, limit=3, asset_urls=None):
    if probe_df is None or probe_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    candidates = probe_df[probe_df["asset_hint"].isin(FETCHABLE_HINTS)].copy()
    if asset_urls:
        candidates = candidates[candidates["asset_url"].isin(asset_urls)].copy()
    if candidates.empty:
        return pd.DataFrame(), pd.DataFrame()

    priority_map = {"data_file": 0, "dataset_page": 1, "data_candidate": 2}
    candidates["priority"] = candidates["asset_hint"].map(priority_map).fillna(9)
    candidates = candidates.sort_values(["priority", "asset_url"]).head(limit)

    fetch_rows = []
    derived_rows = []
    safe_source = source_name.lower().replace(" ", "_").replace("/", "_")
    asset_dir = Path(config.DATA_DIR) / "bronze" / "official_assets" / safe_source
    asset_dir.mkdir(parents=True, exist_ok=True)

    for row in candidates.itertuples(index=False):
        fetch_id = str(uuid.uuid4())
        fetched_at = datetime.now().isoformat()
        asset_key = hashlib.sha1(f"{source_name}|{row.asset_url}".encode("utf-8")).hexdigest()
        try:
            response = requests.get(row.asset_url, timeout=45, stream=True, headers={"User-Agent": "nowcasting-ai/1.0"})
            http_status = response.status_code
            response.raise_for_status()

            chunks = []
            content_length = 0
            for chunk in response.iter_content(chunk_size=65536):
                if not chunk:
                    continue
                chunks.append(chunk)
                content_length += len(chunk)
                if content_length >= MAX_FETCH_BYTES:
                    break
            raw_bytes = b"".join(chunks)
            content_type = response.headers.get("Content-Type", "application/octet-stream")
            checksum = hashlib.sha1(raw_bytes).hexdigest()
            extension = _detect_extension(row.asset_url, content_type)
            stored_path = asset_dir / f"{asset_key}{extension}"
            stored_path.write_bytes(raw_bytes)
            nested = _extract_nested_candidates(row.asset_url, content_type, raw_bytes)

            fetch_rows.append(
                {
                    "fetch_id": fetch_id,
                    "asset_key": asset_key,
                    "source_name": source_name,
                    "asset_url": row.asset_url,
                    "fetch_status": "success",
                    "http_status": http_status,
                    "content_type": content_type,
                    "content_length": content_length,
                    "stored_path": str(stored_path),
                    "checksum": checksum,
                    "derived_candidates": len(nested),
                    "fetched_at": fetched_at,
                    "error_detail": "",
                    "ingestion_method": "probe_fetch",
                }
            )
            for nested_row in nested:
                derived_rows.append(
                    {
                        "source_name": source_name,
                        "source_url": row.asset_url,
                        "asset_url": nested_row["asset_url"],
                        "asset_hint": nested_row["asset_hint"],
                    "status": "derived_from_fetch",
                    "collected_at": fetched_at,
                    "ingestion_method": "probe_fetch",
                }
            )
        except Exception as exc:
            fetch_rows.append(
                {
                    "fetch_id": fetch_id,
                    "asset_key": asset_key,
                    "source_name": source_name,
                    "asset_url": row.asset_url,
                    "fetch_status": "failed",
                    "http_status": None,
                    "content_type": "",
                    "content_length": 0,
                    "stored_path": "",
                    "checksum": "",
                    "derived_candidates": 0,
                    "fetched_at": fetched_at,
                    "error_detail": f"{exc.__class__.__name__}: {exc}",
                    "ingestion_method": "probe_fetch",
                }
            )

    fetch_df = pd.DataFrame(fetch_rows)
    if not fetch_df.empty:
        save_parquet(
            fetch_df,
            f"{config.DATA_DIR}/bronze/official_asset_fetches",
            filename=f"official_fetch_{safe_source}.parquet",
        )

    derived_df = pd.DataFrame(derived_rows)
    if not derived_df.empty:
        combined = pd.concat([probe_df, derived_df], ignore_index=True)
        save_parquet(
            combined,
            f"{config.DATA_DIR}/bronze/official_sources",
            filename=f"official_probe_{safe_source}.parquet",
        )

    logger.info(f"Fetched {len(fetch_df)} official assets for {source_name}")
    return fetch_df, derived_df


def fetch_ckan_catalog(source_name):
    config_row = CKAN_API_SOURCES.get(source_name)
    if config_row is None:
        return pd.DataFrame()

    fetched_at = datetime.now().isoformat()
    try:
        response = requests.get(
            config_row["api_url"],
            params=config_row["params"],
            timeout=45,
            headers={"User-Agent": "nowcasting-ai/1.0"},
        )
        response.raise_for_status()
        payload = response.json()
        results = ((payload or {}).get("result") or {}).get("results") or []
        records = []
        for item in results:
            resources = item.get("resources") or []
            groups = item.get("groups") or []
            tags = item.get("tags") or []
            dataset_url = item.get("url") or f"{SOURCE_URLS[source_name].rstrip('/')}/dataset/{item.get('name', '')}"
            records.append(
                {
                    "source_name": source_name,
                    "api_url": config_row["api_url"],
                    "dataset_id": item.get("id"),
                    "dataset_name": item.get("name"),
                    "dataset_title": item.get("title"),
                    "dataset_state": item.get("state"),
                    "dataset_url": dataset_url,
                    "organization": (item.get("organization") or {}).get("title") or (item.get("organization") or {}).get("name"),
                    "metadata_modified": item.get("metadata_modified"),
                    "resource_count": len(resources),
                    "groups": ", ".join(group.get("name", "") for group in groups if group.get("name")),
                    "tags": ", ".join(tag.get("name", "") for tag in tags if tag.get("name")),
                    "notes_excerpt": (item.get("notes") or "")[:500],
                    "api_status": "success",
                    "fetched_at": fetched_at,
                    "ingestion_method": "api",
                }
            )
        df = pd.DataFrame(records)
    except Exception as exc:
        df = pd.DataFrame(
            [
                {
                    "source_name": source_name,
                    "api_url": config_row["api_url"],
                    "dataset_id": None,
                    "dataset_name": None,
                    "dataset_title": None,
                    "dataset_state": None,
                    "dataset_url": None,
                    "organization": None,
                    "metadata_modified": None,
                    "resource_count": 0,
                    "groups": None,
                    "tags": None,
                    "notes_excerpt": None,
                    "api_status": f"failed:{exc.__class__.__name__}",
                    "fetched_at": fetched_at,
                    "ingestion_method": "api",
                }
            ]
        )

    save_parquet(
        df,
        f"{config.DATA_DIR}/bronze/official_api",
        filename=f"official_api_catalog_{source_name.lower().replace(' ', '_').replace('/', '_')}.parquet",
    )
    logger.info(f"Stored CKAN API catalog for {source_name}: {len(df)} records")
    return df
