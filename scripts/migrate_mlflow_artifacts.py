import sqlite3
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shared import config


TEXT_FILE_NAMES = {"MLmodel", "meta.yaml"}


def _to_file_uri(path: Path) -> str:
    return path.resolve().as_uri()


def _to_file_uri_raw(path: Path) -> str:
    return f"file:///{path.resolve().as_posix()}"


def _replace_prefixes(value: str, replacements: list[tuple[str, str]]) -> str:
    if not value:
        return value
    for old_prefix, new_prefix in replacements:
        if value.startswith(old_prefix):
            return new_prefix + value[len(old_prefix):]
    return value


def _rewrite_text_metadata(root: Path, replacements: list[tuple[str, str]]) -> int:
    updated = 0
    for path in root.rglob("*"):
        if not path.is_file() or path.name not in TEXT_FILE_NAMES:
            continue
        raw = path.read_text(encoding="utf-8")
        patched = raw
        for old_prefix, new_prefix in replacements:
            patched = patched.replace(old_prefix, new_prefix)
        if patched != raw:
            path.write_text(patched, encoding="utf-8")
            updated += 1
    return updated


def migrate():
    db_path = Path(config.MLFLOW_DB)
    old_root = Path("mlruns")
    new_root = Path(config.MLFLOW_ARTIFACT_DIR)

    if not db_path.exists():
        raise FileNotFoundError(f"MLflow DB not found: {db_path}")

    new_root.mkdir(parents=True, exist_ok=True)

    old_root_uri = _to_file_uri(old_root)
    new_root_uri = _to_file_uri(new_root)
    old_root_uri_raw = _to_file_uri_raw(old_root)
    new_root_uri_raw = _to_file_uri_raw(new_root)
    root_replacements = [
        (old_root_uri, new_root_uri),
        (old_root_uri_raw, new_root_uri_raw),
    ]

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    experiments = cur.execute(
        "SELECT experiment_id, artifact_location FROM experiments WHERE lifecycle_stage = 'active'"
    ).fetchall()

    for experiment_id, artifact_location in experiments:
        old_experiment_dir = old_root / str(experiment_id)
        new_experiment_dir = new_root / str(experiment_id)

        if old_experiment_dir.exists():
            if new_experiment_dir.exists():
                for child in old_experiment_dir.iterdir():
                    target = new_experiment_dir / child.name
                    if not target.exists():
                        child.rename(target)
                old_experiment_dir.rmdir()
            else:
                old_experiment_dir.rename(new_experiment_dir)

        desired_artifact_location = _to_file_uri(new_experiment_dir)
        if artifact_location != desired_artifact_location:
            cur.execute(
                "UPDATE experiments SET artifact_location = ?, last_update_time = strftime('%s','now') * 1000 WHERE experiment_id = ?",
                (desired_artifact_location, experiment_id),
            )

        run_rows = cur.execute(
            "SELECT run_uuid, artifact_uri FROM runs WHERE experiment_id = ?",
            (experiment_id,),
        ).fetchall()
        for run_uuid, artifact_uri in run_rows:
            new_run_uri = f"{desired_artifact_location}/{run_uuid}/artifacts"
            updated_uri = _replace_prefixes(
                artifact_uri,
                [
                    (f"{old_root_uri}/{experiment_id}/{run_uuid}/artifacts", new_run_uri),
                    (f"{old_root_uri_raw}/{experiment_id}/{run_uuid}/artifacts", new_run_uri),
                ],
            )
            if updated_uri != artifact_uri:
                cur.execute(
                    "UPDATE runs SET artifact_uri = ? WHERE run_uuid = ?",
                    (updated_uri, run_uuid),
                )

        logged_models = cur.execute(
            "SELECT model_id, artifact_location FROM logged_models WHERE experiment_id = ?",
            (experiment_id,),
        ).fetchall()
        for model_id, artifact_location in logged_models:
            new_model_uri = f"{desired_artifact_location}/models/{model_id}/artifacts"
            updated_location = _replace_prefixes(
                artifact_location,
                [
                    (f"{old_root_uri}/{experiment_id}/models/{model_id}/artifacts", new_model_uri),
                    (f"{old_root_uri_raw}/{experiment_id}/models/{model_id}/artifacts", new_model_uri),
                ],
            )
            if updated_location != artifact_location:
                cur.execute(
                    "UPDATE logged_models SET artifact_location = ?, last_updated_timestamp_ms = strftime('%s','now') * 1000 WHERE model_id = ?",
                    (updated_location, model_id),
                )

        model_versions = cur.execute(
            "SELECT name, version, source, storage_location FROM model_versions WHERE source IS NOT NULL OR storage_location IS NOT NULL"
        ).fetchall()
        for name, version, source, storage_location in model_versions:
            updated_source = _replace_prefixes(source, root_replacements)
            updated_storage = _replace_prefixes(storage_location, root_replacements)
            if updated_source != source or updated_storage != storage_location:
                cur.execute(
                    "UPDATE model_versions SET source = ?, storage_location = ?, last_updated_time = strftime('%s','now') * 1000 WHERE name = ? AND version = ?",
                    (updated_source, updated_storage, name, version),
                )

        _rewrite_text_metadata(
            new_experiment_dir,
            [
                (f"{old_root_uri}/{experiment_id}", f"{desired_artifact_location}"),
                (f"{old_root_uri_raw}/{experiment_id}", _to_file_uri_raw(new_experiment_dir)),
            ],
        )

    conn.commit()
    conn.close()

    if old_root.exists() and not any(old_root.iterdir()):
        old_root.rmdir()


if __name__ == "__main__":
    migrate()
