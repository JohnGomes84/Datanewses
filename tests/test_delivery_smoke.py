import runpy
import sqlite3
import unittest
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "data" / "nowcasting.db"
MLFLOW_DIR = PROJECT_ROOT / "models" / "mlruns"
REGIONAL_BRONZE = PROJECT_ROOT / "data" / "bronze" / "regional" / "regional_signals.parquet"
INMET_BRONZE = PROJECT_ROOT / "data" / "bronze" / "regional" / "regional_inmet_forecast.parquet"


class DeliverySmokeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not DB_PATH.exists():
            raise unittest.SkipTest("Banco de dados nao encontrado; rode o pipeline antes dos testes")
        cls.conn = sqlite3.connect(DB_PATH)

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def _count(self, table_name):
        return self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

    def test_core_tables_have_expected_volume(self):
        self.assertGreaterEqual(self._count("operations_daily"), 2500)
        self.assertGreaterEqual(self._count("regional_monitoring"), 900)
        self.assertGreaterEqual(self._count("workforce_forecasts"), 200)
        self.assertGreaterEqual(self._count("alerts_operacionais"), 50)
        self.assertGreaterEqual(self._count("data_quality_checks"), 18)
        self.assertGreaterEqual(self._count("model_performance_history"), 1)
        self.assertGreaterEqual(self._count("model_backtest_folds"), 1)
        self.assertGreaterEqual(self._count("pipeline_run_summaries"), 1)
        self.assertGreaterEqual(self._count("direct_infrastructure_signals"), 1)

    def test_snapshot_history_is_persisted_for_latest_run(self):
        latest_run = self.conn.execute(
            """
            SELECT run_id
            FROM ingestion_runs
            WHERE status = 'success'
            ORDER BY finished_at DESC
            LIMIT 1
            """
        ).fetchone()
        self.assertIsNotNone(latest_run)
        latest_run_id = latest_run[0]

        regional_history = self.conn.execute(
            "SELECT COUNT(*) FROM regional_monitoring_history WHERE snapshot_run_id = ?",
            (latest_run_id,),
        ).fetchone()[0]
        forecast_history = self.conn.execute(
            "SELECT COUNT(*) FROM workforce_forecasts_history WHERE snapshot_run_id = ?",
            (latest_run_id,),
        ).fetchone()[0]
        alert_history = self.conn.execute(
            "SELECT COUNT(*) FROM alerts_operacionais_history WHERE snapshot_run_id = ?",
            (latest_run_id,),
        ).fetchone()[0]

        self.assertGreaterEqual(regional_history, 900)
        self.assertGreaterEqual(forecast_history, 200)
        self.assertGreaterEqual(alert_history, 50)

    def test_model_summary_is_recorded_for_latest_run(self):
        latest_summary = self.conn.execute(
            """
            SELECT duration_seconds, healthy_sources, failed_sources, cached_fallbacks
            FROM pipeline_run_summaries
            ORDER BY recorded_at DESC
            LIMIT 1
            """
        ).fetchone()
        self.assertIsNotNone(latest_summary)
        self.assertGreater(latest_summary[0], 0)
        self.assertGreaterEqual(latest_summary[1], 1)
        self.assertGreaterEqual(latest_summary[2], 0)
        self.assertGreaterEqual(latest_summary[3], 0)

    def test_source_registry_matches_scope(self):
        rows = self.conn.execute(
            """
            SELECT source_name, last_ingestion_method
            FROM source_registry
            WHERE source_name IN (
                'MDIC Export Demand',
                'BCB Market Indicators',
                'Official Transport News',
                'Regional Monitoring Derived',
                'INMET Regional Forecast'
            )
            ORDER BY source_name
            """
        ).fetchall()
        self.assertEqual(
            rows,
            [
                ("BCB Market Indicators", "api"),
                ("INMET Regional Forecast", "api"),
                ("MDIC Export Demand", "file_download"),
                ("Official Transport News", "web_fetch"),
                ("Regional Monitoring Derived", "derived_official_signals"),
            ],
        )

    def test_regional_outputs_preserve_scope(self):
        regional = pd.read_parquet(REGIONAL_BRONZE)
        inmet = pd.read_parquet(INMET_BRONZE)
        self.assertGreaterEqual(len(regional), 900)
        self.assertEqual(set(regional["origem"].dropna().unique()), {"operational_signals_with_inmet"})
        self.assertGreaterEqual(len(inmet), 10)
        self.assertEqual(set(inmet["origem"].dropna().unique()), {"inmet_municipal_forecast"})
        regional_db = pd.read_sql_query("SELECT * FROM regional_monitoring", self.conn)
        self.assertTrue(
            {"infraestrutura_risk_index_base", "infraestrutura_risk_index", "impacto_fontes_diretas", "source_support_index"}.issubset(regional_db.columns)
        )
        self.assertGreater(regional_db["source_support_index"].fillna(0).max(), 0)
        self.assertGreater(regional_db["impacto_fontes_diretas"].abs().fillna(0).max(), 0)

    def test_mlflow_latest_model_uses_skops(self):
        latest_mlmodel = max(MLFLOW_DIR.glob("**/artifacts/MLmodel"), key=lambda path: path.stat().st_mtime)
        contents = latest_mlmodel.read_text(encoding="utf-8")
        self.assertIn("serialization_format: skops", contents)

    def test_dashboard_loads(self):
        runpy.run_path(str(PROJECT_ROOT / "services" / "dashboard" / "app.py"), run_name="__main__")


if __name__ == "__main__":
    unittest.main()
