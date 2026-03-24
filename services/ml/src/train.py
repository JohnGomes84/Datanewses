from pathlib import Path
import sqlite3

import mlflow
import mlflow.sklearn
import pandas as pd
import sklearn
import skops
from mlflow import MlflowClient
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error, r2_score

from shared import config, get_logger
from services.ml.src.backtest import run_backtest
from services.ml.src.modeling import FEATURE_COLUMNS, build_model_pipeline, predict_group_mean_baseline, prepare_training_frame

logger = get_logger(__name__)


MODEL_PIP_REQUIREMENTS = [
    f"mlflow=={mlflow.__version__}",
    f"scikit-learn=={sklearn.__version__}",
    f"skops=={skops.__version__}",
]


def _ensure_experiment():
    artifact_root = Path(config.MLFLOW_ARTIFACT_DIR)
    artifact_root.mkdir(parents=True, exist_ok=True)

    mlflow.set_tracking_uri(config.MLFLOW_TRACKING_URI)
    client = MlflowClient()
    experiment = client.get_experiment_by_name(config.MLFLOW_EXPERIMENT_NAME)
    if experiment is not None:
        return experiment.experiment_id

    return client.create_experiment(
        name=config.MLFLOW_EXPERIMENT_NAME,
        artifact_location=artifact_root.resolve().as_uri(),
    )


def train_nowcasting(run_id=None):
    conn = sqlite3.connect(config.SQLITE_DB)
    operations = pd.read_sql_query("SELECT * FROM operations_daily ORDER BY data", conn)

    if operations.empty:
        conn.close()
        logger.warning("No operational data available for training")
        return

    operations = prepare_training_frame(operations)
    split_index = int(len(operations) * 0.8)
    train_df = operations.iloc[:split_index].copy()
    test_df = operations.iloc[split_index:].copy()

    pipeline = build_model_pipeline()

    pipeline.fit(train_df[FEATURE_COLUMNS], train_df["trabalhadores_necessarios"])
    predictions = pipeline.predict(test_df[FEATURE_COLUMNS])
    baseline_predictions = predict_group_mean_baseline(train_df, test_df)

    mae = mean_absolute_error(test_df["trabalhadores_necessarios"], predictions)
    mape = mean_absolute_percentage_error(test_df["trabalhadores_necessarios"], predictions)
    r2 = r2_score(test_df["trabalhadores_necessarios"], predictions)
    baseline_mae = mean_absolute_error(test_df["trabalhadores_necessarios"], baseline_predictions)
    baseline_mape = mean_absolute_percentage_error(test_df["trabalhadores_necessarios"], baseline_predictions)
    baseline_r2 = r2_score(test_df["trabalhadores_necessarios"], baseline_predictions)
    backtest_results = run_backtest()
    if backtest_results.empty:
        backtest_summary = {
            "folds": 0,
            "mae_mean": None,
            "mape_mean": None,
            "r2_mean": None,
            "baseline_mae_mean": None,
            "baseline_mape_mean": None,
            "baseline_r2_mean": None,
        }
    else:
        backtest_summary = {
            "folds": int(len(backtest_results)),
            "mae_mean": float(backtest_results["mae"].mean()),
            "mape_mean": float(backtest_results["mape"].mean()),
            "r2_mean": float(backtest_results["r2"].mean()),
            "baseline_mae_mean": float(backtest_results["baseline_mae"].mean()),
            "baseline_mape_mean": float(backtest_results["baseline_mape"].mean()),
            "baseline_r2_mean": float(backtest_results["baseline_r2"].mean()),
        }
        backtest_results.insert(0, "run_id", run_id)
        backtest_results["recorded_at"] = pd.Timestamp.now()
        backtest_results.to_sql("model_backtest_folds", conn, if_exists="append", index=False)

    model_metrics = pd.DataFrame(
        [
            {
                "run_id": run_id,
                "recorded_at": pd.Timestamp.now(),
                "mae": float(mae),
                "mape": float(mape),
                "r2": float(r2),
                "baseline_mae": float(baseline_mae),
                "baseline_mape": float(baseline_mape),
                "baseline_r2": float(baseline_r2),
                "train_rows": int(len(train_df)),
                "test_rows": int(len(test_df)),
                "backtest_folds": int(backtest_summary["folds"]),
                "backtest_mae_mean": backtest_summary["mae_mean"],
                "backtest_mape_mean": backtest_summary["mape_mean"],
                "backtest_r2_mean": backtest_summary["r2_mean"],
                "backtest_baseline_mae_mean": backtest_summary["baseline_mae_mean"],
                "backtest_baseline_mape_mean": backtest_summary["baseline_mape_mean"],
                "backtest_baseline_r2_mean": backtest_summary["baseline_r2_mean"],
            }
        ]
    )
    model_metrics.to_sql("model_performance_history", conn, if_exists="append", index=False)
    conn.close()

    logger.info(f"Training complete: MAE={mae:.2f}, MAPE={mape:.4f}, R2={r2:.4f}")
    logger.info(f"Baseline compare: MAE={baseline_mae:.2f}, MAPE={baseline_mape:.4f}, R2={baseline_r2:.4f}")
    if backtest_summary["folds"]:
        logger.info(
            "Backtest summary: "
            f"folds={backtest_summary['folds']}, "
            f"MAE={backtest_summary['mae_mean']:.2f}, "
            f"MAPE={backtest_summary['mape_mean']:.4f}, "
            f"R2={backtest_summary['r2_mean']:.4f}"
        )
        logger.info(
            "Backtest baseline: "
            f"MAE={backtest_summary['baseline_mae_mean']:.2f}, "
            f"MAPE={backtest_summary['baseline_mape_mean']:.4f}, "
            f"R2={backtest_summary['baseline_r2_mean']:.4f}"
        )

    Path(config.MODEL_DIR).mkdir(parents=True, exist_ok=True)
    experiment_id = _ensure_experiment()

    with mlflow.start_run(experiment_id=experiment_id):
        mlflow.log_param("model_type", "RandomForestPipeline")
        mlflow.log_param("target", "trabalhadores_necessarios")
        mlflow.log_metric("mae", mae)
        mlflow.log_metric("mape", mape)
        mlflow.log_metric("r2", r2)
        mlflow.log_metric("baseline_mae", baseline_mae)
        mlflow.log_metric("baseline_mape", baseline_mape)
        mlflow.log_metric("baseline_r2", baseline_r2)
        if backtest_summary["folds"]:
            mlflow.log_metric("backtest_folds", backtest_summary["folds"])
            mlflow.log_metric("backtest_mae_mean", backtest_summary["mae_mean"])
            mlflow.log_metric("backtest_mape_mean", backtest_summary["mape_mean"])
            mlflow.log_metric("backtest_r2_mean", backtest_summary["r2_mean"])
            mlflow.log_metric("backtest_baseline_mae_mean", backtest_summary["baseline_mae_mean"])
            mlflow.log_metric("backtest_baseline_mape_mean", backtest_summary["baseline_mape_mean"])
            mlflow.log_metric("backtest_baseline_r2_mean", backtest_summary["baseline_r2_mean"])
        mlflow.sklearn.log_model(
            pipeline,
            name="workforce_forecast_model",
            serialization_format="skops",
            pip_requirements=MODEL_PIP_REQUIREMENTS,
        )

    logger.info("Forecast model saved")


if __name__ == "__main__":
    train_nowcasting()
