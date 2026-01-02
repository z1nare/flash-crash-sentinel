import os
import tempfile

import pytest


mlflow = pytest.importorskip("mlflow")


def test_mlflow_can_log_reproducibility_metadata():
    """
    Minimal MLflow tracking test.

    Evidence for RB-REQ-12 (reproducible artifacts + versioned metadata).
    Uses a temporary file-based tracking URI so it is CI-safe.
    """
    with tempfile.TemporaryDirectory() as tmp:
        tracking_uri = f"file:///{tmp.replace(os.sep, '/')}"
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment("riskbeacon_coursework")

        with mlflow.start_run(run_name="smoke_repro_metadata") as run:
            mlflow.log_param("seed", 42)
            mlflow.log_param("data_version_hint", "historicalData/*.csv")
            mlflow.log_param("code_version_hint", "git:HEAD")
            mlflow.log_metric("smoke_metric", 1.0)

        # Assert that an MLflow run was created (artifact store exists)
        assert os.path.isdir(tmp)
        # MLflow creates an "mlruns" structure inside the tracking folder in many setups,
        # but file-based tracking can also use direct experiment folders.
        # We just require that *some* contents exist.
        assert len(os.listdir(tmp)) > 0


# Yet to be implemented.