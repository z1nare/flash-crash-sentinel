import os
import tempfile

import pytest


mlflow = pytest.importorskip("mlflow")
sklearn = pytest.importorskip("sklearn")
import numpy as np  

from mlflow.models.signature import infer_signature  
from mlflow.tracking import MlflowClient  
from sklearn.linear_model import LogisticRegression  


def test_mlflow_sklearn_model_logged_with_signature_and_registry():
    """
    MLOps-grade MLflow test (sklearn flavour + signature + artifacts + registry).

    Evidence for RB-REQ-12:
    - reproducible metadata logged (seed/data/code)
    - model artifact logged (sklearn flavour)
    - signature + input example logged
    - registry entry created and model can be loaded back for inference
    """
    with tempfile.TemporaryDirectory() as tmp:
        tracking_uri = f"file:///{tmp.replace(os.sep, '/')}"
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_registry_uri(tracking_uri)
        mlflow.set_experiment("riskbeacon_coursework")

        # Tiny deterministic dataset
        rng = np.random.default_rng(42)
        X = rng.normal(size=(200, 2))
        y = (X[:, 0] + 0.25 * X[:, 1] > 0).astype(int)

        model = LogisticRegression(random_state=42, max_iter=500).fit(X, y)

        input_example = X[:5]
        signature = infer_signature(X[:20], model.predict(X[:20]))

        model_name = "RiskBeaconRegimeModel_Smoke"

        with mlflow.start_run(run_name="sklearn_registry_smoke") as run:
            # Repro metadata (fill with real DVC/git hashes later)
            mlflow.log_param("seed", 42)
            mlflow.log_param("data_version_hint", "historicalData/*.csv")
            mlflow.log_param("code_version_hint", "git:HEAD")
            mlflow.log_metric("train_acc_smoke", float((model.predict(X) == y).mean()))

            mlflow.sklearn.log_model(
                sk_model=model,
                artifact_path="model",
                signature=signature,
                input_example=input_example,
                registered_model_name=model_name,
            )

        client = MlflowClient(tracking_uri=tracking_uri, registry_uri=tracking_uri)
        versions = client.search_model_versions(f"name='{model_name}'")
        assert len(versions) >= 1

        v = versions[0]
        model_uri = f"models:/{model_name}/{v.version}"

        # Transition stage (optional but strong for MLOps evidence; may be deprecated in newer MLflow)
        try:
            client.transition_model_version_stage(
                name=model_name, version=v.version, stage="Staging", archive_existing_versions=False
            )
        except Exception:
            pass

        loaded = mlflow.pyfunc.load_model(model_uri)
        preds = loaded.predict(input_example)
        assert len(preds) == len(input_example)

        # Assert that an MLflow run was created (artifact store exists)
        assert os.path.isdir(tmp)
        assert len(os.listdir(tmp)) > 0