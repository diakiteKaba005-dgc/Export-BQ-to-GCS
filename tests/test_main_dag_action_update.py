import os
from datetime import datetime, timezone

import pytest

import main


def test_update_dag_action_to_run_updates_local_dag_file(tmp_path, monkeypatch):
    dag_file = tmp_path / "dag_test_update.py"
    content = '{"Job_name": "export_test_config_and_run", "Action": "Config_and_Run"}'
    dag_file.write_text(content, encoding="utf-8")

    monkeypatch.setattr(main, "get_dag_dir", lambda: str(tmp_path))

    result = main.update_dag_action_to_run("export_test_config_and_run")

    assert result is True
    assert dag_file.read_text(encoding="utf-8") == '{"Job_name": "export_test_config_and_run", "Action": "Run"}'


def test_update_dag_action_to_run_returns_false_for_gcs_uri(monkeypatch):
    monkeypatch.setattr(main, "get_dag_dir", lambda: "gs://my-project-prd-dags")

    result = main.update_dag_action_to_run("export_test_config_and_run")

    assert result is False


def test_run_export_pipeline_sets_advice_message_when_dag_update_fails(monkeypatch):
    class FakeExtractor:
        def build_query(self, config):
            return "SELECT 1"

        def estimate_costs(self, query):
            return 0

        def extract_to_gcs(self, query, destination_uri, format_type):
            return 10

    class FakeConsolidator:
        def consolidate_shards(self, bucket_name, file_prefix, table_id, end_time_str, format_type):
            return "gs://bucket/final.csv"

    monkeypatch.setattr(main, "get_bq_extractor", lambda: FakeExtractor())
    monkeypatch.setattr(main, "get_gcs_consolidator", lambda: FakeConsolidator())
    monkeypatch.setattr(main, "update_dag_action_to_run", lambda job_name: False)

    logged = []

    def fake_update_config_action(job_name, action):
        logged.append(("config_action", job_name, action))

    def fake_finalize_log(exec_id, start_time, status, end_time, rows=None, bytes_proc=None, error=None, uri=None):
        logged.append(("finalize", status, error, rows, uri))

    monkeypatch.setattr(main.db, "update_config_action", fake_update_config_action)
    monkeypatch.setattr(main.db, "finalize_log", fake_finalize_log)

    job_config = {
        "job_name": "export_test_config_and_run",
        "source": {
            "project_source": "sandbox-damadou",
            "dataset_id": "SquadData",
            "table_id": "T_ExportDataSalesforce",
        },
        "destination": {
            "bucket_name": "sandbox-damadou-fd-export-finance-bi",
            "file_name_prefix": "extract_finance_",
            "type_extraction": "CSV",
        },
        "export_type": "Full",
    }

    exec_id = "test-exec"
    start_time = datetime.now(timezone.utc)
    main.run_export_pipeline(exec_id, start_time, "Config_and_Run", "Equipe_Test", job_config)

    assert ("config_action", "export_test_config_and_run", "Run") in logged
    finalize_calls = [entry for entry in logged if entry[0] == "finalize"]
    assert len(finalize_calls) == 1
    status, error, rows, uri = finalize_calls[0][1:]
    assert status == "SUCCESS"
    assert error == "Exécution réussie. Veuillez changer le paramètre Action du Dag afin de ne pas écraser la configuration pour les prochaines exécution"
    assert rows == 10
    assert uri == "gs://bucket/final.csv"
