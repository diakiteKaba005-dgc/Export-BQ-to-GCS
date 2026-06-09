import asyncio

from fastapi import BackgroundTasks

from main import ExportRequest, LauncherPayload, db, trigger_export


def test_run_action_uses_saved_config(monkeypatch):
    launcher = LauncherPayload(
        Consommateur="Equipe_Finance",
        Job_name="export_finance_incremental1",
        Action="Run"
    )
    payload = ExportRequest(launcher=launcher, params=None)
    background_tasks = BackgroundTasks()

    saved_config = {
        "job_name": "export_finance_incremental1",
        "export_type": "Full",
        "expected_date_format": "dd/MM/yyyy HH:mm:ss",
        "decimal_separator": ",",
        "Column_partition": "Date",
        "last_Value": None,
        "last_Value_reprise": None,
        "dry_run": True,
        "parameters_Delta_Export": {},
        "source": {
            "project_source": "sandbox-damadou",
            "dataset_id": "SquadData",
            "table_id": "T_ExportDataSalesforce",
            "selected_columns": ["*"],
            "Filtrage_autres": "WHERE 1=1"
        },
        "destination": {
            "project_destination": "sandbox-damadou",
            "bucket_name": "sandbox-damadou-fd-export-finance-bi",
            "file_name_prefix": "extract_finance_",
            "type_extraction": "CSV"
        }
    }

    get_config_calls = []
    insert_log_calls = []

    monkeypatch.setattr(db, "get_config", lambda job_name: saved_config if job_name == "export_finance_incremental1" else None)

    def fake_insert_initial_log(exec_id, start_time, action, consommateur, job_config):
        insert_log_calls.append({
            "exec_id": exec_id,
            "action": action,
            "job_config": job_config,
        })

    monkeypatch.setattr(db, "insert_initial_log", fake_insert_initial_log)

    response = asyncio.run(trigger_export(payload, background_tasks))

    assert response["status"] == "RUNNING"
    assert len(background_tasks.tasks) == 1
    assert len(insert_log_calls) == 1
    assert insert_log_calls[0]["job_config"] == saved_config
