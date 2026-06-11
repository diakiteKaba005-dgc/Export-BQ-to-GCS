import asyncio

from fastapi import BackgroundTasks, HTTPException

from main import ExportRequest, LauncherPayload, db, trigger_export


def test_init_maj_config_requires_params():
    launcher = LauncherPayload(
        Consommateur="Equipe_Finance",
        Job_name="export_finance_incremental1",
        Action="Init_Maj_config"
    )
    payload = ExportRequest(launcher=launcher, params=None)
    background_tasks = BackgroundTasks()

    try:
        asyncio.run(trigger_export(payload, background_tasks))
        assert False, "trigger_export should raise HTTPException when params is missing"
    except HTTPException as exc:
        assert exc.status_code == 400
        assert "params" in str(exc.detail)


def test_init_maj_config_logs_configuration_success(monkeypatch):
    launcher = LauncherPayload(
        Consommateur="Equipe_Finance",
        Job_name="export_finance_incremental1",
        Action="Init_Maj_config"
    )
    payload = ExportRequest(
        launcher=launcher,
        params={
            "job_name": "export_finance_incremental1",
            "export_type": "Full",
            "expected_date_format": "dd/MM/yyyy HH:mm:ss",
            "decimal_separator": ",",
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
    )
    background_tasks = BackgroundTasks()

    save_calls = []
    insert_calls = []
    finalize_calls = []

    monkeypatch.setattr(db, "save_or_update_config", lambda params, consommateur, action: save_calls.append((params, consommateur, action)))

    def fake_insert_initial_log(exec_id, start_time, action, consommateur, job_config):
        insert_calls.append({
            "exec_id": exec_id,
            "action": action,
            "consommateur": consommateur,
            "job_config": job_config,
        })

    def fake_finalize_log(exec_id, start_time, status, end_time, rows=None, bytes_proc=None, error=None, uri=None):
        finalize_calls.append({
            "exec_id": exec_id,
            "status": status,
            "error": error,
            "rows": rows,
            "bytes_proc": bytes_proc,
            "uri": uri,
        })

    monkeypatch.setattr(db, "insert_initial_log", fake_insert_initial_log)
    monkeypatch.setattr(db, "finalize_log", fake_finalize_log)

    response = asyncio.run(trigger_export(payload, background_tasks))

    assert response["status"] == "CONFIG_UPDATED"
    assert len(save_calls) == 1
    assert len(insert_calls) == 1
    assert len(finalize_calls) == 1
    assert finalize_calls[0]["status"] == "SUCCESS"
    assert finalize_calls[0]["error"] == (
        "Configuration réussie. Veuillez changer le paramètre Action à Run afin que le job s’exécute les prochaines fois"
    )
