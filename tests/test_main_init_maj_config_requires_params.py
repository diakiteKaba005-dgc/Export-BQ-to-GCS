import asyncio

from fastapi import BackgroundTasks, HTTPException

from main import ExportRequest, LauncherPayload, trigger_export


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
