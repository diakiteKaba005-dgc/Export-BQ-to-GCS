import asyncio

from fastapi import BackgroundTasks, HTTPException

from main import ExportRequest, LauncherPayload, trigger_export


def test_unknown_action_returns_400():
    launcher = LauncherPayload(
        Consommateur="Equipe_Finance",
        Job_name="export_finance_incremental1",
        Action="InvalidAction"
    )
    payload = ExportRequest(launcher=launcher, params={})
    background_tasks = BackgroundTasks()

    try:
        asyncio.run(trigger_export(payload, background_tasks))
        assert False, "trigger_export should raise HTTPException for invalid action"
    except HTTPException as exc:
        assert exc.status_code == 400
        assert "inconnue" in str(exc.detail).lower()
