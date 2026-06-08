import uuid
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional

from cloudSql.db_manager import DBManager
from bq.extractor import BQExtractor
from gcs.consolidator import GCSConsolidator

app = FastAPI(title="BQ to GCS Data Export Engine", version="1.0")

db = DBManager()
bq = BQExtractor()
gcs = GCSConsolidator()

# Initialisation du schéma à chaque démarrage (Règle de gestion)
@app.on_event("startup")
def startup_event():
    db.init_db_schema()

# Modèles de données pour la validation stricte des requêtes HTTP POST
class LauncherPayload(BaseModel):
    Consommateur: str
    Job_name: str
    Action: str  # Options: Run, Init_Maj_config, Config_and_Run

class ExportRequest(BaseModel):
    launcher: LauncherPayload
    params: Optional[dict] = None

def run_export_pipeline(exec_id: str, start_time: datetime, action: str, consommateur: str, job_config: dict):
    """Pipeline d'extraction asynchrone pour éviter les coupures de timeout HTTP."""
    end_time = None
    try:
        # 1. Construction SQL prenant en compte la logique Delta (Incrémentale)
        query = bq.build_query(job_config)
        bytes_processed = bq.estimate_costs(query) if job_config.get("dry_run") else 0
        
        # 2. Définition des patterns d'URIs cibles
        dest = job_config["destination"]
        end_time_str = start_time.strftime("%Y%m%d_%H%M%S")
        
        # Utilisation du joker '*' requis par BigQuery pour le Sharding en cas de gros volumes
        temp_uri = f"gs://{dest['bucket_name']}/{dest['file_name_prefix']}{job_config['source']['table_id']}_{end_time_str}_*"
        
        # 3. Extraction vers GCS via la table temporaire anonyme
        rows_exported = bq.extract_to_gcs(query, temp_uri, dest["type_extraction"])
        
        # 4. MISE À JOUR : Consolidation des shards avec gestion des paquets de 32 et de l'extension
        final_uri = gcs.consolidate_shards(
            bucket_name=dest["bucket_name"],
            file_prefix=dest["file_name_prefix"],
            table_id=job_config["source"]["table_id"],
            end_time_str=end_time_str,
            format_type=dest["type_extraction"]  # <-- Prise en compte du format cible
        )
        
        # 5. Clôture des Logs à l'état SUCCESS
        end_time = datetime.now(timezone.utc)
        db.finalize_log(
            exec_id=exec_id, start_time=start_time, status="SUCCESS", 
            end_time=end_time, rows=rows_exported, bytes_proc=bytes_processed, uri=final_uri
        )
        
    except Exception as e:
        print(f"[ERROR] Échec du pipeline d'extraction {exec_id} : {str(e)}")
        end_time = datetime.now(timezone.utc)
        db.finalize_log(
            exec_id=exec_id, start_time=start_time, status="FAILED", 
            end_time=end_time, error=str(e)
        )

@app.post("/api/v1/export", status_code=202)
async def trigger_export(payload: ExportRequest, background_tasks: BackgroundTasks):
    exec_id = str(uuid.uuid4())
    start_time = datetime.now(timezone.utc)
    
    launcher = payload.launcher
    params = payload.params
    
    # Règle Métier : Détermination de la configuration finale à appliquer
    if launcher.Action in ["Init_Maj_config", "Config_and_Run"]:
        if not params:
            raise HTTPException(status_code=400, detail="Le payload 'params' est obligatoire pour cette action.")
        db.save_or_update_config(params, launcher.Consommateur)
        job_config = params
        
    elif launcher.Action == "Run":
        if params:
            # Mode Lancement unitaire : params prioritaires, pas de mise à jour de la config en base
            job_config = params
        else:
            # Mode Standard : Récupération de la configuration enregistrée en PostgreSQL (clés corrigées)
            try:
                job_config = db.get_config(launcher.Job_name)
            except ValueError as e:
                raise HTTPException(status_code=404, detail=str(e))
    else:
        raise HTTPException(status_code=400, detail=f"Action '{launcher.Action}' inconnue.")

    # Seule l'action 'Init_Maj_config' ne lance pas l'export de données
    if launcher.Action == "Init_Maj_config":
        return {"status": "CONFIG_UPDATED", "job_name": launcher.Job_name, "execution_id": exec_id}

    # Initialisation du log à l'état 'RUNNING'
    db.insert_initial_log(exec_id, start_time, launcher.Action, launcher.Consommateur, job_config)
    
    # Envoi du traitement lourd en tâche de fond (Background Task) pour libérer le worker HTTP immédiatement
    background_tasks.add_task(
        run_export_pipeline, 
        exec_id=exec_id, start_time=start_time, action=launcher.Action, 
        consommateur=launcher.Consommateur, job_config=job_config
    )
    
    return {
        "status": "RUNNING",
        "execution_id": exec_id,
        "job_name": launcher.Job_name,
        "started_at": start_time.isoformat()
    }