import os
import re
import uuid
import google.auth
from google.auth.impersonated_credentials import Credentials as ImpersonatedCredentials
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional

from cloudSql.db_manager import DBManager
from bq.extractor import BQExtractor
from gcs.consolidator import GCSConsolidator
from config.loader import load_config

# Chargement de la configuration d'environnement (dev/rec/prd)
try:
    app_config = load_config()
except FileNotFoundError:
    app_config = {}


PLACEHOLDER_PATTERN = re.compile(r"\$\{[^}]+\}")


def _is_unresolved_placeholder(value: str) -> bool:
    return isinstance(value, str) and bool(PLACEHOLDER_PATTERN.search(value))


def set_google_application_credentials(app_config: dict, env_name: str | None = None):
    """Configure les credentials GCP en fonction de la configuration.

    En local, on peut fournir un `credentials_path` vers une clé JSON.
    En production, on peut renseigner `service_account` pour l'impersonation.
    """
    env_name = env_name or os.environ.get("APP_ENV") or os.environ.get("ENV") or "dev"
    creds_path = app_config.get("credentials_path")
    service_account = app_config.get("service_account")

    if creds_path and not _is_unresolved_placeholder(creds_path):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path
        return None

    if service_account and not _is_unresolved_placeholder(service_account):
        source_credentials, _ = google.auth.default()
        if source_credentials is None:
            raise RuntimeError(
                "Unable to resolve source credentials for service account impersonation."
            )
        target_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
        return ImpersonatedCredentials(
            source_credentials=source_credentials,
            target_principal=service_account,
            target_scopes=target_scopes,
            lifetime=3600,
        )

    if env_name == "prd" and not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        raise RuntimeError(
            "Missing Google credentials: set service_account or credentials_path in config,"
            " or export SA_KEY_PATH/GOOGLE_APPLICATION_CREDENTIALS"
        )

    print("[WARN] No credentials_path or service_account in config; using existing GOOGLE_APPLICATION_CREDENTIALS if present.")
    return None


def get_dag_dir(config: dict | None = None) -> str:
    """Retourne le chemin du répertoire DAG configuré ou la valeur par défaut."""
    config = config or app_config
    raw_dir = config.get("dag_dir")
    if raw_dir:
        raw_dir = os.path.expandvars(raw_dir)
        if raw_dir.startswith("gs://"):
            return raw_dir
        if not os.path.isabs(raw_dir):
            raw_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), raw_dir))
        return raw_dir
    return os.path.join(os.path.dirname(__file__), "dag")


# Si la configuration fournit un chemin de clef de service, l'exposer
# vers l'API client Google via la variable d'environnement standard.
app_credentials = set_google_application_credentials(app_config)

app = FastAPI(title="BQ to GCS Data Export Engine", version="1.0")

db = DBManager()
bq = None
gcs = None


def get_bq_extractor():
    global bq
    if bq is None:
        bq = BQExtractor(defaults=app_config, credentials=app_credentials)
    return bq


def get_gcs_consolidator():
    global gcs
    if gcs is None:
        gcs = GCSConsolidator(defaults=app_config, credentials=app_credentials)
    return gcs


def update_dag_action_to_run(job_name: str) -> bool:
    """Tente de mettre à jour le DAG source pour basculer Action=Config_and_Run en Run.

    Retourne True si un fichier a été mis à jour, False sinon.
    Si les DAGs sont stockés en GCS (gs://) ou inaccessibles localement, la mise à jour
    est considérée comme échouée, mais l'export ne doit pas être interrompu.
    """
    try:
        dag_dir = get_dag_dir()

        if dag_dir.startswith("gs://"):
            print(f"[INFO] DAGs stored in GCS ({dag_dir}); skipping local Action update.")
            return False

        if not os.path.isdir(dag_dir):
            print(f"[INFO] DAG directory not found: {dag_dir}")
            return False

        for filename in os.listdir(dag_dir):
            if not filename.endswith(".py"):
                continue

            path = os.path.join(dag_dir, filename)
            try:
                with open(path, "r", encoding="utf-8") as f:
                    content = f.read()
            except OSError:
                continue

            if f'"Job_name": "{job_name}"' in content and '"Action": "Config_and_Run"' in content:
                updated = content.replace('"Action": "Config_and_Run"', '"Action": "Run"', 1)
                if updated != content:
                    with open(path, "w", encoding="utf-8") as f:
                        f.write(updated)
                    return True
        return False
    except Exception as e:
        print(f"[WARN] Could not update DAG action: {str(e)}")
        return False


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
    """Pipeline d'extraction asynchrone pour éviter les coupures de timeout HTTP.

    Règles métier Delta :
    - Pour un export de type Delta, la date de dernier export `last_export_date`
      doit être mise à jour après chaque export réussi avec la dernière date
      effectivement exportée.
    - La requête exécutée doit être construite de sorte que
      `Delta_column > last_export_date` lorsque `last_export_date` est disponible.
    """
    end_time = None
    dag_update_error = None  # Tracker les erreurs de mise à jour du DAG
    
    try:
        extractor = get_bq_extractor()

        # 1. Construction SQL prenant en compte la logique Delta (Incrémentale)
        query = extractor.build_query(job_config)
        bytes_processed = extractor.estimate_costs(query) if job_config.get("dry_run") else 0
        
        # 2. Définition des patterns d'URIs cibles
        dest = job_config["destination"]
        end_time_str = start_time.strftime("%Y%m%d_%H%M%S")
        
        # Utilisation du joker '*' requis par BigQuery pour le Sharding en cas de gros volumes
        temp_uri = f"gs://{dest['bucket_name']}/{dest['file_name_prefix']}{job_config['source']['table_id']}_{end_time_str}_*"
        
        # 3. Extraction vers GCS via la table temporaire anonyme
        rows_exported = extractor.extract_to_gcs(query, temp_uri, dest["type_extraction"])
        
        # 4. MISE À JOUR : Consolidation des shards avec gestion des paquets de 32 et de l'extension
        consolidator = get_gcs_consolidator()
        final_uri = consolidator.consolidate_shards(
            bucket_name=dest["bucket_name"],
            file_prefix=dest["file_name_prefix"],
            table_id=job_config["source"]["table_id"],
            end_time_str=end_time_str,
            format_type=dest["type_extraction"]  # <-- Prise en compte du format cible
        )
        
        # 5. Tentative de mise à jour du DAG si Config_and_Run
        if action == "Config_and_Run":
            db.update_config_action(job_config["job_name"], "Run")
            if not update_dag_action_to_run(job_config["job_name"]):
                dag_update_error = (
                    "Exécution réussie. Veuillez changer le paramètre Action du Dag afin de ne pas écraser la configuration pour les prochaines exécution"
                )
                print("[WARN] DAG action was not updated to Run; logging advice message.")
        
        # 6. Mise à jour des dates Delta si applicable
        if job_config.get("export_type") == "Delta":
            last_export_date = extractor.get_last_export_timestamp(job_config)
            if last_export_date:
                db.update_last_export_date(job_config["job_name"], last_export_date)

        # 7. Clôture des Logs à l'état SUCCESS
        end_time = datetime.now(timezone.utc)
        db.finalize_log(
            exec_id=exec_id, start_time=start_time, status="SUCCESS", 
            end_time=end_time, rows=rows_exported, bytes_proc=bytes_processed, uri=final_uri,
            error=dag_update_error
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
        db.save_or_update_config(params, launcher.Consommateur, launcher.Action)
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
        try:
            db.insert_initial_log(exec_id, start_time, launcher.Action, launcher.Consommateur, job_config)
            db.finalize_log(
                exec_id=exec_id,
                start_time=start_time,
                status="SUCCESS",
                end_time=start_time,
                rows=0,
                bytes_proc=0,
                error="Configuration réussie. Veuillez changer le paramètre Action à Run afin que le job s’exécute les prochaines fois",
                uri=None
            )
        except Exception as log_exc:
            print(f"[WARN] Failed to write Init_Maj_config log: {str(log_exc)}")
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