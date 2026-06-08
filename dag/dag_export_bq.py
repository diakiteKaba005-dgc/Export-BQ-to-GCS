from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
import json

# 1. Configuration des arguments par défaut d'Airflow
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 2. Fonction pour générer dynamiquement le token d'identité Google (OIDC)
def get_google_oidc_token():
    # On cible l'URL racine de ta Cloud Run comme audience
    audience = "https://bq-to-gcs-extractor-935656668098.europe-west1.run.app"
    try:
        # Utilisation du Hook Google natif de Composer pour obtenir les credentials du cluster
        google_hook = GoogleBaseHook(gcp_conn_id='google_cloud_default')
        credentials = google_hook.get_credentials()
        
        # Import interne de la bibliothèque d'authentification Google
        from google.auth.transport.requests import Request
        from google.oauth2 import id_token
        
        # Demande d'un token d'identité spécifiquement pour notre Cloud Run
        auth_req = Request()
        token = id_token.fetch_id_token(auth_req, audience=audience)
        return f"Bearer {token}"
    except Exception as e:
        # Fallback ou log si l'environnement local n'a pas accès aux métadonnées GCP
        print(self_log := f"Impossible de générer le token OIDC automatiquement : {str(e)}")
        return ""

# 3. Déclaration du DAG
with DAG(
    'dag_export_bigquery_to_gcs_finance',
    default_args=default_args,
    description='Orchestration de l extraction BQ vers GCS via Cloud Run',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Payload de recette (strictement identique)
    payload = {
        "launcher": {
            "Consommateur": "Equipe_Finance",
            "Job_name": "export_finance_incremental1",
            "Action": "Config_and_Run"
        },
        "params": {
            "job_name": "export_finance_incremental1",
            "export_type": "Delta",
            "expected_date_format": "dd/MM/yyyy HH:mm:ss",
            "decimal_separator": ",",
            "Set_config": True,
            "Column_partition": "Date",
            "last_Value": "2020-06-05T12:00:00Z",
            "last_Value_reprise": "2020-06-01T00:00:00Z",
            "dry_run": True,
            "parameters_Delta_Export": {
                "Delta_column": "Date",
                "last_export_date": "2020-06-04T23:59:59Z",
                "depth_days": None
            },
            "source": {
                "project_source": "sandbox-damadou",
                "dataset_id": "SquadData",
                "table_id": "T_ExportDataSalesforce",
                "selected_columns": ["*"],
                "Filtrage_autres": "WHERE 1=1"
            },
            "destination": {
                "project_destination": "sandbox-damadou",
                "bucket_name": "fd-export-finance-bi",
                "file_name_prefix": "extract_finance_",
                "type_extraction": "CSV"
            }
        }
    }

    # 4. Tâche unique avec injection propre de l'entête Authorization
    trigger_extraction = HttpOperator(
        task_id='trigger_extraction_run',
        http_conn_id='cloud_run_api',  
        endpoint='api/v1/export',
        method='POST',
        data=json.dumps(payload),
        headers={
            "Content-Type": "application/json",
            # On injecte la chaîne "Bearer eyJhbG..." générée par notre fonction
            "Authorization": get_google_oidc_token() 
        },
        extra_options={"check_response": False} 
    )

    trigger_extraction