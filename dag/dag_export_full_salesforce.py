"""DAG de test : Export complet de T_ExportDataSalesforce

CAS DE TEST :
- Export Full (toutes les données sans filtrage incrémental)
- Récupération de toutes les colonnes (*)
- Format de sortie : CSV
- Sans filtrage additionnel

RÉSULTAT ATTENDU :
- Un fichier CSV unique ou fragmenté en shards dans GCS
- Chemin : gs://sandbox-damadou-fd-export-salesforce-bi/export_salesforce_full_*.csv
- Contient toutes les 23 colonnes de la table T_ExportDataSalesforce
- Tous les enregistrements sont exportés
- Les decimals séparés par des virgules

EXÉCUTION :
- Quotidienne (schedule_interval='@daily')
- Idéal pour tester la récupération complète des données
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
import json

# Configuration des arguments par défaut d'Airflow
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Fonction pour générer dynamiquement le token d'identité Google (OIDC)
def get_google_oidc_token():
    audience = "https://bq-to-gcs-extractor-935656668098.europe-west1.run.app"
    try:
        google_hook = GoogleBaseHook(gcp_conn_id='google_cloud_default')
        credentials = google_hook.get_credentials()
        
        from google.auth.transport.requests import Request
        from google.oauth2 import id_token
        
        auth_req = Request()
        token = id_token.fetch_id_token(auth_req, audience=audience)
        return f"Bearer {token}"
    except Exception as e:
        print(f"Impossible de générer le token OIDC automatiquement : {str(e)}")
        return ""

# Déclaration du DAG
with DAG(
    'dag_export_salesforce_full_all_columns',
    default_args=default_args,
    description='Export complet de T_ExportDataSalesforce vers GCS (toutes colonnes)',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    payload = {
        "launcher": {
            "Consommateur": "Equipe_Sales",
            "Job_name": "export_salesforce_full_all_columns",
            "Action": "Config_and_Run"
        },
        "params": {
            "job_name": "export_salesforce_full_all_columns",
            "export_type": "Full",
            "expected_date_format": "dd/MM/yyyy HH:mm:ss",
            "decimal_separator": ",",
            "Column_partition": None,
            "last_Value": None,
            "last_Value_reprise": None,
            "dry_run": False,
            "parameters_Delta_Export": {
                "Delta_column": None,
                "last_export_date": None,
                "depth_days": None
            },
            "source": {
                "project_source": "sandbox-damadou",
                "dataset_id": "SquadData",
                "table_id": "T_ExportDataSalesforce",
                "selected_columns": ["*"],
                "Filtrage_autres": None
            },
            "destination": {
                "project_destination": "sandbox-damadou",
                "bucket_name": "sandbox-damadou-fd-export-salesforce-bi",
                "file_name_prefix": "export_salesforce_full_",
                "type_extraction": "CSV"
            }
        }
    }

    trigger_export = HttpOperator(
        task_id='trigger_export_full_salesforce',
        http_conn_id='cloud_run_api',
        endpoint='api/v1/export',
        method='POST',
        data=json.dumps(payload),
        headers={
            "Content-Type": "application/json",
            "Authorization": get_google_oidc_token()
        },
        extra_options={"check_response": False}
    )

    trigger_export
