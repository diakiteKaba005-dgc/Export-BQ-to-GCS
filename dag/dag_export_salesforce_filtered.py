"""DAG de test : Export filtré de T_ExportDataSalesforce (Job = Consultant)

CAS DE TEST :
- Export Full avec filtre utilisateur personnalisé
- Filtrage WHERE Job = 'Consultant'
- Récupération de toutes les colonnes (*)
- Format de sortie : CSV

RÉSULTAT ATTENDU :
- Seuls les enregistrements avec Job='Consultant' sont exportés
- Chemin : gs://sandbox-damadou-fd-export-salesforce-bi/export_salesforce_filtered_consultant_*.csv
- La clause WHERE est ajoutée à la requête BigQuery
- Données d'une population spécifique (les consultants)
- Volume d'export réduit grâce au filtrage métier

EXÉCUTION :
- Quotidienne (schedule_interval='@daily')
- Idéal pour tester le filtrage dynamique et les clauses WHERE personnalisées
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
    'dag_export_salesforce_filtered_by_job',
    default_args=default_args,
    description='Export filtré de T_ExportDataSalesforce par Job=Consultant',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    payload = {
        "launcher": {
            "Consommateur": "Equipe_Sales",
            "Job_name": "export_salesforce_filtered_job_consultant",
            "Action": "Config_and_Run"
        },
        "params": {
            "job_name": "export_salesforce_filtered_job_consultant",
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
                "Filtrage_autres": "WHERE Job = 'Consultant'"
            },
            "destination": {
                "project_destination": "sandbox-damadou",
                "bucket_name": "sandbox-damadou-fd-export-salesforce-bi",
                "file_name_prefix": "export_salesforce_filtered_consultant_",
                "type_extraction": "CSV"
            }
        }
    }

    trigger_export = HttpOperator(
        task_id='trigger_export_filtered_consultant',
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
