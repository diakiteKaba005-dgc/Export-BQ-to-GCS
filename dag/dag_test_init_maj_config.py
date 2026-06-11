from datetime import datetime
import os
from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
import json


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
        print(f"Impossible de générer le token OIDC : {e}")
        return ""


default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 1),
    'email_on_failure': False,
    'retries': 0,
}

with DAG(
    'dag_test_init_maj_config',
    default_args=default_args,
    description='DAG de test pour Init_Maj_config',
    schedule_interval=None,
    catchup=False,
) as dag:

    payload = {
        "launcher": {
            "Consommateur": "Equipe_Test",
            "Job_name": "export_test_init_maj_config",
            "Action": "Init_Maj_config"
        },
        "params": {
            "job_name": "export_test_init_maj_config",
            "export_type": "Full",
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
                "file_name_prefix": "tests/init_maj_config/export_",
                "type_extraction": "CSV"
            }
        }
    }

    trigger = HttpOperator(
        task_id='trigger_test_init_maj_config',
        http_conn_id='cloud_run_api',
        endpoint='api/v1/export',
        method='POST',
        data=json.dumps(payload),
        headers={"Content-Type": "application/json", "Authorization": get_google_oidc_token()},
        extra_options={"check_response": False}
    )

    trigger
