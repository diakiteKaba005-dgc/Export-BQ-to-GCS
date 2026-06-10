from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
import json


default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 1),
    'email_on_failure': False,
    'retries': 0,
}


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


with DAG(
    'dag_test_export_full_referentiel',
    default_args=default_args,
    description='DAG de test pour Full_Referentiel',
    schedule_interval=None,
    catchup=False,
) as dag:

    payload = {
        "launcher": {
            "Consommateur": "Equipe_Test",
            "Job_name": "export_test_full_referentiel",
            "Action": "Run"
        },
        "params": {
            "job_name": "export_test_full_referentiel",
            "export_type": "Full_Referentiel",
            "Column_partition": "Date",
            "last_Value": "2026-06-01T00:00:00Z",
            "last_Value_reprise": "2026-06-05T00:00:00Z",
            "dry_run": True,
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
                "file_name_prefix": "tests/full_referentiel/export_",
                "type_extraction": "CSV"
            }
        }
    }

    trigger = HttpOperator(
        task_id='trigger_test_full_referentiel',
        http_conn_id='cloud_run_api',
        endpoint='api/v1/export',
        method='POST',
        data=json.dumps(payload),
        headers={"Content-Type": "application/json", "Authorization": get_google_oidc_token()},
        extra_options={"check_response": False}
    )

    trigger
