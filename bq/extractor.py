import os
from google.cloud import bigquery
from datetime import datetime, timedelta

class BQExtractor:
    def __init__(self, defaults: dict | None = None, credentials=None):
        self.client = None
        self.defaults = defaults or {}
        self.credentials = credentials

    def _resolve_project(self, src: dict) -> str:
        # Priorité: source def dans la config -> defaults.project_id -> env GOOGLE_CLOUD_PROJECT
        return src.get("project_source") or self.defaults.get("project_id") or os.environ.get("GOOGLE_CLOUD_PROJECT")

    def get_client(self):
        if self.client is None:
            if self.credentials is not None:
                self.client = bigquery.Client(credentials=self.credentials)
            else:
                self.client = bigquery.Client()
        return self.client

    def build_where_clauses(self, config: dict) -> list[str]:
        src = config["source"]
        where_clauses = []
        export_type = config.get("export_type")

        if export_type == "Delta" and config.get("parameters_Delta_Export"):
            delta_cfg = config["parameters_Delta_Export"]
            delta_col = delta_cfg.get("Delta_column")
            if delta_col:
                if delta_cfg.get("last_export_date"):
                    last_exp = delta_cfg.get("last_export_date")
                    where_clauses.append(f"CAST({delta_col} AS TIMESTAMP) > TIMESTAMP('{last_exp}')")
                else:
                    depth_days = delta_cfg.get("depth_days")
                    if depth_days is not None and str(depth_days).upper() != "NULL":
                        charniere_date = (datetime.utcnow() - timedelta(days=int(depth_days))).strftime("%Y-%m-%dT%H:%M:%S")
                        where_clauses.append(f"CAST({delta_col} AS TIMESTAMP) >= TIMESTAMP('{charniere_date}')")

        elif export_type == "Full_Referentiel":
            column_partition = config.get("Column_partition")
            last_value = config.get("last_Value")
            last_value_reprise = config.get("last_Value_reprise")
            if column_partition and last_value and last_value_reprise:
                where_clauses.append(
                    f"CAST({column_partition} AS TIMESTAMP) > TIMESTAMP('{last_value}')"
                )
                where_clauses.append(
                    f"CAST({column_partition} AS TIMESTAMP) <= TIMESTAMP('{last_value_reprise}')"
                )

        if src.get("Filtrage_autres"):
            clean_filter = src["Filtrage_autres"].strip()
            if clean_filter.upper().startswith("WHERE"):
                condition = clean_filter[5:].strip()
                if condition:
                    where_clauses.append(condition)

        return where_clauses

    def build_max_delta_query(self, config: dict) -> str | None:
        if config.get("export_type") != "Delta" or not config.get("parameters_Delta_Export"):
            return None

        delta_col = config["parameters_Delta_Export"].get("Delta_column")
        if not delta_col:
            return None

        src = config["source"]
        project = self._resolve_project(src)
        query = f"SELECT MAX(CAST({delta_col} AS TIMESTAMP)) AS max_delta FROM `{project}.{src['dataset_id']}.{src['table_id']}`"
        where_clauses = self.build_where_clauses(config)
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
        return query

    def get_last_export_timestamp(self, config: dict):
        query = self.build_max_delta_query(config)
        if not query:
            return None

        client = self.get_client()
        query_job = client.query(query)
        result = query_job.result()
        row = next(iter(result), None)
        if not row:
            return None

        value = row[0]
        if not value:
            return None

        if hasattr(value, "isoformat"):
            return value.isoformat()
        return str(value)

    def build_query(self, config: dict) -> str:
        """Assemble dynamiquement la requête SQL avec gestion du mode Delta (Incrémental)."""
        src = config["source"]
        cols = ", ".join(src["selected_columns"])
        project = self._resolve_project(src)
        query = f"SELECT {cols} FROM `{project}.{src['dataset_id']}.{src['table_id']}`"
        where_clauses = self.build_where_clauses(config)

        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)

        if config.get("dry_run"):
            query += " LIMIT 100"

        print(f"[BQExtractor] Requête finale compilée : {query}")
        return query

    def estimate_costs(self, query: str) -> int:
        """Simule la requête (Dry Run) pour évaluer le volume de données scanné."""
        client = self.get_client()
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        query_job = client.query(query, job_config=job_config)
        return query_job.total_bytes_processed

    def extract_to_gcs(self, query: str, destination_uri: str, format_type: str) -> int:
        """Exécute la requête et extrait directement les résultats vers GCS."""
        format_map = {
            "CSV": bigquery.DestinationFormat.CSV,
            "PARQUET": bigquery.DestinationFormat.PARQUET,
            "AVRO": bigquery.DestinationFormat.AVRO
        }
        
        # 1. Étape intermédiaire : Exécution de la requête vers la table temporaire
        client = self.get_client()
        query_job = client.query(query)
        result = query_job.result()  # Attend la fin du traitement BigQuery
        temp_table = query_job.destination
        
        # 2. Lancement de l'extraction de la table temporaire vers le Bucket GCS
        extract_config = bigquery.ExtractJobConfig(
            destination_format=format_map.get(format_type.upper(), bigquery.DestinationFormat.CSV),
            print_header=True
        )
        
        extract_job = self.client.extract_table(
            temp_table,
            destination_uri,
            job_config=extract_config
        )
        extract_job.result()  # Attend que GCS ait fini d'écrire les éclats (.csv)
        
        return result.total_rows