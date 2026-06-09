from google.cloud import bigquery
from datetime import datetime, timedelta

class BQExtractor:
    def __init__(self):
        self.client = bigquery.Client()

    def build_query(self, config: dict) -> str:
        """Assemble dynamiquement la requête SQL avec gestion du mode Delta (Incrémental)."""
        src = config["source"]
        cols = ", ".join(src["selected_columns"])
        
        # CORRECTION BUG 1 : Accès correct à table_id via le dictionnaire src
        query = f"SELECT {cols} FROM `{src['project_source']}.{src['dataset_id']}.{src['table_id']}`"
        
        # Conteneur pour nos clauses WHERE dynamiques
        where_clauses = []
        
        # 1. Gestion de la logique d'exportation Delta
        if config.get("export_type") == "Delta" and config.get("parameters_Delta_Export"):
            delta_cfg = config["parameters_Delta_Export"]
            delta_col = delta_cfg.get("Delta_column")
            
            if delta_col:
                # Récupération de la profondeur en jours (depth_days)
                depth_days = delta_cfg.get("depth_days")
                
                # Si depth_days est fourni (différent de None), on calcule la date charnière
                if depth_days is not None and str(depth_days).upper() != "NULL":
                    # On calcule : Date du jour - X jours de profondeur
                    charniere_date = (datetime.utcnow() - timedelta(days=int(depth_days))).strftime("%Y-%m-%dT%H:%M:%S")
                    # Cast the column to TIMESTAMP to avoid DATE vs TIMESTAMP comparison errors
                    where_clauses.append(f"CAST({delta_col} AS TIMESTAMP) >= TIMESTAMP('{charniere_date}')")
                
                # Si depth_days n'est pas fourni, on se base sur la dernière date d'export (last_export_date)
                elif delta_cfg.get("last_export_date"):
                    last_exp = delta_cfg.get("last_export_date")
                    where_clauses.append(f"CAST({delta_col} AS TIMESTAMP) >= TIMESTAMP('{last_exp}')")

        # 2. Ajout des filtres utilisateurs additionnels (ex: WHERE 1=1)
        if src.get("Filtrage_autres"):
            clean_filter = src["Filtrage_autres"].strip()
            # Si le filtre commence par WHERE, on extrait juste la condition
            if clean_filter.upper().startswith("WHERE"):
                condition = clean_filter[5:].strip()
                if condition:
                    where_clauses.append(condition)

        # Assemblage final des clauses WHERE
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
            
        # 3. Sécurité d'échantillonnage pour le Dry Run
        if config.get("dry_run"):
            query += " LIMIT 100"
            
        print(f"[BQExtractor] Requête finale compilée : {query}")
        return query

    def estimate_costs(self, query: str) -> int:
        """Simule la requête (Dry Run) pour évaluer le volume de données scanné."""
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        query_job = self.client.query(query, job_config=job_config)
        return query_job.total_bytes_processed

    def extract_to_gcs(self, query: str, destination_uri: str, format_type: str) -> int:
        """Exécute la requête et extrait directement les résultats vers GCS."""
        format_map = {
            "CSV": bigquery.DestinationFormat.CSV,
            "PARQUET": bigquery.DestinationFormat.PARQUET,
            "AVRO": bigquery.DestinationFormat.AVRO
        }
        
        # 1. Étape intermédiaire : Exécution de la requête vers la table temporaire
        query_job = self.client.query(query)
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