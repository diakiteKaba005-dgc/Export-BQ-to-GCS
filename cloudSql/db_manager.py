import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime

class DBManager:
    def __init__(self):
        config_path = os.path.join(os.path.dirname(__file__), 'connexion.json')

        env_db_host = os.getenv('DB_HOST')
        env_db_name = os.getenv('DB_NAME')
        env_db_user = os.getenv('DB_USER')

        # 1. Priorité aux variables d'environnement si elles sont définies.
        #    Cela permet d'utiliser la même connexion PostgreSQL que sur Cloud Run
        #    même en local, sans se baser sur le socket Cloud SQL local.
        if env_db_host and env_db_name and env_db_user:
            env_db_password = os.getenv('DB_PASSWORD')
            self.dsn = (
                f"host={env_db_host} "
                f"port={os.getenv('DB_PORT', 5432)} "
                f"dbname={env_db_name} "
                f"user={env_db_user}"
            )
            if env_db_password:
                self.dsn += f" password={env_db_password}"
            return

        # 2. Sinon, lecture du fichier de config locale.
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                self.config = json.load(f)

            db_host = self.config['DB_HOST']
            db_port = self.config['DB_PORT']
            db_password = self.config.get('DB_PASSWORD')

            # Cas local : si la configuration pointe vers un socket Cloud SQL
            # mais que l'on souhaite utiliser PostgreSQL localement, on remplace
            # par l'hôte local défini via DB_HOST_LOCAL ou par 127.0.0.1.
            local_override = os.getenv('DB_HOST_LOCAL')
            if db_host.startswith('/cloudsql/'):
                db_host = local_override or '127.0.0.1'

            self.dsn = (
                f"host={db_host} "
                f"port={db_port} "
                f"dbname={self.config['DB_NAME']} "
                f"user={self.config['DB_USER']}"
            )
            if db_password:
                self.dsn += f" password={db_password}"
            return

        raise RuntimeError(
            'Aucune configuration PostgreSQL trouvée. Définissez DB_HOST, DB_NAME, DB_USER '
            'dans les variables d environnement ou créez cloudSql/connexion.json.'
        )

    def _get_connection(self):
        return psycopg2.connect(self.dsn)

    def init_db_schema(self):
        """Vérifie la présence des tables et exécute les DDL si nécessaire."""
        queries = [
            """
            CREATE TABLE IF NOT EXISTS public.config_export_bq_to_gcs (
                job_name VARCHAR(255) PRIMARY KEY,
                consommateur VARCHAR(100) NOT NULL,
                export_type VARCHAR(50) NOT NULL,
                expected_date_format VARCHAR(50) DEFAULT 'dd/MM/yyyy HH:mm:ss',
                decimal_separator VARCHAR(5) DEFAULT ',',
                column_partition VARCHAR(100),
                last_value TIMESTAMP WITH TIME ZONE,
                last_value_reprise TIMESTAMP WITH TIME ZONE,
                dry_run BOOLEAN DEFAULT FALSE,
                delta_column VARCHAR(100),
                last_export_date TIMESTAMP WITH TIME ZONE,
                depth_days INT,
                project_source VARCHAR(100) NOT NULL,
                dataset_id VARCHAR(100) NOT NULL,
                table_id VARCHAR(100) NOT NULL,
                selected_columns TEXT[] NOT NULL,
                filtrage_autres TEXT,
                project_destination VARCHAR(100) NOT NULL,
                bucket_name VARCHAR(255) NOT NULL,
                file_name_prefix VARCHAR(255),
                type_extraction VARCHAR(50) NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """,
            "CREATE INDEX IF NOT EXISTS idx_config_consommateur ON public.config_export_bq_to_gcs(consommateur);",
            """
            CREATE TABLE IF NOT EXISTS public.logs_export_bq_to_gcs (
                execution_id VARCHAR(255) NOT NULL,
                start_time TIMESTAMP WITH TIME ZONE NOT NULL,
                end_time TIMESTAMP WITH TIME ZONE,
                status VARCHAR(50) NOT NULL,
                rows_exported BIGINT,
                bytes_processed BIGINT,
                error_message TEXT,
                job_name VARCHAR(255) NOT NULL,
                recipient_name VARCHAR(255),
                action VARCHAR(50),
                export_type VARCHAR(50),
                expected_date_format VARCHAR(50),
                decimal_separator VARCHAR(5),
                output_encoding VARCHAR(50),
                dry_run BOOLEAN,
                column_partition VARCHAR(100),
                last_value TIMESTAMP WITH TIME ZONE,
                last_value_reprise TIMESTAMP WITH TIME ZONE,
                applied_filter_date DATE,
                input_delta_column VARCHAR(100),
                input_last_export_date VARCHAR(100),
                input_depth_days INT,
                source_project_id VARCHAR(100),
                source_dataset_id VARCHAR(100),
                source_table_id VARCHAR(255),
                input_selected_columns TEXT[],
                source_filtrage_autres TEXT,
                destination_project VARCHAR(100),
                destination_bucket_name VARCHAR(255),
                destination_file_name_prefix VARCHAR(255),
                destination_type_extraction VARCHAR(50),
                destination_uri TEXT,
                PRIMARY KEY (execution_id, start_time)
            ) PARTITION BY RANGE (start_time);
            """,
            "CREATE INDEX IF NOT EXISTS idx_logs_job_name ON public.logs_export_bq_to_gcs(job_name);",
            "CREATE INDEX IF NOT EXISTS idx_logs_status ON public.logs_export_bq_to_gcs(status);",
            "CREATE TABLE IF NOT EXISTS public.logs_export_bq_to_gcs_default PARTITION OF public.logs_export_bq_to_gcs DEFAULT;"
        ]
        
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                for query in queries:
                    cur.execute(query)
            conn.commit()

    def save_or_update_config(self, params: dict, consommateur: str):
        """Sauvegarde ou écrase la configuration en base (Upsert via job_name)."""
        query = """
            INSERT INTO public.config_export_bq_to_gcs (
                job_name, consommateur, export_type, expected_date_format, decimal_separator,
                column_partition, last_value, last_value_reprise, dry_run,
                delta_column, last_export_date, depth_days,
                project_source, dataset_id, table_id, selected_columns, filtrage_autres,
                project_destination, bucket_name, file_name_prefix, type_extraction, updated_at
            ) VALUES (
                %(job_name)s, %(consommateur)s, %(export_type)s, %(expected_date_format)s, %(decimal_separator)s,
                %(column_partition)s, %(last_value)s, %(last_value_reprise)s, %(dry_run)s,
                %(delta_column)s, %(last_export_date)s, %(depth_days)s,
                %(project_source)s, %(dataset_id)s, %(table_id)s, %(selected_columns)s, %(filtrage_autres)s,
                %(project_destination)s, %(bucket_name)s, %(file_name_prefix)s, %(type_extraction)s, CURRENT_TIMESTAMP
            )
            ON CONFLICT (job_name) DO UPDATE SET
                consommateur = EXCLUDED.consommateur,
                export_type = EXCLUDED.export_type,
                expected_date_format = EXCLUDED.expected_date_format,
                decimal_separator = EXCLUDED.decimal_separator,
                column_partition = EXCLUDED.column_partition,
                last_value = EXCLUDED.last_value,
                last_value_reprise = EXCLUDED.last_value_reprise,
                dry_run = EXCLUDED.dry_run,
                delta_column = EXCLUDED.delta_column,
                last_export_date = EXCLUDED.last_export_date,
                depth_days = EXCLUDED.depth_days,
                project_source = EXCLUDED.project_source,
                dataset_id = EXCLUDED.dataset_id,
                table_id = EXCLUDED.table_id,
                selected_columns = EXCLUDED.selected_columns,
                filtrage_autres = EXCLUDED.filtrage_autres,
                project_destination = EXCLUDED.project_destination,
                bucket_name = EXCLUDED.bucket_name,
                file_name_prefix = EXCLUDED.file_name_prefix,
                type_extraction = EXCLUDED.type_extraction,
                updated_at = CURRENT_TIMESTAMP;
        """
        delta_p = params.get("parameters_Delta_Export", {})
        flat_params = {
            "job_name": params["job_name"],
            "consommateur": consommateur,
            "export_type": params["export_type"],
            "expected_date_format": params.get("expected_date_format"),
            "decimal_separator": params.get("decimal_separator"),
            "column_partition": params.get("Column_partition"),
            "last_value": params.get("last_Value"),
            "last_value_reprise": params.get("last_Value_reprise"),
            "dry_run": params.get("dry_run", False),
            "delta_column": delta_p.get("Delta_column"),
            "last_export_date": delta_p.get("last_export_date"),
            "depth_days": delta_p.get("depth_days"),
            "project_source": params["source"]["project_source"],
            "dataset_id": params["source"]["dataset_id"],
            "table_id": params["source"]["table_id"],
            "selected_columns": params["source"]["selected_columns"],
            "filtrage_autres": params["source"].get("Filtrage_autres"),
            "project_destination": params["destination"]["project_destination"],
            "bucket_name": params["destination"]["bucket_name"],
            "file_name_prefix": params["destination"].get("file_name_prefix"),
            "type_extraction": params["destination"]["type_extraction"]
        }
        
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, flat_params)
            conn.commit()

    def get_config(self, job_name: str) -> dict:
        """Récupère la configuration depuis PostgreSQL et reconstruit la structure JSON attendue."""
        query = "SELECT * FROM public.config_export_bq_to_gcs WHERE job_name = %s;"
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, (job_name,))
                row = cur.fetchone()
                
        if not row:
            raise ValueError(f"Aucune configuration trouvée pour le job : {job_name}")
            
        # APPLICATION RECOMMANDATIONS : Correction stricte de la casse des clés pour main.py et Pydantic
        return {
            "job_name": row["job_name"],
            "export_type": row["export_type"],
            "expected_date_format": row["expected_date_format"],
            "decimal_separator": row["decimal_separator"],
            "Column_partition": row["column_partition"],
            "last_Value": row["last_value"].isoformat() if row["last_value"] else None,
            "last_Value_reprise": row["last_value_reprise"].isoformat() if row["last_value_reprise"] else None,
            "dry_run": row["dry_run"],
            "parameters_Delta_Export": {
                "Delta_column": row["delta_column"],
                "last_export_date": row["last_export_date"].isoformat() if row["last_export_date"] else None,
                "depth_days": row["depth_days"]
            },
            "source": {
                "project_source": row["project_source"],
                "dataset_id": row["dataset_id"],
                "table_id": row["table_id"],
                "selected_columns": row["selected_columns"],
                "Filtrage_autres": row["filtrage_autres"]
            },
            "destination": {
                "project_destination": row["project_destination"],
                "bucket_name": row["bucket_name"],
                "file_name_prefix": row["file_name_prefix"],
                "type_extraction": row["type_extraction"]
            }
        }

    def insert_initial_log(self, exec_id: str, start_time: datetime, action: str, consommateur: str, params: dict):
        """Écrit le log initial à l'état RUNNING avec l'ensemble des métadonnées de configuration."""
        query = """
            INSERT INTO public.logs_export_bq_to_gcs (
                execution_id, start_time, status, job_name, recipient_name, action,
                export_type, expected_date_format, decimal_separator, dry_run,
                column_partition, last_value, last_value_reprise,
                input_delta_column, input_last_export_date, input_depth_days,
                source_project_id, source_dataset_id, source_table_id, input_selected_columns, source_filtrage_autres,
                destination_project, destination_bucket_name, destination_file_name_prefix, destination_type_extraction
            ) VALUES (
                %s, %s, 'RUNNING', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            );
        """
        delta_p = params.get("parameters_Delta_Export", {})
        values = (
            exec_id, start_time, params["job_name"], consommateur, action,
            params["export_type"], params.get("expected_date_format"), params.get("decimal_separator"), params.get("dry_run"),
            params.get("Column_partition"), params.get("last_Value"), params.get("last_Value_reprise"),
            delta_p.get("Delta_column"), delta_p.get("last_export_date"), delta_p.get("depth_days"),
            params["source"]["project_source"], params["source"]["dataset_id"], params["source"]["table_id"],
            params["source"]["selected_columns"], params["source"].get("Filtrage_autres"),
            params["destination"]["project_destination"], params["destination"]["bucket_name"],
            params["destination"].get("file_name_prefix"), params["destination"]["type_extraction"]
        )
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, values)
            conn.commit()

    def finalize_log(self, exec_id: str, start_time: datetime, status: str, end_time: datetime, 
                     rows: int = None, bytes_proc: int = None, error: str = None, uri: str = None):
        """Met à jour le statut final du log d'exécution (SUCCESS ou FAILED)."""
        query = """
            UPDATE public.logs_export_bq_to_gcs
            SET status = %s, end_time = %s, rows_exported = %s, bytes_processed = %s, error_message = %s, destination_uri = %s
            WHERE execution_id = %s AND start_time = %s;
        """
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (status, end_time, rows, bytes_proc, error, uri, exec_id, start_time))
            conn.commit()