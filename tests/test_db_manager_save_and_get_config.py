from cloudSql.db_manager import DBManager


class FakeCursor:
    def __init__(self, row=None):
        self.queries = []
        self.row = row

    def execute(self, query, params=None):
        self.queries.append((query, params))

    def fetchone(self):
        return self.row

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.committed = False

    def cursor(self, cursor_factory=None):
        return self._cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def commit(self):
        self.committed = True


def test_save_or_update_config_upserts_expected_columns(monkeypatch):
    cursor = FakeCursor()
    connection = FakeConnection(cursor)
    monkeypatch.setattr(DBManager, "_get_connection", lambda self: connection)

    manager = DBManager()
    params = {
        "job_name": "export_finance_incremental1",
        "export_type": "Full",
        "expected_date_format": "dd/MM/yyyy HH:mm:ss",
        "decimal_separator": ",",
        "Column_partition": "Date",
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
            "bucket_name": "sandbox-damadou-fd-export-finance-bi",
            "file_name_prefix": "extract_finance_",
            "type_extraction": "CSV"
        }
    }

    manager.save_or_update_config(params, "Equipe_Finance")

    assert connection.committed is True
    assert cursor.queries
    query, query_params = cursor.queries[0]
    assert "INSERT INTO public.config_export_bq_to_gcs" in query
    assert query_params["job_name"] == "export_finance_incremental1"
    assert query_params["project_source"] == "sandbox-damadou"
    assert query_params["type_extraction"] == "CSV"


def test_get_config_reconstructs_expected_payload(monkeypatch):
    row = {
        "job_name": "export_finance_incremental1",
        "export_type": "Full",
        "expected_date_format": "dd/MM/yyyy HH:mm:ss",
        "decimal_separator": ",",
        "column_partition": "Date",
        "last_value": None,
        "last_value_reprise": None,
        "dry_run": False,
        "delta_column": None,
        "last_export_date": None,
        "depth_days": None,
        "project_source": "sandbox-damadou",
        "dataset_id": "SquadData",
        "table_id": "T_ExportDataSalesforce",
        "selected_columns": ["*"],
        "filtrage_autres": None,
        "project_destination": "sandbox-damadou",
        "bucket_name": "sandbox-damadou-fd-export-finance-bi",
        "file_name_prefix": "extract_finance_",
        "type_extraction": "CSV"
    }
    cursor = FakeCursor(row=row)
    connection = FakeConnection(cursor)
    monkeypatch.setattr(DBManager, "_get_connection", lambda self: connection)

    manager = DBManager()
    result = manager.get_config("export_finance_incremental1")

    assert result["job_name"] == "export_finance_incremental1"
    assert result["Column_partition"] == "Date"
    assert result["destination"]["type_extraction"] == "CSV"
    assert result["parameters_Delta_Export"]["Delta_column"] is None
