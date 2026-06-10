from bq.extractor import BQExtractor


def test_build_query_with_delta_filter_and_dry_run():
    config = {
        "source": {
            "project_source": "sandbox-damadou",
            "dataset_id": "SquadData",
            "table_id": "T_ExportDataSalesforce",
            "selected_columns": ["*"],
            "Filtrage_autres": "WHERE status = 'active'"
        },
        "export_type": "Delta",
        "parameters_Delta_Export": {
            "Delta_column": "Date",
            "depth_days": 7,
            "last_export_date": "2026-06-01T00:00:00Z"
        },
        "dry_run": True
    }

    extractor = BQExtractor()
    query = extractor.build_query(config)

    assert "SELECT * FROM `sandbox-damadou.SquadData.T_ExportDataSalesforce`" in query
    assert "CAST(Date AS TIMESTAMP) > TIMESTAMP('2026-06-01T00:00:00Z')" in query
    assert "status = 'active'" in query
    assert query.strip().endswith("LIMIT 100")


def test_build_max_delta_query_for_delta_export():
    config = {
        "source": {
            "project_source": "sandbox-damadou",
            "dataset_id": "SquadData",
            "table_id": "T_ExportDataSalesforce",
            "selected_columns": ["*"],
            "Filtrage_autres": "WHERE status = 'active'"
        },
        "export_type": "Delta",
        "parameters_Delta_Export": {
            "Delta_column": "Date",
            "last_export_date": "2026-06-01T00:00:00Z",
            "depth_days": None
        },
        "dry_run": True
    }

    extractor = BQExtractor()
    query = extractor.build_max_delta_query(config)

    assert query is not None
    assert "SELECT MAX(CAST(Date AS TIMESTAMP)) AS max_delta FROM `sandbox-damadou.SquadData.T_ExportDataSalesforce`" in query
    assert "CAST(Date AS TIMESTAMP) > TIMESTAMP('2026-06-01T00:00:00Z')" in query
    assert "status = 'active'" in query


def test_build_query_with_full_referentiel_filters():
    config = {
        "source": {
            "project_source": "sandbox-damadou",
            "dataset_id": "SquadData",
            "table_id": "T_ExportDataSalesforce",
            "selected_columns": ["*"],
            "Filtrage_autres": None
        },
        "export_type": "Full_Referentiel",
        "Column_partition": "Date",
        "last_Value": "2026-06-01T00:00:00Z",
        "last_Value_reprise": "2026-06-03T00:00:00Z",
        "dry_run": False
    }

    extractor = BQExtractor()
    query = extractor.build_query(config)

    assert "SELECT * FROM `sandbox-damadou.SquadData.T_ExportDataSalesforce`" in query
    assert "CAST(Date AS TIMESTAMP) > TIMESTAMP('2026-06-01T00:00:00Z')" in query
    assert "CAST(Date AS TIMESTAMP) <= TIMESTAMP('2026-06-03T00:00:00Z')" in query
