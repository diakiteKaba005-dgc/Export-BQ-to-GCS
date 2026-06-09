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
    assert "CAST(Date AS TIMESTAMP) >=" in query
    assert "status = 'active'" in query
    assert query.strip().endswith("LIMIT 100")
