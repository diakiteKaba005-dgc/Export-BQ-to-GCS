import pytest

from bq.extractor import BQExtractor


class FakeQueryJob:
    def __init__(self):
        self.destination = "fake_temp_table"
        self.total_rows = 42

    def result(self):
        return self


class FakeClient:
    def __init__(self):
        self.extract_calls = []

    def query(self, query):
        return FakeQueryJob()

    def extract_table(self, temp_table, destination_uri, job_config):
        self.extract_calls.append((temp_table, destination_uri, job_config))
        return FakeQueryJob()


def test_extract_to_gcs_maps_csv_parquet_avro(monkeypatch):
    monkeypatch.setattr("bq.extractor.bigquery.Client", lambda *args, **kwargs: FakeClient())

    extractor = BQExtractor()

    extractor.extract_to_gcs("SELECT 1", "gs://bucket/file_*.csv", "CSV")
    extractor.extract_to_gcs("SELECT 1", "gs://bucket/file_*.parquet", "PARQUET")
    extractor.extract_to_gcs("SELECT 1", "gs://bucket/file_*.avro", "AVRO")

    assert extractor.client.extract_calls[0][2].destination_format == "CSV"
    assert extractor.client.extract_calls[1][2].destination_format == "PARQUET"
    assert extractor.client.extract_calls[2][2].destination_format == "AVRO"


def test_extract_to_gcs_uses_csv_as_default_for_invalid_type(monkeypatch):
    monkeypatch.setattr("bq.extractor.bigquery.Client", lambda *args, **kwargs: FakeClient())

    extractor = BQExtractor()
    extractor.extract_to_gcs("SELECT 1", "gs://bucket/file_*.csv", "UNKNOWN")

    assert extractor.client.extract_calls[0][2].destination_format == "CSV"
