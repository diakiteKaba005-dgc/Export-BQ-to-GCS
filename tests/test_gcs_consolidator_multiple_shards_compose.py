from gcs.consolidator import GCSConsolidator


class FakeBlob:
    def __init__(self, name):
        self.name = name
        self.composed = []
        self.deleted = False

    def compose(self, sources):
        self.composed.append([source.name for source in sources])

    def delete(self):
        self.deleted = True


class FakeBucket:
    def __init__(self, blobs):
        self._blobs = blobs
        self._created = {}

    def list_blobs(self, prefix):
        return self._blobs

    def blob(self, name):
        if name not in self._created:
            self._created[name] = FakeBlob(name)
        return self._created[name]

    def rename_blob(self, blob, new_name):
        blob.name = new_name


class FakeClient:
    def __init__(self, bucket):
        self._bucket = bucket

    def bucket(self, bucket_name):
        return self._bucket


def test_consolidator_composes_more_than_32_shards(monkeypatch):
    shards = [FakeBlob(f"extract_finance_T_ExportDataSalesforce_20240601_000000_{i}.csv") for i in range(33)]
    fake_bucket = FakeBucket(shards)

    monkeypatch.setattr("gcs.consolidator.storage.Client", lambda *args, **kwargs: FakeClient(fake_bucket))

    consolidator = GCSConsolidator()
    final_uri = consolidator.consolidate_shards(
        bucket_name="sandbox-damadou-fd-export-finance-bi",
        file_prefix="extract_finance_",
        table_id="T_ExportDataSalesforce",
        end_time_str="20240601_000000",
        format_type="CSV"
    )

    assert final_uri == "gs://sandbox-damadou-fd-export-finance-bi/extract_finance_T_ExportDataSalesforce_20240601_000000_all.csv"
    assert all(blob.deleted for blob in shards)
    assert "extract_finance_T_ExportDataSalesforce_20240601_000000_all.csv" in fake_bucket._created
    final_blob = fake_bucket._created["extract_finance_T_ExportDataSalesforce_20240601_000000_all.csv"]
    assert len(final_blob.composed) >= 1
