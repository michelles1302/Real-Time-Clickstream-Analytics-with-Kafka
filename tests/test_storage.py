import tempfile
from pathlib import Path
import pandas as pd
import pytest
from moto import mock_aws
import boto3

from app.storage.local_storage import LocalStorageWriter
from app.storage.s3_storage import S3StorageWriter
from app.processing.event_generator import ClickstreamEventGenerator
from app.processing.processor import EventProcessor

def test_local_storage_writer():
    with tempfile.TemporaryDirectory() as tmp_proc, tempfile.TemporaryDirectory() as tmp_rej:
        writer = LocalStorageWriter(
            base_processed_dir=Path(tmp_proc),
            base_rejected_dir=Path(tmp_rej)
        )

        generator = ClickstreamEventGenerator(seed=10)
        processor = EventProcessor()
        events = [processor.process_event(generator.generate_event())[1] for _ in range(20)]
        df = processor.events_to_dataframe(events)

        written_paths = writer.write_batch(df)
        assert len(written_paths) > 0
        assert written_paths[0].exists()
        assert written_paths[0].name.endswith(".parquet")

        # Read parquet back and verify length
        read_df = pd.read_parquet(written_paths[0])
        assert len(read_df) == 20

def test_local_storage_rejected_writer():
    with tempfile.TemporaryDirectory() as tmp_proc, tempfile.TemporaryDirectory() as tmp_rej:
        writer = LocalStorageWriter(
            base_processed_dir=Path(tmp_proc),
            base_rejected_dir=Path(tmp_rej)
        )

        rejected = [{"event_id": "evt_bad", "reason": "Missing user_id"}]
        rej_path = writer.write_rejected(rejected)

        assert rej_path is not None
        assert rej_path.exists()
        assert rej_path.name.endswith(".json")

@mock_aws
def test_s3_storage_writer():
    bucket_name = "test-clickstream-bucket"
    region = "us-east-1"

    # Create mock S3 bucket
    s3 = boto3.client("s3", region_name=region)
    s3.create_bucket(Bucket=bucket_name)

    s3_writer = S3StorageWriter(
        bucket_name=bucket_name,
        region=region,
        access_key="testing",
        secret_key="testing"
    )

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
        tmp_file.write(b"dummy parquet bytes")
        tmp_path = Path(tmp_file.name)

    s3_key = "processed/year=2026/month=08/day=16/events_test.parquet"
    success = s3_writer.upload_file(tmp_path, s3_key)
    assert success is True

    # Verify object exists in mock S3 bucket
    resp = s3.list_objects_v2(Bucket=bucket_name)
    assert "Contents" in resp
    assert resp["Contents"][0]["Key"] == s3_key
