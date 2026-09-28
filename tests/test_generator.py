import tempfile
from pathlib import Path
import pandas as pd
import pytest

from app.processing.event_generator import ClickstreamEventGenerator, ALLOWED_EVENT_TYPES, DEVICE_TYPES, BROWSERS, COUNTRIES
from app.processing.processor import EventProcessor
from app.analytics.analytics import ClickstreamAnalytics
from scripts.generate_sample_data import clean_sample_directory
from app.storage.local_storage import LocalStorageWriter

def test_event_generator_required_fields():
    generator = ClickstreamEventGenerator(seed=123)
    event = generator.generate_event()

    required_keys = ["event_id", "user_id", "session_id", "event_type", "timestamp", "device_type", "browser", "country"]
    for key in required_keys:
        assert key in event
        assert event[key] is not None
        assert str(event[key]).strip() != ""

def test_event_generator_allowed_values():
    generator = ClickstreamEventGenerator()
    events = generator.generate_batch(50)

    for evt in events:
        assert evt["event_type"] in ALLOWED_EVENT_TYPES
        assert evt["device_type"] in DEVICE_TYPES
        assert evt["browser"] in BROWSERS
        assert evt["country"] in COUNTRIES

def test_event_generator_unique_ids():
    generator = ClickstreamEventGenerator()
    events = generator.generate_batch(100)
    event_ids = [e["event_id"] for e in events]

    assert len(event_ids) == len(set(event_ids)), "Generated event IDs must be unique"

def test_event_generator_product_id_association():
    generator = ClickstreamEventGenerator()
    evt = generator.generate_event(event_type="product_view")
    assert "product_id" in evt
    assert evt["product_id"].startswith("product_")
    assert "/products/" in evt["page_url"]

def test_event_generator_realistic_funnel_rates():
    """Verify that sample generator produces realistic e-commerce conversion rates."""
    generator = ClickstreamEventGenerator(seed=42)
    processor = EventProcessor()
    raw_events = generator.generate_batch(3000)
    processed = [processor.process_event(e)[1] for e in raw_events]

    df = processor.events_to_dataframe(processed)
    analytics = ClickstreamAnalytics(df)
    kpis = analytics.get_overview_kpis()

    # Verify realistic conversion benchmarks
    assert kpis["total_events"] == 3000
    assert 3.0 <= kpis["add_to_cart_rate"] <= 25.0, f"Add-to-cart rate {kpis['add_to_cart_rate']}% should be realistic (3-25%)"
    assert 0.1 <= kpis["purchase_conversion_rate"] <= 10.0, f"Purchase rate {kpis['purchase_conversion_rate']}% should be realistic (0.1-10%)"

def test_generate_sample_data_overwrite_and_append():
    """Verify sample data script default overwrite vs --append behavior."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        sample_dir = Path(tmp_dir)
        processor = EventProcessor()
        writer = LocalStorageWriter(base_processed_dir=sample_dir)

        generator = ClickstreamEventGenerator(seed=10)

        # Batch 1 (100 events)
        b1 = [processor.process_event(e)[1] for e in generator.generate_batch(100)]
        writer.write_batch(processor.events_to_dataframe(b1))

        files1 = list(sample_dir.glob("**/*.parquet"))
        assert len(files1) == 1

        # Simulate default non-append behavior (clean existing sample files)
        clean_sample_directory(sample_dir)
        assert len(list(sample_dir.glob("**/*.parquet"))) == 0

        # Batch 2 (150 events) after clean
        b2 = [processor.process_event(e)[1] for e in generator.generate_batch(150)]
        writer.write_batch(processor.events_to_dataframe(b2))

        files2 = list(sample_dir.glob("**/*.parquet"))
        assert len(files2) == 1
        df_clean = pd.read_parquet(files2[0])
        assert len(df_clean) == 150

        # Simulate append behavior (batch 3, 50 events)
        b3 = [processor.process_event(e)[1] for e in generator.generate_batch(50)]
        writer.write_batch(processor.events_to_dataframe(b3))

        files3 = list(sample_dir.glob("**/*.parquet"))
        assert len(files3) == 2
        df_appended = pd.concat([pd.read_parquet(f) for f in files3])
        assert len(df_appended) == 200
