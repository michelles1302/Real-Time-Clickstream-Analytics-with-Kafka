import pytest
from app.processing.processor import EventProcessor, EventDeduplicator
from app.processing.event_generator import ClickstreamEventGenerator

def test_deduplicator():
    dedup = EventDeduplicator(max_capacity=10)
    assert dedup.is_duplicate("evt_101") is False
    assert dedup.is_duplicate("evt_101") is True
    assert dedup.is_duplicate("evt_102") is False

def test_processor_normalization():
    processor = EventProcessor()
    generator = ClickstreamEventGenerator(seed=1)
    event = generator.generate_event()

    success, processed, err = processor.process_event(event)
    assert success is True
    assert err is None
    assert "ingestion_timestamp" in processed
    assert "year" in processed
    assert "month" in processed
    assert "day" in processed
    assert processed["timestamp"].endswith("Z")

def test_processor_detects_duplicate_event():
    processor = EventProcessor()
    generator = ClickstreamEventGenerator(seed=5)
    event = generator.generate_event()

    # First pass: success
    s1, p1, e1 = processor.process_event(event)
    assert s1 is True

    # Second pass with same event_id: duplicate failure
    s2, p2, e2 = processor.process_event(event)
    assert s2 is False
    assert "Duplicate event_id" in e2

def test_processor_events_to_dataframe():
    processor = EventProcessor()
    generator = ClickstreamEventGenerator()
    raw_events = generator.generate_batch(10)
    processed_list = [processor.process_event(e)[1] for e in raw_events]

    df = processor.events_to_dataframe(processed_list)
    assert len(df) == 10
    assert "ingestion_timestamp" in df.columns
    assert "year" in df.columns
