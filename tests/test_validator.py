import pytest
from app.processing.validator import EventValidator
from app.processing.event_generator import ClickstreamEventGenerator

def test_validator_accepts_valid_event():
    generator = ClickstreamEventGenerator(seed=42)
    event = generator.generate_event()
    
    is_valid, reason = EventValidator.validate(event)
    assert is_valid is True
    assert reason is None

def test_validator_rejects_missing_field():
    generator = ClickstreamEventGenerator()
    event = generator.generate_event()
    del event["user_id"]

    is_valid, reason = EventValidator.validate(event)
    assert is_valid is False
    assert "user_id" in reason

def test_validator_rejects_invalid_event_type():
    generator = ClickstreamEventGenerator()
    event = generator.generate_event()
    event["event_type"] = "invalid_action_type"

    is_valid, reason = EventValidator.validate(event)
    assert is_valid is False
    assert "Invalid event_type" in reason

def test_validator_rejects_invalid_timestamp():
    generator = ClickstreamEventGenerator()
    event = generator.generate_event()
    event["timestamp"] = "not-a-timestamp"

    is_valid, reason = EventValidator.validate(event)
    assert is_valid is False
    assert "Invalid timestamp" in reason

def test_validator_rejects_missing_product_id_when_required():
    generator = ClickstreamEventGenerator()
    event = generator.generate_event(event_type="product_view")
    event["product_id"] = ""

    is_valid, reason = EventValidator.validate(event)
    assert is_valid is False
    assert "product_id" in reason
