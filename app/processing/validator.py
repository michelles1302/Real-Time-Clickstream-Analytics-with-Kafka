import dateutil.parser
from typing import Dict, Any, Tuple, Optional
from app.processing.event_generator import ALLOWED_EVENT_TYPES

REQUIRED_FIELDS = [
    "event_id",
    "user_id",
    "session_id",
    "event_type",
    "timestamp",
    "device_type",
    "browser",
    "country"
]

PRODUCT_REQUIRED_EVENTS = {
    "product_view",
    "add_to_cart",
    "remove_from_cart",
    "wishlist",
    "checkout",
    "purchase"
}

class EventValidator:
    @staticmethod
    def validate(event: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Validate a clickstream event dictionary against the required schema rules.
        Returns (is_valid, rejection_reason).
        """
        if not isinstance(event, dict):
            return False, "Payload is not a valid JSON object"

        # Check required fields
        for field in REQUIRED_FIELDS:
            if field not in event:
                return False, f"Missing required field: '{field}'"
            val = event[field]
            if val is None or (isinstance(val, str) and not val.strip()):
                return False, f"Field '{field}' cannot be empty or null"

        # Validate event_type
        event_type = event.get("event_type")
        if event_type not in ALLOWED_EVENT_TYPES:
            return False, f"Invalid event_type '{event_type}'. Must be one of {ALLOWED_EVENT_TYPES}"

        # Validate timestamp format
        timestamp_str = event.get("timestamp")
        try:
            dateutil.parser.isoparse(str(timestamp_str))
        except Exception:
            return False, f"Invalid timestamp format '{timestamp_str}'. Must be ISO 8601 compliant"

        # Validate product_id for events requiring it
        if event_type in PRODUCT_REQUIRED_EVENTS:
            product_id = event.get("product_id")
            if not product_id or not str(product_id).strip():
                return False, f"Event type '{event_type}' requires a non-empty 'product_id'"

        return True, None
