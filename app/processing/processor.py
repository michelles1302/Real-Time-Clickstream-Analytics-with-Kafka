from datetime import datetime, timezone
import dateutil.parser
from typing import Dict, Any, List, Tuple, Optional
import pandas as pd

class EventDeduplicator:
    """
    In-memory duplicate detector based on event_id.
    Maintains a rolling window of recent event IDs.
    """
    def __init__(self, max_capacity: int = 100000):
        self.max_capacity = max_capacity
        self.seen_ids = set()
        self.id_queue = []

    def is_duplicate(self, event_id: str) -> bool:
        if not event_id:
            return False

        if event_id in self.seen_ids:
            return True

        self.seen_ids.add(event_id)
        self.id_queue.append(event_id)

        # Evict oldest entries if over capacity
        if len(self.id_queue) > self.max_capacity:
            oldest_id = self.id_queue.pop(0)
            self.seen_ids.discard(oldest_id)

        return False

    def clear(self):
        self.seen_ids.clear()
        self.id_queue.clear()


class EventProcessor:
    def __init__(self):
        self.deduplicator = EventDeduplicator()

    def process_event(self, event: Dict[str, Any]) -> Tuple[bool, Dict[str, Any], Optional[str]]:
        """
        Process a single valid event:
        1. Check duplicate
        2. Normalize timestamp to UTC string
        3. Extract year, month, day partition attributes
        4. Add ingestion_timestamp
        Returns (success, processed_event, error_message)
        """
        event_id = event.get("event_id", "")
        if self.deduplicator.is_duplicate(event_id):
            return False, event, f"Duplicate event_id detected: {event_id}"

        processed = dict(event)

        # Normalize timestamp
        ts_raw = processed.get("timestamp")
        try:
            dt = dateutil.parser.isoparse(str(ts_raw))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            
            iso_utc = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            processed["timestamp"] = iso_utc
            processed["year"] = dt.strftime("%Y")
            processed["month"] = dt.strftime("%m")
            processed["day"] = dt.strftime("%d")
        except Exception as e:
            return False, event, f"Failed to parse timestamp '{ts_raw}': {e}"

        # Add ingestion_timestamp
        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        processed["ingestion_timestamp"] = now_utc

        # Standardize strings
        for key in ["event_id", "user_id", "session_id", "event_type", "device_type", "browser", "country"]:
            if key in processed and isinstance(processed[key], str):
                processed[key] = processed[key].strip()

        return True, processed, None

    @staticmethod
    def events_to_dataframe(events: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Convert a list of processed event dictionaries into a cleaned Pandas DataFrame.
        """
        if not events:
            return pd.DataFrame()

        df = pd.DataFrame(events)

        # Standardize column types
        string_cols = [
            "event_id", "user_id", "session_id", "event_type",
            "product_id", "page_url", "device_type", "browser",
            "country", "year", "month", "day", "timestamp", "ingestion_timestamp"
        ]

        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).replace("nan", None).replace("None", None)

        return df
