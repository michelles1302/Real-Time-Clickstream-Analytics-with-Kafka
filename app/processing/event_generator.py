import random
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Generator, Optional, List

ALLOWED_EVENT_TYPES = [
    "page_view",
    "product_view",
    "search",
    "add_to_cart",
    "remove_from_cart",
    "wishlist",
    "checkout",
    "purchase",
    "logout"
]

DEVICE_TYPES = ["mobile", "desktop", "tablet"]
BROWSERS = ["Chrome", "Safari", "Firefox", "Edge"]
COUNTRIES = ["India", "United States", "United Kingdom", "Germany", "Canada", "Australia", "Japan", "France"]
SEARCH_QUERIES = ["laptop", "wireless headphones", "smartwatch", "running shoes", "gaming monitor", "mechanical keyboard", "backpack", "smartphone"]

# Large pool of user IDs (3000 unique users) to allow realistic conversion funnels
USER_POOL = [f"user_{i}" for i in range(1001, 4001)]
PRODUCT_POOL = [f"product_{i}" for i in range(1, 51)]

class ClickstreamEventGenerator:
    def __init__(self, seed: Optional[int] = None):
        if seed is not None:
            random.seed(seed)
        self._counter = 1000

    def _next_event_id(self) -> str:
        self._counter += 1
        unique_suffix = uuid.uuid4().hex[:6]
        return f"evt_{self._counter}_{unique_suffix}"

    def _get_user_allowed_event_types(self, user_id: str) -> List[str]:
        """
        Assign user intent tier based on user_id hash:
        - 5% Buyers: can perform all event types (including checkout & purchase)
        - 13% Cart Shoppers: can perform product views, cart actions, wishlist, but do not purchase
        - 82% Browsers: can only perform page views, product views, search, logout
        """
        user_hash = abs(hash(user_id)) % 100

        if user_hash < 5:
            # Buyer tier: full funnel access
            return ALLOWED_EVENT_TYPES
        elif user_hash < 18:
            # Cart shopper tier: views, cart, wishlist
            return ["page_view", "product_view", "search", "add_to_cart", "remove_from_cart", "wishlist", "logout"]
        else:
            # Browser tier: top-of-funnel browsing only
            return ["page_view", "product_view", "search", "logout"]

    def generate_event(
        self,
        event_type: Optional[str] = None,
        timestamp: Optional[datetime] = None,
        force_invalid: bool = False
    ) -> Dict[str, Any]:
        """
        Generate a single realistic clickstream event.
        If force_invalid is True, omits mandatory fields to test validation.
        """
        if force_invalid:
            return {
                "event_id": self._next_event_id(),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "device_type": "mobile"
            }

        user_id = random.choice(USER_POOL)

        if event_type:
            selected_event_type = event_type
        else:
            allowed_types = self._get_user_allowed_event_types(user_id)
            if "purchase" in allowed_types:
                weights = [25, 25, 10, 15, 5, 5, 8, 5, 2] # Buyer weights
            elif "add_to_cart" in allowed_types:
                weights = [35, 30, 15, 12, 3, 3, 2] # Cart shopper weights
            else:
                weights = [45, 35, 15, 5] # Browser weights
            
            selected_event_type = random.choices(allowed_types, weights=weights[:len(allowed_types)], k=1)[0]

        event_time = timestamp if timestamp else datetime.now(timezone.utc)
        iso_timestamp = event_time.strftime("%Y-%m-%dT%H:%M:%SZ")

        session_id = f"session_{abs(hash(user_id + str(event_time.day))) % 9000 + 1000}"
        device_type = random.choice(DEVICE_TYPES)
        browser = random.choice(BROWSERS)
        country = random.choice(COUNTRIES)

        event: Dict[str, Any] = {
            "event_id": self._next_event_id(),
            "user_id": user_id,
            "session_id": session_id,
            "event_type": selected_event_type,
            "timestamp": iso_timestamp,
            "device_type": device_type,
            "browser": browser,
            "country": country
        }

        # Contextual metadata based on event type
        if selected_event_type in ["product_view", "add_to_cart", "remove_from_cart", "wishlist", "checkout", "purchase"]:
            product_id = random.choice(PRODUCT_POOL)
            event["product_id"] = product_id
            event["page_url"] = f"/products/{product_id}"
        elif selected_event_type == "search":
            query = random.choice(SEARCH_QUERIES)
            event["page_url"] = f"/search?q={query.replace(' ', '+')}"
        elif selected_event_type == "page_view":
            pages = ["/home", "/categories/electronics", "/categories/fashion", "/about", "/contact"]
            event["page_url"] = random.choice(pages)
        elif selected_event_type == "checkout":
            event["page_url"] = "/checkout"
        elif selected_event_type == "purchase":
            event["page_url"] = "/order-confirmation"
        elif selected_event_type == "logout":
            event["page_url"] = "/logout"

        return event

    def generate_batch(self, count: int) -> List[Dict[str, Any]]:
        """Generate a specified number of events."""
        return [self.generate_event() for _ in range(count)]

    def stream_events(
        self,
        events_per_second: float = 5.0,
        limit: Optional[int] = None
    ) -> Generator[Dict[str, Any], None, None]:
        """Continuous stream generator with configurable delay."""
        interval = 1.0 / max(events_per_second, 0.1)
        emitted = 0

        while True:
            if limit is not None and emitted >= limit:
                break

            yield self.generate_event()
            emitted += 1

            time.sleep(interval)
