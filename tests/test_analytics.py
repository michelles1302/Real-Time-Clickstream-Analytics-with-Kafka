import pandas as pd
import pytest
from app.analytics.analytics import ClickstreamAnalytics

def test_analytics_empty_dataframe():
    analytics = ClickstreamAnalytics(pd.DataFrame())
    kpis = analytics.get_overview_kpis()

    assert kpis["total_events"] == 0
    assert kpis["unique_users"] == 0
    assert kpis["add_to_cart_rate"] == 0.0
    assert kpis["purchase_conversion_rate"] == 0.0

def test_analytics_conversion_formulas():
    data = [
        {"user_id": "u1", "event_type": "product_view", "product_id": "p1", "timestamp": "2026-08-16T10:00:00Z"},
        {"user_id": "u2", "event_type": "product_view", "product_id": "p1", "timestamp": "2026-08-16T10:01:00Z"},
        {"user_id": "u1", "event_type": "add_to_cart", "product_id": "p1", "timestamp": "2026-08-16T10:02:00Z"},
        {"user_id": "u1", "event_type": "purchase", "product_id": "p1", "timestamp": "2026-08-16T10:05:00Z"},
    ]
    df = pd.DataFrame(data)
    analytics = ClickstreamAnalytics(df)
    kpis = analytics.get_overview_kpis()

    assert kpis["total_events"] == 4
    assert kpis["unique_users"] == 2
    assert kpis["total_product_views"] == 2
    assert kpis["total_purchases"] == 1

    # 2 users viewed products, 1 added to cart -> 1/2 = 50.0%
    assert kpis["add_to_cart_rate"] == 50.0
    # 2 users viewed products, 1 purchased -> 1/2 = 50.0%
    assert kpis["purchase_conversion_rate"] == 50.0

def test_analytics_groupings():
    data = [
        {"user_id": "u1", "event_type": "product_view", "device_type": "mobile", "country": "India"},
        {"user_id": "u2", "event_type": "purchase", "device_type": "desktop", "country": "India"},
        {"user_id": "u3", "event_type": "page_view", "device_type": "mobile", "country": "United States"},
    ]
    df = pd.DataFrame(data)
    analytics = ClickstreamAnalytics(df)

    types_df = analytics.get_events_by_type()
    assert len(types_df) == 3

    dev_df = analytics.get_device_distribution()
    assert len(dev_df) == 2

    country_df = analytics.get_country_distribution()
    assert len(country_df) == 2
