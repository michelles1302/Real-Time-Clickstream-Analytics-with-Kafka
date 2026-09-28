from typing import Dict, Any, List
import pandas as pd

class ClickstreamAnalytics:
    def __init__(self, df: pd.DataFrame):
        self.df = df if df is not None else pd.DataFrame()

    def get_overview_kpis(self) -> Dict[str, Any]:
        """Compute key summary KPIs."""
        if self.df.empty:
            return {
                "total_events": 0,
                "unique_users": 0,
                "unique_sessions": 0,
                "total_purchases": 0,
                "total_product_views": 0,
                "total_add_to_cart": 0,
                "add_to_cart_rate": 0.0,
                "purchase_conversion_rate": 0.0
            }

        total_events = len(self.df)
        unique_users = self.df["user_id"].nunique() if "user_id" in self.df.columns else 0
        unique_sessions = self.df["session_id"].nunique() if "session_id" in self.df.columns else 0

        # Event counts
        purchases_df = self.df[self.df["event_type"] == "purchase"] if "event_type" in self.df.columns else pd.DataFrame()
        views_df = self.df[self.df["event_type"] == "product_view"] if "event_type" in self.df.columns else pd.DataFrame()
        cart_df = self.df[self.df["event_type"] == "add_to_cart"] if "event_type" in self.df.columns else pd.DataFrame()

        total_purchases = len(purchases_df)
        total_product_views = len(views_df)
        total_add_to_cart = len(cart_df)

        # Unique users per funnel stage for conversion rates
        users_viewed = views_df["user_id"].nunique() if not views_df.empty and "user_id" in views_df.columns else 0
        users_carted = cart_df["user_id"].nunique() if not cart_df.empty and "user_id" in cart_df.columns else 0
        users_purchased = purchases_df["user_id"].nunique() if not purchases_df.empty and "user_id" in purchases_df.columns else 0

        # Formulas:
        # Add-to-cart rate = (users who added to cart / users who viewed products) * 100
        add_to_cart_rate = (users_carted / users_viewed * 100.0) if users_viewed > 0 else 0.0

        # Purchase conversion rate = (users who purchased / users who viewed products) * 100
        purchase_conversion_rate = (users_purchased / users_viewed * 100.0) if users_viewed > 0 else 0.0

        return {
            "total_events": total_events,
            "unique_users": unique_users,
            "unique_sessions": unique_sessions,
            "total_purchases": total_purchases,
            "total_product_views": total_product_views,
            "total_add_to_cart": total_add_to_cart,
            "add_to_cart_rate": round(add_to_cart_rate, 2),
            "purchase_conversion_rate": round(purchase_conversion_rate, 2)
        }

    def get_events_by_type(self) -> pd.DataFrame:
        """Distribution of event types."""
        if self.df.empty or "event_type" not in self.df.columns:
            return pd.DataFrame(columns=["event_type", "count"])

        res = self.df["event_type"].value_counts().reset_index()
        res.columns = ["event_type", "count"]
        return res

    def get_events_over_time(self, freq: str = "1Min") -> pd.DataFrame:
        """Aggregate events over time intervals."""
        if self.df.empty or "timestamp" not in self.df.columns:
            return pd.DataFrame(columns=["timestamp", "count"])

        temp_df = self.df.copy()
        temp_df["dt"] = pd.to_datetime(temp_df["timestamp"], errors="coerce")
        temp_df = temp_df.dropna(subset=["dt"])

        if temp_df.empty:
            return pd.DataFrame(columns=["timestamp", "count"])

        resampled = temp_df.set_index("dt").resample(freq).size().reset_index()
        resampled.columns = ["timestamp", "count"]
        return resampled

    def get_device_distribution(self) -> pd.DataFrame:
        """Events by device type."""
        if self.df.empty or "device_type" not in self.df.columns:
            return pd.DataFrame(columns=["device_type", "count"])

        res = self.df["device_type"].value_counts().reset_index()
        res.columns = ["device_type", "count"]
        return res

    def get_country_distribution(self) -> pd.DataFrame:
        """Events by country."""
        if self.df.empty or "country" not in self.df.columns:
            return pd.DataFrame(columns=["country", "count"])

        res = self.df["country"].value_counts().reset_index()
        res.columns = ["country", "count"]
        return res

    def get_top_products(self, limit: int = 10, event_type: str = "product_view") -> pd.DataFrame:
        """Top products filtered by event type (e.g. view, add_to_cart, purchase)."""
        if self.df.empty or "product_id" not in self.df.columns:
            return pd.DataFrame(columns=["product_id", "count"])

        filtered = self.df[self.df["event_type"] == event_type] if "event_type" in self.df.columns else self.df
        filtered = filtered.dropna(subset=["product_id"])
        filtered = filtered[filtered["product_id"] != "None"]

        if filtered.empty:
            return pd.DataFrame(columns=["product_id", "count"])

        res = filtered["product_id"].value_counts().head(limit).reset_index()
        res.columns = ["product_id", "count"]
        return res
