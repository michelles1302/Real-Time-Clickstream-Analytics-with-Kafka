import json
import time
import uuid
from pathlib import Path
from typing import List, Dict, Any, Optional
import pandas as pd
from app.config import PROCESSED_DATA_DIR, REJECTED_DATA_DIR

class LocalStorageWriter:
    def __init__(self, base_processed_dir: Path = PROCESSED_DATA_DIR, base_rejected_dir: Path = REJECTED_DATA_DIR):
        self.base_processed_dir = base_processed_dir
        self.base_rejected_dir = base_rejected_dir
        self.base_processed_dir.mkdir(parents=True, exist_ok=True)
        self.base_rejected_dir.mkdir(parents=True, exist_ok=True)

    def write_batch(self, df: pd.DataFrame) -> List[Path]:
        """
        Write a batch DataFrame to local Parquet files partitioned by year/month/day.
        Returns a list of created Parquet file paths.
        """
        if df.empty:
            return []

        written_paths = []
        now_ts = int(time.time() * 1000)

        # Check if partitioning columns exist
        if not all(col in df.columns for col in ["year", "month", "day"]):
            # Default to current date if missing
            now_dt = pd.Timestamp.now(tz="UTC")
            df["year"] = now_dt.strftime("%Y")
            df["month"] = now_dt.strftime("%m")
            df["day"] = now_dt.strftime("%d")

        # Group by date partitions
        grouped = df.groupby(["year", "month", "day"])
        for (yr, mn, dy), group in grouped:
            partition_dir = self.base_processed_dir / f"year={yr}" / f"month={mn}" / f"day={dy}"
            partition_dir.mkdir(parents=True, exist_ok=True)

            filename = f"events_{now_ts}_{uuid.uuid4().hex[:6]}.parquet"
            file_path = partition_dir / filename

            # Drop partition columns from actual parquet schema for clean columnar storage
            save_df = group.drop(columns=["year", "month", "day"], errors="ignore")
            save_df.to_parquet(file_path, engine="pyarrow", index=False)
            written_paths.append(file_path)

        return written_paths

    def write_rejected(self, rejected_items: List[Dict[str, Any]]) -> Optional[Path]:
        """
        Write malformed or invalid events to the rejected directory in JSON format.
        """
        if not rejected_items:
            return None

        now_ts = int(time.time() * 1000)
        filename = f"rejected_{now_ts}_{uuid.uuid4().hex[:6]}.json"
        file_path = self.base_rejected_dir / filename

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(rejected_items, f, indent=2)

        return file_path
