import argparse
import shutil
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import SAMPLE_DATA_DIR
from app.processing.event_generator import ClickstreamEventGenerator
from app.processing.processor import EventProcessor
from app.storage.local_storage import LocalStorageWriter

def clean_sample_directory(dir_path: Path):
    """Clean all existing sample files and folders inside sample directory."""
    if dir_path.exists():
        for item in dir_path.iterdir():
            if item.name == ".gitkeep":
                continue
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()
    dir_path.mkdir(parents=True, exist_ok=True)

def main():
    parser = argparse.ArgumentParser(description="Generate sample clickstream data for dashboard demo")
    parser.add_argument("--count", type=int, default=10000, help="Number of sample events (default: 10000)")
    parser.add_argument("--append", action="store_true", default=False, help="Append sample data to existing files without overwriting")
    args = parser.parse_args()

    if not args.append:
        print(f"[SAMPLE] Cleaning existing sample data in '{SAMPLE_DATA_DIR}'...")
        clean_sample_directory(SAMPLE_DATA_DIR)
    else:
        print(f"[SAMPLE] Appending new sample data to existing files in '{SAMPLE_DATA_DIR}'...")

    print(f"[SAMPLE] Generating {args.count} realistic clickstream events...")
    generator = ClickstreamEventGenerator(seed=42 if not args.append else None)
    processor = EventProcessor()
    writer = LocalStorageWriter(base_processed_dir=SAMPLE_DATA_DIR)

    raw_events = generator.generate_batch(args.count)

    processed_events = []
    for evt in raw_events:
        success, proc_evt, err = processor.process_event(evt)
        if success:
            processed_events.append(proc_evt)

    df = processor.events_to_dataframe(processed_events)
    written_paths = writer.write_batch(df)

    print(f"[SAMPLE] Successfully generated {len(processed_events)} valid sample events.")
    print(f"[SAMPLE] Stored in {len(written_paths)} Parquet file(s) under '{SAMPLE_DATA_DIR}'.")
    for p in written_paths:
        print(f"  - {p}")

if __name__ == "__main__":
    main()
