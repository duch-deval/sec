#!/usr/bin/env python3
import logging
import shutil
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Tuple

logger = logging.getLogger(__name__)

try:
    from .sec_discovery import run_pipeline as run_sec_discovery
except ImportError:
    sys.exit("sec_discovery.py not found")

try:
    from .classification import run_pipeline as run_classification
except ImportError:
    run_classification = None

try:
    from .download import run_pipeline as run_download
except ImportError:
    run_download = None

try:
    from .annotation import run_pipeline as run_annotation
except ImportError:
    run_annotation = None


def parse_date_input(date_input: str) -> Tuple[str, str]:
    date_input = date_input.strip()
    if ".." in date_input:
        parts = date_input.split("..")
        if len(parts) != 2:
            raise ValueError("Invalid format. Use: YYYY-MM-DD..YYYY-MM-DD")
        return parts[0].strip(), parts[1].strip()
    return date_input, date_input


def generate_dates(start_date: str, end_date: str):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    current = start
    while current <= end:
        yield current.strftime("%Y-%m-%d")
        current += timedelta(days=1)


def cleanup(root_dir: Path, date: str):
    asset_dir = root_dir / "asset"

    candidates = asset_dir / "candidates_for_extraction.csv"
    if candidates.exists():
        candidates.rename(root_dir / f"{date}_raw.csv")

    for f in ["filings.csv", "exhibits.csv", "exhibits_classified.csv", "phase4_results.csv"]:
        (asset_dir / f).unlink(missing_ok=True)

    shutil.rmtree(root_dir / "rejected", ignore_errors=True)
    shutil.rmtree(asset_dir, ignore_errors=True)

    hq = root_dir / "exhibits" / "holding_queue"
    if hq.exists() and not any(hq.iterdir()):
        hq.rmdir()


def run_pipeline(start_date: str, end_date: str, root_dir: Path, config: dict = None):
    if config is None:
        config = {"forms": ["6-K", "8-K"]}

    try:
        run_sec_discovery(
            start_date=start_date,
            end_date=end_date,
            root_dir=root_dir,
            forms=tuple(config.get("forms", ["6-K", "8-K"]))
        )
    except Exception as e:
        logger.error("SEC Discovery failed: %s", e)
        return False

    if run_classification:
        try:
            run_classification(root_dir=root_dir, verbose=False)
        except Exception as e:
            logger.error("Classification failed: %s", e)
            return False

    if run_download:
        try:
            if not run_download(root_dir=root_dir, verbose=False):
                return False
        except Exception as e:
            logger.error("Download failed: %s", e)
            return False

    if run_annotation:
        try:
            if not run_annotation(root_dir=root_dir, verbose=False):
                return False
        except Exception as e:
            logger.error("Annotation failed: %s", e)
            return False

    return True


def run_data_collection():
    if len(sys.argv) < 2:
        sys.exit("Usage: python -m corpus_builder <YYYY-MM-DD or YYYY-MM-DD..YYYY-MM-DD>")

    date_input = sys.argv[1].strip()

    try:
        start_date, end_date = parse_date_input(date_input)
    except ValueError as e:
        sys.exit(str(e))

    dates = list(generate_dates(start_date, end_date))
    all_success = True

    try:
        for idx, date in enumerate(dates, 1):
            print(f"\n[{idx}/{len(dates)}] {date}")
            root_dir = Path(date)
            if run_pipeline(date, date, root_dir):
                cleanup(root_dir, date)
            else:
                all_success = False

        print("\nDone." if all_success else "\nCompleted with errors.")
        sys.exit(0 if all_success else 1)

    except KeyboardInterrupt:
        sys.exit("\nInterrupted")
    except Exception as e:
        logger.exception("Error: %s", e)
        sys.exit(1)