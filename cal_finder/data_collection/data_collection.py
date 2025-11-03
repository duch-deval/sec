#!/usr/bin/env python3
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Tuple

try:
    from phase1 import run_pipeline as run_phase1
except ImportError:
    print("ERROR: phase1.py not found")
    sys.exit(1)
try:
    from phase2 import run_pipeline as run_phase2
except ImportError:
    run_phase2 = None
try:
    from phase3 import run_pipeline as run_phase3
except ImportError:
    run_phase3 = None


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


PIPELINE_PHASES = {
    1: {"name": "Filing & Exhibit Extraction", "enabled": True},
    2: {"name": "Exhibit Classification", "enabled": bool(run_phase2)},
    3: {"name": "Document Download & Organization", "enabled": bool(run_phase3)},
}


def execute_phase1(
    start_date: str, end_date: str, root_dir: Path, config: dict
) -> bool:
    try:
        run_phase1(
            start_date=start_date,
            end_date=end_date,
            root_dir=root_dir,
            mode=config.get("mode", "both"),
            forms=tuple(config.get("forms", ["6-K", "8-K"])),
            query=config.get("query", ""),
            verbose=True,
        )
        return True
    except Exception as e:
        print(f"Phase 1 failed: {e}")
        return False


def execute_phase2(root_dir: Path, config: dict) -> bool:
    if not run_phase2:
        return False
    try:
        run_phase2(root_dir=root_dir, verbose=True)
        return True
    except Exception as e:
        print(f"Phase 2 failed: {e}")
        return False


def execute_phase3(root_dir: Path, config: dict) -> bool:
    if not run_phase3:
        return False
    try:
        return run_phase3(root_dir=root_dir, verbose=True)
    except Exception as e:
        print(f"Phase 3 failed: {e}")
        return False


def run_pipeline(start_date: str, end_date: str, root_dir: Path, config: dict = None):
    if config is None:
        config = {"mode": "both", "forms": ["6-K", "8-K"], "query": ""}

    enabled_phases = [p for p in PIPELINE_PHASES if PIPELINE_PHASES[p]["enabled"]]
    results = {}

    for phase_num in sorted(enabled_phases):
        print(f"\nPhase {phase_num}: {PIPELINE_PHASES[phase_num]['name']}")

        if phase_num == 1:
            success = execute_phase1(start_date, end_date, root_dir, config)
        elif phase_num == 2:
            success = execute_phase2(root_dir, config)
        elif phase_num == 3:
            success = execute_phase3(root_dir, config)
        else:
            success = False

        results[phase_num] = success
        if not success:
            break

    return all(results.values())


def run_data_collection():
    date_input = input(
        "\nDate or range (YYYY-MM-DD or YYYY-MM-DD..YYYY-MM-DD): "
    ).strip()

    if not date_input:
        print("Error: No date provided")
        sys.exit(1)

    try:
        start_date, end_date = parse_date_input(date_input)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    dates = list(generate_dates(start_date, end_date))
    all_success = True

    try:
        for idx, date in enumerate(dates, 1):
            print(f"Date {idx}/{len(dates)}: {date}")
            success = run_pipeline(date, date, Path(date))
            if not success:
                all_success = False
        sys.exit(0 if all_success else 1)
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)
