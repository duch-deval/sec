#!/usr/bin/env python3
import argparse
import csv
import os
import re
import sys
import time
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse

import requests
import openpyxl

logger = logging.getLogger(__name__)

USER_AGENT = os.getenv("SEC_USER_AGENT", "Bloomberg-Drexel-Capstone (dhd37@drexel.edu)")
REQUEST_TIMEOUT = 30
RETRY_MAX = 3
RETRY_BASE_DELAY = 1.0
REQUEST_DELAY = 0.15

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,*/*",
})

EXPORT_COLUMNS = [
    "Company ", "File Date", "File Type", "File Link ", "Exhibit",
    "Security Description", "CUSIP", "ISIN", "Text",
    "Business Days - Standardized", "Mapping",
]


def fetch_with_retry(url: str) -> Optional[bytes]:
    for attempt in range(RETRY_MAX):
        try:
            time.sleep(REQUEST_DELAY)
            resp = SESSION.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.content
        except requests.exceptions.RequestException as e:
            if attempt < RETRY_MAX - 1:
                time.sleep(RETRY_BASE_DELAY * (2 ** attempt))
            else:
                logger.error("Failed %s: %s", url, e)
    return None


def parse_xlsx(xlsx_path: Path) -> List[Dict[str, str]]:
    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = wb.active
    all_rows = list(ws.iter_rows(values_only=True))
    wb.close()

    if not all_rows:
        return []

    records = []
    seen_urls = set()
    headers = None

    for row in all_rows:
        cells = list(row)

        if cells and str(cells[0]).strip() in ("Company ", "Company"):
            headers = [str(c).strip() if c else "" for c in cells]
            continue

        if headers is None or not cells[0]:
            continue

        raw = {}
        for i, header in enumerate(headers):
            if i < len(cells):
                raw[header] = cells[i]

        url = raw.get("File Link ") or raw.get("File Link")
        if not url or not str(url).startswith("http"):
            continue
        url = str(url).strip()

        if url in seen_urls:
            continue
        seen_urls.add(url)

        file_date = raw.get("File Date", "")
        if isinstance(file_date, datetime):
            file_date = file_date.strftime("%Y-%m-%d")
        else:
            file_date = str(file_date).strip()

        exhibit = raw.get("Exhibit", "")
        if isinstance(exhibit, (int, float)):
            exhibit = str(exhibit)
        else:
            exhibit = str(exhibit).strip() if exhibit else ""

        records.append({
            "company": str(raw.get("Company ") or raw.get("Company") or "").strip(),
            "file_date": file_date,
            "file_type": str(raw.get("File Type", "")).strip(),
            "file_link": url,
            "exhibit": exhibit,
        })

    return records


def get_exhibit_folder(exhibit: str) -> str:
    exhibit = str(exhibit).strip().lower()
    if exhibit.startswith("99") or exhibit.startswith("ex-99") or exhibit.startswith("ex99"):
        return "ex99"
    return "ex4"


def download_exhibit(url: str, exhibits_dir: Path, exhibit: str) -> Optional[Path]:
    subfolder = exhibits_dir / get_exhibit_folder(exhibit)
    subfolder.mkdir(parents=True, exist_ok=True)

    url_path = urlparse(url).path
    filename = url_path.split("/")[-1] or "exhibit.htm"
    filename = re.sub(r"[^\w\-.]", "_", filename)
    target = subfolder / filename

    if target.exists():
        base, ext = (filename.rsplit(".", 1) + [""])[:2]
        counter = 1
        while target.exists():
            target = subfolder / (f"{base}_{counter}.{ext}" if ext else f"{base}_{counter}")
            counter += 1

    content = fetch_with_retry(url)
    if not content:
        return None

    target.write_bytes(content)
    return target


def run(input_path: Path, output_dir: Path):
    records = parse_xlsx(input_path)

    if not records:
        raise ValueError(f"No exhibit URLs found in {input_path}")

    logger.info("Downloading %d exhibits", len(records))

    exhibits_dir = output_dir / "exhibits"
    exhibits_dir.mkdir(parents=True, exist_ok=True)

    csv_rows = []
    stats = {"downloaded": 0, "failed": 0}

    for i, rec in enumerate(records, 1):
        local_path = download_exhibit(rec["file_link"], exhibits_dir, rec["exhibit"])

        if local_path:
            stats["downloaded"] += 1
        else:
            stats["failed"] += 1
            local_path = ""

        csv_rows.append({
            "Company ": rec["company"],
            "File Date": rec["file_date"],
            "File Type": rec["file_type"],
            "File Link ": rec["file_link"],
            "Exhibit": rec["exhibit"],
            "Security Description": "",
            "CUSIP": "",
            "ISIN": "",
            "Text": "",
            "Business Days - Standardized": "",
            "Mapping": "",
            "_local_path": str(local_path.resolve()) if local_path else "",
        })

    output_csv = output_dir / f"{output_dir.name}.csv"
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=EXPORT_COLUMNS + ["_local_path"])
        writer.writeheader()
        writer.writerows(csv_rows)

    logger.info("Download: %d ok, %d failed -> %s", stats["downloaded"], stats["failed"], output_csv)
    SESSION.close()


def main():
    parser = argparse.ArgumentParser(description="Download SEC exhibits from Bloomberg XLSX")
    parser.add_argument("xlsx", type=Path, help="Path to Bloomberg sample XLSX")
    parser.add_argument("--output-dir", "-o", type=Path, default=None)
    args = parser.parse_args()

    if not args.xlsx.exists():
        sys.exit(f"File not found: {args.xlsx}")

    if args.output_dir is None:
        args.output_dir = Path(f"samples_{args.xlsx.stem}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    run(args.xlsx, args.output_dir)


if __name__ == "__main__":
    main()