#!/usr/bin/env python3
import csv
import re
from pathlib import Path
from typing import Dict, List
import logging

INDENTURE_KEYWORDS = [
    "indenture",
    "supplemental",
    "senior-note",
    "senior-notes",
    "debenture",
    "note-supplement",
    "series-",
    "trust-agreement",
    "trustee",
    "pooling",
    "servicing",
    "paying-agent",
]

PRESS_KEYWORDS = [
    "press",
    "release",
    "earnings",
    "dividend",
    "pr-",
    "pr_",
    "q1fy",
    "q2fy",
    "q3fy",
    "q4fy",
    "presentation",
    "slides",
    "investor",
]

logger = logging.getLogger(__name__)


def parse_exhibit_number(doc_type: str) -> tuple:
    match = re.search(r"EX[-\s]?(\d+)\.?(\d+)?", doc_type, re.IGNORECASE)
    if match:
        major = int(match.group(1))
        minor = int(match.group(2)) if match.group(2) else 0
        return (major, minor)
    return (0, 0)


def has_keyword(text: str, keywords: List[str]) -> bool:
    text_lower = text.lower()
    return any(keyword in text_lower for keyword in keywords)


def classify_exhibit(row: Dict[str, str]) -> Dict[str, str]:
    doc_name = row["doc_name"]
    doc_type = row["doc_type"]
    doc_description = row.get("doc_description", "")
    asset_count = int(row.get("asset_count", 0))
    major_num, minor_num = parse_exhibit_number(doc_type)

    has_indenture_keyword = has_keyword(doc_name, INDENTURE_KEYWORDS) or has_keyword(
        doc_description, INDENTURE_KEYWORDS
    )

    if has_keyword(doc_name, PRESS_KEYWORDS):
        return {
            "category": "press_release",
            "download_action": "skip",
            "confidence": "high",
            "reason": "Press release keyword in filename",
            "priority": 999,
        }

    if asset_count >= 100 and major_num == 4:
        return {
            "category": "image_based_indenture",
            "download_action": "download_with_assets",
            "confidence": "high",
            "reason": f"{asset_count} images + EX-4 type",
            "priority": 1,
        }

    if asset_count >= 100 and has_indenture_keyword:
        return {
            "category": "image_based_indenture",
            "download_action": "download_with_assets",
            "confidence": "high",
            "reason": f"{asset_count} images + indenture keyword",
            "priority": 1,
        }

    if major_num == 4:
        if 1 <= minor_num <= 5:
            return {
                "category": "indenture",
                "download_action": "download",
                "confidence": "high",
                "reason": f"EX-4.{minor_num} exhibit type",
                "priority": 2,
            }

        elif minor_num >= 6 and has_indenture_keyword:
            return {
                "category": "indenture",
                "download_action": "download",
                "confidence": "high",
                "reason": f"EX-4.{minor_num} with indenture keyword",
                "priority": 2,
            }

        else:
            return {
                "category": "indenture",
                "download_action": "download",
                "confidence": "medium",
                "reason": f"EX-4.{minor_num} exhibit type",
                "priority": 3,
            }

    if has_indenture_keyword:
        return {
            "category": "indenture",
            "download_action": "download",
            "confidence": "high",
            "reason": "Indenture keyword in filename or description",
            "priority": 3,
        }

    return {
        "category": "uncertain",
        "download_action": "download",
        "confidence": "low",
        "reason": "No clear signals - downloading to be safe",
        "priority": 5,
    }


def run_pipeline(root_dir: Path, verbose: bool = False):
    asset_dir = root_dir / "asset"
    exhibits_csv = asset_dir / "exhibits.csv"
    output_csv = asset_dir / "exhibits_classified.csv"

    if not exhibits_csv.exists():
        raise FileNotFoundError(f"Phase 1 output not found: {exhibits_csv}")

    if verbose:
        logger.info("Classifying exhibits...")

    classified_rows = []
    stats = {
        "total": 0,
        "indenture": 0,
        "image_based_indenture": 0,
        "press_release": 0,
        "uncertain": 0,
        "to_download": 0,
        "to_skip": 0,
    }

    with open(exhibits_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames

        for row in reader:
            stats["total"] += 1
            classification = classify_exhibit(row)
            classified_row = {**row, **classification}
            classified_rows.append(classified_row)

            stats[classification["category"]] += 1
            if classification["download_action"] == "skip":
                stats["to_skip"] += 1
            else:
                stats["to_download"] += 1

    output_fieldnames = list(fieldnames) + [
        "category",
        "download_action",
        "confidence",
        "reason",
        "priority",
    ]

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=output_fieldnames)
        writer.writeheader()
        writer.writerows(classified_rows)

    if verbose:
        total_indentures = stats["indenture"] + stats["image_based_indenture"]
        logger.info("%d indentures identified", total_indentures)
