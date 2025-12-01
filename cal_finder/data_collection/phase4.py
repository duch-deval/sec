#!/usr/bin/env python3
import csv
import re
import shutil
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

REJECT_PATTERNS = [
    (r"COMMON\s+STOCK\s+PURCHASE\s+WARRANT", "warrant", "Common stock purchase warrant"),
    (r"PRE-?FUNDED\s+WARRANT", "warrant", "Pre-funded warrant"),
    (r"WARRANT\s+TO\s+PURCHASE", "warrant", "Warrant to purchase"),
    (r"STRATEGIC\s+ADVISORY\s+WARRANT", "warrant", "Strategic advisory warrant"),
    (r"THIS\s+WARRANT\s+CERTIFIES", "warrant", "Warrant certificate"),
    (r"AMENDMENT\s+TO\s+.*WARRANT", "warrant_amendment", "Warrant amendment"),
    (r"PROMISSORY\s+NOTE", "promissory_note", "Promissory note"),
    (r"PRE-?PAID\s+ADVANCE\s+AGREEMENT", "purchase_agreement", "Pre-paid advance agreement"),
    (r"PREPAID\s+PURCHASE", "purchase_agreement", "Prepaid purchase agreement"),
    (r"DISTRIBUTION\s+REINVESTMENT\s+PLAN", "reinvestment_plan", "Distribution reinvestment plan"),
    (r"SHARE\s+REDEMPTION\s+PROGRAM", "redemption_program", "Share redemption program"),
    (r"POOLING\s+AND\s+SERVICING\s+AGREEMENT", "cmbs_psa", "CMBS pooling & servicing agreement"),
    (r"DEPOSIT\s+AGREEMENT", "deposit_agreement", "Deposit agreement"),
    (r"HAS\s+NOT\s+ENTERED\s+INTO\s+AN\s+INDENTURE", "no_indenture_note", "Note without indenture"),
    (r"SHAREHOLDER\s+RIGHTS\s+AGREEMENT", "rights_agreement", "Shareholder rights agreement"),
    (r"CERTIFICATE\s+OF\s+DESIGNATION.*PREFERRED", "preferred_stock", "Preferred stock certificate"),
    (r"CERTIFICATE\s+OF\s+DESIGNATIONS.*PREFERRED", "preferred_stock", "Preferred stock certificate"),
]

ACCEPT_PATTERNS = [
    (r"as\s+Trustee", 15, "Trustee designation"),
    (r"as\s+Indenture\s+Trustee", 15, "Indenture Trustee designation"),
    (r"Trustee\s+and\s+Collateral\s+Agent", 15, "Trustee and collateral agent"),
    (r"(?:BANK|TRUST).{0,40}Trustee", 15, "Bank/Trust as Trustee"),
    (r',\s*"?\s*Trustee\s*"?', 10, "Trustee reference"),
    (r'the\s+["\']?\s*Trustee\s*["\']?', 10, "The Trustee reference"),
    (r'the\s+Indenture\s+Trustee', 10, "The Indenture Trustee reference"),
    (r"as\s+trustee\s+under", 10, "Trustee under indenture"),
    (r"SUPPLEMENTAL\s+INDENTURE", 12, "Supplemental indenture"),
    (r"BASE\s+INDENTURE", 12, "Base indenture"),
    (r"TRUST\s+INDENTURE", 10, "Trust indenture"),
    (r"INDENTURE\s+dated", 8, "Indenture with date"),
    (r"Senior\s+Notes?\s+due\s+20\d{2}", 6, "Senior notes with maturity"),
    (r"Subordinated\s+Notes?\s+due\s+20\d{2}", 6, "Subordinated notes with maturity"),
    (r"Secured\s+Notes?\s+due\s+20\d{2}", 6, "Secured notes with maturity"),
    (r"aggregate\s+principal\s+amount", 4, "Aggregate principal amount"),
    (r"Events\s+of\s+Default", 4, "Events of default section"),
    (r"Paying\s+Agent", 3, "Paying agent reference"),
]

ACCEPT_THRESHOLD = 20
TRUSTEE_REQUIRED = True


def extract_text_preview(file_path: Path, max_chars: int = 8000) -> str:
    try:
        content = file_path.read_bytes()
        text = content.decode("utf-8", errors="replace")
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"\s+", " ", text)
        return text[:max_chars]
    except Exception as e:
        logger.error("Failed to read %s: %s", file_path, e)
        return ""


def check_reject_patterns(text: str) -> Optional[Tuple[str, str]]:
    text_upper = text.upper()
    for pattern, category, description in REJECT_PATTERNS:
        if re.search(pattern, text_upper):
            return (category, description)
    return None


def calculate_accept_score(text: str) -> Tuple[int, bool, List[str]]:
    score = 0
    trustee_found = False
    matches = []

    for pattern, weight, description in ACCEPT_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            score += weight
            matches.append(f"{description} (+{weight})")
            if "Trustee" in description:
                trustee_found = True

    return score, trustee_found, matches


def classify_document(file_path: Path) -> Dict:
    file_name = file_path.name
    file_size = file_path.stat().st_size
    text = extract_text_preview(file_path)

    if not text:
        return {
            "file_name": file_name,
            "file_size": file_size,
            "classification": "rejected",
            "confidence": "high",
            "reject_category": "unreadable",
            "reject_pattern": "Could not extract text",
            "accept_score": 0,
            "trustee_found": False,
            "decision_reason": "File unreadable or empty",
            "accept_matches": [],
        }

    reject_match = check_reject_patterns(text)
    if reject_match:
        category, description = reject_match
        return {
            "file_name": file_name,
            "file_size": file_size,
            "classification": "rejected",
            "confidence": "high",
            "reject_category": category,
            "reject_pattern": description,
            "accept_score": 0,
            "trustee_found": False,
            "decision_reason": f"Hard reject: {description}",
            "accept_matches": [],
        }

    score, trustee_found, matches = calculate_accept_score(text)

    if not trustee_found and TRUSTEE_REQUIRED:
        return {
            "file_name": file_name,
            "file_size": file_size,
            "classification": "rejected",
            "confidence": "medium",
            "reject_category": "no_trustee",
            "reject_pattern": "",
            "accept_score": score,
            "trustee_found": False,
            "decision_reason": "No trustee reference found",
            "accept_matches": matches,
        }

    if score < ACCEPT_THRESHOLD:
        return {
            "file_name": file_name,
            "file_size": file_size,
            "classification": "rejected",
            "confidence": "low",
            "reject_category": "low_score",
            "reject_pattern": "",
            "accept_score": score,
            "trustee_found": trustee_found,
            "decision_reason": f"Score {score} below threshold {ACCEPT_THRESHOLD}",
            "accept_matches": matches,
        }

    return {
        "file_name": file_name,
        "file_size": file_size,
        "classification": "indenture",
        "confidence": "high" if score >= 30 else "medium",
        "reject_category": "",
        "reject_pattern": "",
        "accept_score": score,
        "trustee_found": trustee_found,
        "decision_reason": f"Accepted: score={score}, trustee={trustee_found}",
        "accept_matches": matches,
    }


def run_pipeline(root_dir: Path, verbose: bool = False) -> bool:
    root_dir = Path(root_dir)
    indentures_dir = root_dir / "indentures"
    rejected_dir = root_dir / "rejected"
    asset_dir = root_dir / "asset"
    results_csv = asset_dir / "phase4_results.csv"

    if not indentures_dir.exists():
        logger.info("No indentures/ folder found, skipping Phase 4")
        return True

    files = list(indentures_dir.glob("*.htm")) + list(indentures_dir.glob("*.html"))

    if not files:
        logger.info("No HTML files in indentures/, skipping Phase 4")
        return True

    logger.info("Phase 4: Filtering %d files...", len(files))

    rejected_dir.mkdir(parents=True, exist_ok=True)
    asset_dir.mkdir(parents=True, exist_ok=True)

    results = []
    stats = {"indenture": 0, "rejected": 0}

    for file_path in files:
        result = classify_document(file_path)
        results.append(result)

        if result["classification"] == "rejected":
            dest = rejected_dir / file_path.name
            shutil.move(str(file_path), str(dest))
            stats["rejected"] += 1
            if verbose:
                logger.info("  REJECTED: %s (%s)", file_path.name, result["reject_category"])
        else:
            stats["indenture"] += 1
            if verbose:
                logger.info("  ACCEPTED: %s (score=%d)", file_path.name, result["accept_score"])

    fieldnames = [
        "file_name", "file_size", "classification", "confidence",
        "reject_category", "reject_pattern", "accept_score",
        "trustee_found", "decision_reason", "accept_matches"
    ]

    with open(results_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            row = {k: r[k] for k in fieldnames if k != "accept_matches"}
            row["accept_matches"] = "; ".join(r["accept_matches"]) if r["accept_matches"] else ""
            writer.writerow(row)

    logger.info(
        "Phase 4 complete: %d accepted, %d rejected",
        stats["indenture"], stats["rejected"]
    )

    return True