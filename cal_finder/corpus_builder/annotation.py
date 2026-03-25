#!/usr/bin/env python3
import csv
import html as html_mod
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
    (r"WARRANT\s+AGREEMENT", "warrant", "Warrant agreement"),
    (r"ANNOUNCES?\s+(?:CASH\s+)?TENDER\s+OFFER", "press_release", "Tender offer press release"),
    (r"CASH\s+TENDER\s+OFFER", "press_release", "Cash tender offer"),
    (r"STRATEGIC\s+ADVISORY\s+WARRANT", "warrant", "Strategic advisory warrant"),
    (r"THIS\s+WARRANT\s+CERTIFIES", "warrant", "Warrant certificate"),
    (r"AMENDMENT\s+TO\s+.*WARRANT", "warrant_amendment", "Warrant amendment"),
    (r"PROMISSORY\s+NOTE(?!\s+INDENTURE)", "promissory_note", "Promissory note"),
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
    (r"VOLUNTARY\s+ANNOUNCEMENT", "stock_exchange_announcement", "Voluntary announcement"),
    (r"STOCK\s+EXCHANGE\s+OF\s+HONG\s+KONG", "stock_exchange_announcement", "HK stock exchange announcement"),
    (r"HONG\s+KONG\s+EXCHANGES\s+AND\s+CLEARING", "stock_exchange_announcement", "HK exchanges announcement"),
    (r"PURCHASE\s+OF\s+SHARES\s+ON\s+MARKET", "share_purchase_announcement", "Share purchase announcement"),
    (r"TRUST\s+AND\s+SERVICING\s+AGREEMENT", "cmbs_tsa", "CMBS trust & servicing agreement"),
    (r"AUTO\s+RECEIVABLES?\b.{0,30}TRUST", "asset_backed", "Auto receivables trust"),
    (r"AUTO\s+(?:LOAN|LEASE)\s+(?:RECEIVABLES?|TRUST)", "asset_backed", "Auto loan/lease trust"),
    (r"ASSET[- ]BACKED\s+(?:NOTES?|SECURITIES|CERTIFICATES)", "asset_backed", "Asset-backed securities"),
    (r"RECEIVABLES?\s+(?:LLC|TRUST|CORP).{0,30}(?:AS\s+)?ISSU(?:ER|ING)", "asset_backed", "Receivables issuing entity"),
    (r"MORTGAGE[- ]BACKED", "asset_backed", "Mortgage-backed securities"),
    (r"COLLATERALIZED\s+LOAN\s+OBLIGATION", "asset_backed", "CLO"),
    (r"MEDIUM[- ]TERM\s+NOTE", "medium_term_note", "Medium-term note master form"),
    (r"AMENDMENT\s+TO\s+INVESTMENT\s+AGREEMENT", "investment_agreement", "Investment agreement amendment"),
    (r"REGISTRATION\s+RIGHTS\s+AGREEMENT", "rights_agreement", "Registration rights agreement"),
    (r"APPENDIX\s+2A", "asx_disclosure", "ASX securities quotation notice — share count, not bond"),
    (r"APPENDIX\s+3[YZ]", "asx_disclosure", "ASX director interest/substantial holder notice"),
    (r"ORDINARY\s+FULLY\s+PAID", "asx_equity", "ASX ordinary equity shares — not a bond"),
    (r"SECURITIES\s+TO\s+BE\s+QUOTED", "asx_equity", "ASX quotation notice — equity, not bond"),
    (r"CASH\s+AND\s+INVESTMENTS\s+HELD\s+IN\s+TRUST\s+ACCOUNT", "spac_trust", "SPAC trust account notice — not a bond indenture"),
    (r"BLANK\s+CHECK\s+COMPANY", "spac_trust", "SPAC blank check company — not a bond"),
    (r"(?:162ND|163RD|164TH|165TH|\d{2,3}(?:ST|ND|RD|TH))\s+SUPPLEMENTAL\s+INDENTURE", "sce_mortgage", "SCE numbered mortgage supplemental — property lien collateral, not bond terms"),
    (r"CHANGE\s+OF\s+DIRECTOR", "asx_disclosure", "ASX change of director notice"),
    (r"THIS\s+(?:NOTE|CERTIFICATE|SECURITY)\s+IS\s+A\s+GLOBAL\s+SECURITY", "global_security_form", "Global security certificate form"),
]

# Patterns that should only match in the document title area (first 500 chars)
TITLE_REJECT_PATTERNS = [
    (r"OFFICERS?['.\u2019]?\s*CERTIFICATE", "officers_cert", "Officers certificate"),
    (r"COMPANY\s+ORDER", "company_order", "Company order document"),
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
    (r"GLOBAL\s+SECURITY", 8, "Global Security designation"),
    (r"INDENTURE\s+HEREINAFTER\s+REFERRED\s+TO", 8, "Indenture reference in note"),
    (r"REGISTERED\s+IN\s+THE\s+NAME\s+OF", 4, "Registered holder"),
    (r"CUSIP\s+(?:No\.?|Number)", 4, "CUSIP identifier present"),
]

ACCEPT_THRESHOLD, TRUSTEE_REQUIRED = 20, True

EXPORT_COLUMNS = ["Company ", "File Date", "File Type", "File Link ", "Description of Exhibit",
                  "Exhibit", "Security Description", "CUSIP", "ISIN", "Text", "Business Days - Standardized", "Mapping"]


def extract_text_preview(file_path: Path, max_chars: int = 8000) -> str:
    try:
        text = file_path.read_bytes().decode("utf-8", errors="replace")
        text = html_mod.unescape(text)
        return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", text))[:max_chars]
    except Exception:
        return ""


def check_reject_patterns(text: str) -> Optional[Tuple[str, str]]:
    text_upper = text.upper()
    for pattern, category, description in REJECT_PATTERNS:
        if re.search(pattern, text_upper):
            return (category, description)
    # Title-area patterns: only check the first 500 chars
    title_area = text_upper[:500]
    for pattern, category, description in TITLE_REJECT_PATTERNS:
        if re.search(pattern, title_area):
            return (category, description)
    return None


def check_abs_fingerprint(text: str) -> Optional[Tuple[str, str]]:
    """Detect asset-backed securities by structural co-occurrence rather than
    individual title patterns. Catches ABS variants (auto lease trusts, credit
    card trusts, student loan ABS, etc.) that slip through enumerated
    REJECT_PATTERNS. Requires 2+ co-occurring signals to avoid false positives."""
    title_area = text[:3000].upper()

    signals = []
    if re.search(r'\bDEPOSITOR\b', title_area):
        signals.append('Depositor')
    if re.search(r'\bSERVICER\b', title_area):
        signals.append('Servicer')
    if re.search(r'\bSECURITIZ', title_area):
        signals.append('Securitization')
    if re.search(r'\bASSET[- ]BACKED\b', title_area):
        signals.append('Asset-Backed')
    if re.search(r'\b(?:RECEIVABLES?|LEASING)\s+(?:LLC|TRUST|CORP)', title_area):
        signals.append('Receivables/Leasing entity')
    if re.search(r'\bISSUER\s+TRUST\b', title_area):
        signals.append('Issuer Trust')

    if len(signals) >= 2:
        return ("asset_backed_structural",
                f"Structural ABS fingerprint: {', '.join(signals)}")
    return None


def calculate_accept_score(text: str) -> Tuple[int, bool, List[str]]:
    score, trustee_found, matches = 0, False, []
    for pattern, weight, description in ACCEPT_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            score += weight
            matches.append(f"{description} (+{weight})")
            if "Trustee" in description:
                trustee_found = True
    return score, trustee_found, matches


def classify_document(file_path: Path) -> Dict:
    file_name, file_size = file_path.name, file_path.stat().st_size
    text = extract_text_preview(file_path)
    base = {"file_name": file_name, "file_path": str(file_path), "file_size": file_size}

    if not text:
        return {**base, "classification": "rejected", "confidence": "high", "reject_category": "unreadable",
                "reject_reason": "Could not extract text", "accept_score": 0, "trustee_found": False, "accept_matches": []}

    if reject_match := check_reject_patterns(text):
        return {**base, "classification": "rejected", "confidence": "high", "reject_category": reject_match[0],
                "reject_reason": f"Hard reject: {reject_match[1]}", "accept_score": 0, "trustee_found": False, "accept_matches": []}

    if abs_match := check_abs_fingerprint(text):
        return {**base, "classification": "rejected", "confidence": "high", "reject_category": abs_match[0],
                "reject_reason": f"Hard reject: {abs_match[1]}", "accept_score": 0, "trustee_found": False, "accept_matches": []}

    score, trustee_found, matches = calculate_accept_score(text)

    if not trustee_found and TRUSTEE_REQUIRED:
        return {**base, "classification": "rejected", "confidence": "medium", "reject_category": "no_trustee",
                "reject_reason": "No trustee reference found", "accept_score": score, "trustee_found": False, "accept_matches": matches}

    if score < ACCEPT_THRESHOLD:
        return {**base, "classification": "rejected", "confidence": "low", "reject_category": "low_score",
                "reject_reason": f"Score {score} below threshold {ACCEPT_THRESHOLD}", "accept_score": score,
                "trustee_found": trustee_found, "accept_matches": matches}

    return {**base, "classification": "accepted", "confidence": "high" if score >= 30 else "medium",
            "reject_category": "", "reject_reason": "", "accept_score": score, "trustee_found": trustee_found, "accept_matches": matches}


def load_classification_metadata(csv_path: Path) -> Dict[str, Dict]:
    if not csv_path.exists():
        return {}
    with open(csv_path, "r", encoding="utf-8") as f:
        return {row.get("doc_name", ""): row for row in csv.DictReader(f) if row.get("doc_name")}


def _resolve_metadata(filename: str, metadata: Dict[str, Dict]) -> Dict:
    if meta := metadata.get(filename):
        return meta
    m = re.match(r'^(.+?)_(\d+)(\.(?:htm|html))$', filename, re.IGNORECASE)
    if m:
        original = m.group(1) + m.group(3)
        if meta := metadata.get(original):
            return meta
    return {}


def clean_company_name(company: str) -> str:
    return company.split("(")[0].strip() if company and "(" in company else (company or "").strip()


def extract_exhibit_number(doc_type: str) -> str:
    if m := re.search(r'EX[-\s]?(\d+(?:\.\d+)?)', doc_type or "", re.IGNORECASE):
        return m.group(1)
    return (doc_type or "").strip()


def run_pipeline(root_dir: Path, verbose: bool = False) -> bool:
    root_dir = Path(root_dir)
    asset_dir, rejected_dir = root_dir / "asset", root_dir / "rejected"
    candidates_csv, results_csv = asset_dir / "candidates_for_extraction.csv", asset_dir / "phase4_results.csv"

    metadata = load_classification_metadata(asset_dir / "exhibits_classified.csv")

    all_files = []
    for folder in [root_dir / "exhibits" / "ex4", root_dir / "exhibits" / "ex99"]:
        if folder.exists():
            all_files.extend(folder.glob("*.htm"))
            all_files.extend(folder.glob("*.html"))

    if not all_files:
        logger.info("No HTML files found, skipping Annotation")
        return True

    logger.info("Annotation: Filtering %d files...", len(all_files))
    rejected_dir.mkdir(parents=True, exist_ok=True)
    asset_dir.mkdir(parents=True, exist_ok=True)

    results, candidates, stats = [], [], {"accepted": 0, "rejected": 0}

    for file_path in all_files:
        result = classify_document(file_path)
        results.append(result)

        if result["classification"] == "rejected":
            try:
                shutil.move(str(file_path), str(rejected_dir / file_path.name))
            except Exception:
                pass
            stats["rejected"] += 1
        else:
            meta = _resolve_metadata(file_path.name, metadata)
            candidates.append({
                "Company ": clean_company_name(meta.get("company", "")),
                "File Date": meta.get("filed", ""), "File Type": meta.get("form", ""),
                "File Link ": meta.get("doc_url", ""), "Description of Exhibit": "",
                "Exhibit": extract_exhibit_number(meta.get("doc_type", "")),
                "Security Description": "", "CUSIP": "", "ISIN": "", "Text": "",
                "Business Days - Standardized": "", "Mapping": "",
                "_local_path": str(file_path), "_category": meta.get("category", ""), "_accept_score": result["accept_score"],
            })
            stats["accepted"] += 1

    with open(candidates_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=EXPORT_COLUMNS + ["_local_path", "_category", "_accept_score"])
        w.writeheader()
        w.writerows(candidates)

    with open(results_csv, "w", newline="", encoding="utf-8") as f:
        fields = ["file_name", "file_size", "classification", "confidence", "reject_category", "reject_reason",
                  "accept_score", "trustee_found", "accept_matches"]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in results:
            w.writerow({k: ("; ".join(r[k]) if k == "accept_matches" else r.get(k, "")) for k in fields})

    logger.info("Annotation: %d accepted, %d rejected -> %s", stats["accepted"], stats["rejected"], candidates_csv.name)
    return True