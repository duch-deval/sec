#!/usr/bin/env python3
import csv
import re
import logging
from pathlib import Path
from typing import Dict, List, Tuple
from collections import defaultdict

logger = logging.getLogger(__name__)

IMAGE_ASSET_THRESHOLD = 50

STRONG_INDENTURE_KW = ['supplemental indenture', 'base indenture', 'trust indenture', 'indenture agreement',
                       'first supplemental', 'second supplemental', 'third supplemental', 'fourth supplemental',
                       'fifth supplemental', 'sixth supplemental']

INDENTURE_KW = ['indenture', 'senior notes', 'subordinated notes', 'debenture', 'debt securities', 'trustee',
                'paying agent', 'fiscal agent', 'officer certificate', "officer's certificate", 'global note', 'registered note',
                'underwriting agreement']

WARRANT_KW = ['warrant', 'warrants', 'warrant agreement', 'warrant agent']

NOISE_KW = ['press release', 'earnings release', 'earnings call', 'investor presentation', 'investor slides',
            'presentation slides', 'financial results', 'quarterly results', 'annual results', 'conference call',
            'transcript', 'shareholder letter', 'news release']

CONTEXTUAL_KW = ['redemption', 'notice', 'consent', 'supplement', 'amendment', 'exchange', 'tender', 'offering',
                 'maturity', 'interest', 'principal', 'coupon', 'noteholder', 'bondholder', 'holder',
                 'pricing supplement', 'terms agreement', 'fiscal agency', 'agency agreement']


def has_kw(text: str, keywords: List[str]) -> bool:
    t = text.lower()
    return any(k.lower() in t for k in keywords)


def parse_exhibit_number(doc_type: str) -> Tuple[int, int]:
    if m := re.search(r'EX[-\s]?(\d+)\.?(\d+)?', doc_type or "", re.IGNORECASE):
        return int(m.group(1)), int(m.group(2) or 0)
    return 0, 0


def classify_exhibit(row: Dict[str, str], filing_has_ex4: bool) -> Dict[str, str]:
    doc_type, doc_name, doc_desc = row.get('doc_type', ''), row.get('doc_name', ''), row.get('doc_description', '')
    asset_count = int(row.get('asset_count', 0))
    major, minor = parse_exhibit_number(doc_type)
    combined = f"{doc_type} {doc_name} {doc_desc}".lower()
    form = row.get('form', '').strip().upper()
    is_image = asset_count >= IMAGE_ASSET_THRESHOLD

    base = {'is_image_based': 'yes' if is_image else 'no'}

    if has_kw(combined, NOISE_KW):
        return {**base, 'category': 'noise', 'download_action': 'skip', 'confidence': 'high',
                'reason': 'Noise keywords detected', 'priority': 999}

    if major == 4:
        if has_kw(combined, WARRANT_KW):
            return {**base, 'category': 'ex4_warrant', 'download_action': 'holding_queue', 'confidence': 'medium',
                    'reason': 'EX-4 with warrant keywords', 'priority': 10}
        label = f'4.{minor}' if minor else '4'
        return {**base, 'category': 'ex4_indenture', 'download_action': 'download', 'confidence': 'high',
                'reason': f'EX-{label} exhibit', 'priority': 1}

    if major == 99:
        if form == '8-K' and minor == 1:
            return {**base, 'category': 'ex99_8k_excluded', 'download_action': 'skip', 'confidence': 'high',
                    'reason': 'EX-99.1 excluded for 8-K filings per Bloomberg QA', 'priority': 999}
        label = f'99.{minor}' if minor else '99'
        if has_kw(combined, STRONG_INDENTURE_KW):
            return {**base, 'category': 'ex99_indenture', 'download_action': 'download', 'confidence': 'high',
                    'reason': f'Strong indenture keyword in EX-{label}', 'priority': 2}
        if has_kw(combined, INDENTURE_KW):
            return {**base, 'category': 'ex99_indenture', 'download_action': 'download', 'confidence': 'medium',
                    'reason': f'Indenture keyword in EX-{label}', 'priority': 3}
        if has_kw(combined, CONTEXTUAL_KW):
            reason = f'EX-{label} with contextual keywords' + (' (filing has EX-4)' if filing_has_ex4 else '')
            return {**base, 'category': 'ex99_uncertain', 'download_action': 'holding_queue', 'confidence': 'low',
                    'reason': reason, 'priority': 5}
        return {**base, 'category': 'ex99_uncertain', 'download_action': 'holding_queue', 'confidence': 'low',
                'reason': f'EX-{label} no keyword signals (not confirmed noise)', 'priority': 6}

    return {**base, 'category': 'other', 'download_action': 'skip', 'confidence': 'low',
            'reason': f'Unknown exhibit type: {doc_type}', 'priority': 999}


def run_pipeline(root_dir: Path, verbose: bool = False) -> bool:
    asset_dir = root_dir / "asset"
    input_csv, output_csv = asset_dir / "exhibits.csv", asset_dir / "exhibits_classified.csv"

    if not input_csv.exists():
        raise FileNotFoundError(f"Phase 1 output not found: {input_csv}")

    with open(input_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames, rows = reader.fieldnames, list(reader)

    if not rows:
        return True

    filings = defaultdict(list)
    for row in rows:
        filings[(row.get('cik', ''), row.get('accession', ''))].append(row)

    filing_has_ex4 = {k: any(parse_exhibit_number(r.get('doc_type', ''))[0] == 4 for r in v) for k, v in filings.items()}

    classified, stats = [], defaultdict(int)
    for row in rows:
        key = (row.get('cik', ''), row.get('accession', ''))
        c = classify_exhibit(row, filing_has_ex4.get(key, False))
        classified.append({**row, **c})
        stats['total'] += 1
        stats[c['download_action']] += 1
        stats[c['category']] += 1
        if c.get('is_image_based') == 'yes':
            stats['image_based'] += 1

    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=list(fieldnames) + ['category', 'download_action', 'confidence', 'reason', 'priority', 'is_image_based'])
        w.writeheader()
        w.writerows(classified)

    logger.info("Classification: %d total â†’ %d download, %d holding_queue, %d skip",
                stats['total'], stats['download'], stats['holding_queue'], stats['skip'])
    return True