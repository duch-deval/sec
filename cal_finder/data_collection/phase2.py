#!/usr/bin/env python3
import csv
import re
from pathlib import Path
from typing import Dict, List
from collections import defaultdict

INDENTURE_KEYWORDS = [
    'indenture', 'supplemental', 'senior-note', 'senior-notes', 
    'debenture', 'note-supplement', 'series-', 'trust-agreement', 
    'trustee', 'pooling', 'servicing', 'paying-agent'
]

STRONG_INDENTURE_KEYWORDS = [
    'supplemental indenture', 'base indenture', 'trust indenture',
    'senior notes', 'subordinated notes', 'debenture', 'trustee'
]

PRESS_KEYWORDS = [
    'press', 'release', 'earnings', 'dividend',
    'pr-', 'pr_', 'q1fy', 'q2fy', 'q3fy', 'q4fy',
    'presentation', 'slides', 'investor',
    'q1', 'q2', 'q3', 'q4', 'quarter', 'quarterly',
    'financial', 'consolidated', 'certification', 'mda',
    'fs-', 'fs_', 'earnpr', 'cert-', 'cert_',
    'ceocert', 'cfocert', 'officer-cert',
    '3rdquarter', '4thquarter', '1stquarter', '2ndquarter'
]

WARRANT_KEYWORDS = [
    'warrant', 'warrants', 'stock purchase', 'pre-funded'
]

EX99_CONTEXTUAL_KEYWORDS = [
    'supplement', 'amendment', 'notice', 'redemption', 'offering', 'consent'
]

def parse_exhibit_number(doc_type: str) -> tuple:
    match = re.search(r'EX[-\s]?(\d+)\.?(\d+)?', doc_type, re.IGNORECASE)
    if match:
        major = int(match.group(1))
        minor = int(match.group(2)) if match.group(2) else 0
        return (major, minor)
    return (0, 0)


def has_keyword(text: str, keywords: List[str]) -> bool:
    text_lower = text.lower()
    return any(keyword in text_lower for keyword in keywords)


def has_strong_indenture_keyword(text: str) -> bool:
    text_lower = text.lower()
    return any(keyword in text_lower for keyword in STRONG_INDENTURE_KEYWORDS)


def classify_exhibit(row: Dict[str, str], filing_has_ex4: bool = False) -> Dict[str, str]:
    doc_name = row['doc_name']
    doc_type = row['doc_type']
    doc_description = row.get('doc_description', '')
    asset_count = int(row.get('asset_count', 0))
    major_num, minor_num = parse_exhibit_number(doc_type)

    combined_text = doc_name + ' ' + doc_description
    has_indenture_keyword = has_keyword(combined_text, INDENTURE_KEYWORDS)
    
    if has_keyword(combined_text, WARRANT_KEYWORDS):
        return {
            'category': 'warrant',
            'download_action': 'skip',
            'confidence': 'high',
            'reason': 'Warrant keyword detected',
            'priority': 999
        }
    
    if has_keyword(combined_text, PRESS_KEYWORDS):
        return {
            'category': 'press_release',
            'download_action': 'skip',
            'confidence': 'high',
            'reason': 'Press release keyword in filename/description',
            'priority': 999
        }

    if asset_count >= 100 and major_num == 4:
        return {
            'category': 'image_based_indenture',
            'download_action': 'download_with_assets',
            'confidence': 'high',
            'reason': f'{asset_count} images + EX-4 type',
            'priority': 1
        }
    
    if asset_count >= 100 and has_indenture_keyword:
        return {
            'category': 'image_based_indenture',
            'download_action': 'download_with_assets',
            'confidence': 'high',
            'reason': f'{asset_count} images + indenture keyword',
            'priority': 1
        }
    
    if major_num == 4:
        if 1 <= minor_num <= 5:
            return {
                'category': 'indenture',
                'download_action': 'download',
                'confidence': 'high',
                'reason': f'EX-4.{minor_num} exhibit type',
                'priority': 2
            }
        
        elif minor_num >= 6 and has_indenture_keyword:
            return {
                'category': 'indenture',
                'download_action': 'download',
                'confidence': 'high',
                'reason': f'EX-4.{minor_num} with indenture keyword',
                'priority': 2
            }
        
        else:
            return {
                'category': 'indenture',
                'download_action': 'download',
                'confidence': 'medium',
                'reason': f'EX-4.{minor_num} exhibit type',
                'priority': 3
            }
    
    if major_num == 99:
        if has_strong_indenture_keyword(combined_text):
            return {
                'category': 'indenture',
                'download_action': 'download',
                'confidence': 'high',
                'reason': 'Strong indenture keyword in EX-99',
                'priority': 2
            }
        
        if has_indenture_keyword:
            return {
                'category': 'indenture',
                'download_action': 'download',
                'confidence': 'medium',
                'reason': 'Indenture keyword in EX-99',
                'priority': 3
            }
        
        if filing_has_ex4 and has_keyword(combined_text, EX99_CONTEXTUAL_KEYWORDS):
            return {
                'category': 'uncertain',
                'download_action': 'download',
                'confidence': 'low',
                'reason': 'EX-99 with contextual keywords in EX-4 filing',
                'priority': 4
            }
        
        return {
            'category': 'likely_noise',
            'download_action': 'skip',
            'confidence': 'high',
            'reason': 'EX-99 without indenture signals',
            'priority': 999
        }

    if has_indenture_keyword:
        return {
            'category': 'indenture',
            'download_action': 'download',
            'confidence': 'high',
            'reason': 'Indenture keyword in filename or description',
            'priority': 3
        }

    return {
        'category': 'unknown',
        'download_action': 'skip',
        'confidence': 'high',
        'reason': 'No indenture signals detected',
        'priority': 999
    }


def run_pipeline(root_dir: Path, verbose: bool = False):
    asset_dir = root_dir / "asset"
    exhibits_csv = asset_dir / "exhibits.csv"
    output_csv = asset_dir / "exhibits_classified.csv"
    
    if not exhibits_csv.exists():
        raise FileNotFoundError(f"Phase 1 output not found: {exhibits_csv}")
    
    if verbose:
        print(f"Classifying exhibits...")
    
    exhibits_by_filing = defaultdict(list)
    all_rows = []
    
    with open(exhibits_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        
        for row in reader:
            filing_key = (row['cik'], row['accession'])
            exhibits_by_filing[filing_key].append(row)
            all_rows.append(row)
    
    filings_with_ex4 = set()
    for filing_key, exhibits in exhibits_by_filing.items():
        for exhibit in exhibits:
            major_num, _ = parse_exhibit_number(exhibit['doc_type'])
            if major_num == 4:
                filings_with_ex4.add(filing_key)
                break
    
    classified_rows = []
    stats = {
        'total': 0,
        'indenture': 0,
        'image_based_indenture': 0,
        'warrant': 0,
        'press_release': 0,
        'likely_noise': 0,
        'uncertain': 0,
        'unknown': 0,
        'to_download': 0,
        'to_skip': 0
    }
    
    for row in all_rows:
        stats['total'] += 1
        filing_key = (row['cik'], row['accession'])
        filing_has_ex4 = filing_key in filings_with_ex4
        
        classification = classify_exhibit(row, filing_has_ex4)
        classified_row = {**row, **classification}
        classified_rows.append(classified_row)
        
        category = classification['category']
        if category in stats:
            stats[category] += 1
        
        if classification['download_action'] == 'skip':
            stats['to_skip'] += 1
        else:
            stats['to_download'] += 1

    output_fieldnames = list(fieldnames) + [
        'category', 'download_action', 'confidence', 'reason', 'priority'
    ]
    
    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=output_fieldnames)
        writer.writeheader()
        writer.writerows(classified_rows)
    
    if verbose:
        total_indentures = stats['indenture'] + stats['image_based_indenture']
        print(f"{total_indentures} indentures identified")