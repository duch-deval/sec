#!/usr/bin/env python3
import csv
import html
import re
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
load_dotenv(Path(__file__).parents[2] / '.env')

from collections import Counter

from bs4 import BeautifulSoup
import openpyxl

from .llm_fallback import extract_issue_size, extract_bd_by_reference

logger = logging.getLogger(__name__)


def _cusip_check_digit(cusip8: str) -> Optional[int]:
    """Calculate CUSIP Luhn mod-10 check digit for first 8 characters."""
    values = []
    for c in cusip8.upper():
        if c.isdigit():
            values.append(int(c))
        elif c.isalpha():
            values.append(ord(c) - ord('A') + 10)
        elif c in ('*', '@', '#'):
            values.append({'*': 36, '@': 37, '#': 38}[c])
        else:
            return None
    if len(values) != 8:
        return None
    total = 0
    for i, v in enumerate(values):
        if i % 2 == 1:
            v *= 2
        total += v // 10 + v % 10
    return (10 - total % 10) % 10


def _is_valid_cusip(cusip9: str) -> bool:
    """Validate a 9-character CUSIP using its check digit."""
    if len(cusip9) != 9:
        return False
    expected = _cusip_check_digit(cusip9[:8])
    if expected is None:
        return False
    try:
        return int(cusip9[8]) == expected
    except ValueError:
        return False


@dataclass(frozen=True)
class ExtractionConfig:
    location_mapping: Dict[str, str]
    compiled_security_patterns: List[Tuple[re.Pattern, str]] = field(default_factory=list)
    compiled_cusip_pattern: re.Pattern = field(default=None)

    @classmethod
    def from_mapping_file(cls, mapping_path: Path, security_patterns: List[Tuple[str, str]]) -> 'ExtractionConfig':
        location_mapping = cls._load_calendar_mapping(mapping_path)
        return cls(
            location_mapping={loc.lower(): code for loc, code in location_mapping.items()},
            compiled_security_patterns=[(re.compile(pat, re.IGNORECASE), ptype) for pat, ptype in security_patterns],
            compiled_cusip_pattern=re.compile(r"CUSIP.{0,60}?([0-9A-Z]{6})\s*([0-9A-Z]{2}[0-9])", re.IGNORECASE | re.DOTALL),
        )

    @staticmethod
    def _load_calendar_mapping(xlsx_path: Path) -> Dict[str, str]:
        if not xlsx_path.exists():
            raise FileNotFoundError(f"Mapping file not found: {xlsx_path}")
        wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
        ws = wb.active
        mapping = {}
        for row in ws.iter_rows(min_row=2, values_only=True):
            if row and len(row) >= 2 and row[0] and row[1]:
                mapping[str(row[0]).strip()] = str(row[1]).strip().upper()
        wb.close()
        if not mapping:
            raise ValueError(f"No mappings found in {xlsx_path}")
        logger.info("Loaded %d location mappings", len(mapping))
        return mapping



_MONTH = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)'
_OPT_FULL_DATE = rf'(?:{_MONTH}\s+\d{{1,2}},?\s+)?'
_MONTH_PAT = re.compile(
    r'January|February|March|April|May|June|July|August'
    r'|September|October|November|December',
    re.IGNORECASE,
)
_BD_REF_PAT = re.compile(r'place\s+of\s+payment|legal\s+holiday', re.IGNORECASE)


SECURITY_PATTERNS = [
    (rf"(\d+(?:\.\d+)?%\s+Series\s+[Dd]ue\s+{_OPT_FULL_DATE}20\d{{2}})", "year"),
    (r"(\d+(?:\.\d+)?%\s+(?:Fixed[- ]?Rate[- ]?Reset\s+)?Limited\s+Recourse\s+Capital\s+Notes?,?\s+Series\s+\d+)", "series"),
    (r"(\d+(?:\.\d+)?%\s+(?:Secured\s+)?[A-Za-z\s]+Term\s+Notes?,?\s+Series\s+\d{4}-\d+,?\s+Class\s+[A-Z0-9-]+)", "series"),
    (rf"(\d+(?:\.\d+)?%\s+Non[- ]?Viability\s+Contingent\s+Capital\s+Subordinated\s+Notes?\s+[Dd]ue\s+{_OPT_FULL_DATE}20\d{{2}})", "year"),
    (rf"(\d+(?:\.\d+)?%\s+Senior\s+(?:Priority\s+Guaranteed|Unsecured)\s+Notes?\s+[Dd]ue\s+{_OPT_FULL_DATE}20\d{{2}})", "year"),
    (rf"(\d+(?:\.\d+)?%\s+Fixed[- ]?to[- ]?Fixed\s+(?:Reset\s+)?Rate\s+(?:Junior\s+)?(?:Senior\s+)?Subordinated\s+Notes?,?\s+(?:Series\s+[A-Z]\s+)?[Dd]ue\s+{_OPT_FULL_DATE}20\d{{2}})", "year"),
    (rf"(\d+(?:\.\d+)?%\s+Fixed[- ]?to[- ]?Fixed\s+Rate\s+Junior\s+Subordinated\s+Notes?,?\s+(?:Series\s+[A-Z]\s+)?[Dd]ue\s+{_OPT_FULL_DATE}20\d{{2}})", "year"),
    (r"(\d+(?:\.\d+)?%\s+Convertible\s+(?:Senior\s+)?Notes?\s+[Dd]ue\s+20\d{2})", "year"),
    (r"(\d+(?:\.\d+)?%\s+(?:First|Second)\s+Lien\s+Senior\s+Secured\s+Notes?\s+[Dd]ue\s+20\d{2})", "year"),
    (rf"(\d+(?:\.\d+)?%\s+(?:Senior\s+)?(?:Secured\s+)?Notes?\s+[Dd]ue\s+{_OPT_FULL_DATE}20\d{{2}})", "year"),
    (r"(\d+(?:\.\d+)?%\s+(?:Senior\s+)?Subordinated\s+Notes?\s+[Dd]ue\s+20\d{2})", "year"),
    (r"(\d+(?:\.\d+)?%\s+Fixed[- ]?to[- ]?Floating\s+Rate\s+(?:Senior\s+)?Notes?\s+[Dd]ue\s+20\d{2})", "year"),
    (r"(\d+(?:\.\d+)?%\s+(?:Class\s+[A-Z0-9-]+\s+)?Asset[- ]?[Bb]acked\s+Notes?)", "asset_backed"),
    (r"(\d+(?:\.\d+)?%\s+(?:Auto\s+Loan\s+)?Asset\s+Backed\s+Notes?)", "asset_backed"),
    (r"(\d+(?:\.\d+)?%\s+(?:Floating\s+Rate\s+)?Asset[- ]?[Bb]acked\s+Notes?)", "asset_backed"),
    (rf"(Floating\s+Rate\s+(?:Senior\s+)?(?:Secured\s+)?(?:Subordinated\s+)?Notes?\s+[Dd]ue\s+{_OPT_FULL_DATE}20\d{{2}})", "year_no_pct"),
    (rf"(\d+(?:\.\d+)?%\s+[\w\s\-,]{{3,80}}?\b[Dd]ue\s+{_OPT_FULL_DATE}20\d{{2}})", "year"),
]

LOCATION_PATTERNS = [
    (r'federal reserve bank of new york', 'Federal Reserve Bank of New York'),
    (r'u\.?s\.?\s+government\s+securities\s+business\s+day', 'U.S. Government Securities Business Day'),
    (r'trans-european automated real-time gross settlement express transfer system \(the target2? system\)', 'TARGET'),
    (r'trans-european automated real-time gross settlement express transfer system', 'TARGET'),
    (r'target2 system', 'TARGET'),
    (r'target system', 'TARGET'),
    (r't2 payment system', 'TARGET'),
    (r't2 system', 'TARGET'),
    (r'\bt2\b(?=\s+(?:system|payment))', 'TARGET'),
    (r'real time gross settlement system operated by eurosystem', 'TARGET'),
    (r'\btarget2\b', 'TARGET'),
    (r'\btarget\b(?!\s+system)', 'TARGET'),
    (r'borough of manhattan,?\s*the city of new york', 'New York'),
    (r'(?:the )?city of new york', 'New York'),
    (r'new york,?\s*new york', 'New York'),
    (r'new york city', 'New York'),
    (r'\bnew york\b(?!,?\s*new york)', 'New York'),
    (r"st\. john's,?\s*newfoundland and labrador", 'Newfoundland and Labrador'),
    (r'newfoundland and labrador', 'Newfoundland and Labrador'),
    (r'toronto,?\s*ontario', ['Toronto', 'Ontario']),
    (r'province of ontario', 'Ontario'),
    (r'\bontario\b', 'Ontario'),
    (r'\btoronto\b', 'Toronto'),
    (r'minneapolis,?\s*minnesota', 'Minneapolis'),
    (r'\bminneapolis\b', 'Minneapolis'),
    (r'hartford,?\s*connecticut', 'Hartford'),
    (r'\bhartford\b', 'Hartford'),
    (r'\btokyo\b', 'Tokyo'),
    (r'\blondon\b', 'London'),
    (r'\bsydney\b(?:,?\s*australia)?', 'Sydney'),
    (r'\bbrussels\b', 'Brussels'),
    (r'\bmexico city\b', 'Mexico City'),
    (r'\bparis\b', 'Paris'),
    (r'\bmunich\b', 'Munich'),
    (r'\bfrankfurt\b', 'Frankfurt (Frankfurt am Main)'),
    (r'\bbasel\b', 'Basel'),
    (r'\bzurich\b', 'Zurich'),
    (r'\bseoul\b', 'Seoul'),
    (r'\bstockholm\b', 'Stockholm'),
    (r'\boslo\b', 'Oslo'),
    (r'\bsao paulo\b', 'Sao Paulo'),
    (r'\bmadrid\b', 'Madrid'),
    (r"people'?s\s+republic\s+of\s+china", 'China'),
    (r'\bchina\b', 'China'),
    (r'\bhong kong\b', 'Hong Kong'),
    (r'\bbangkok\b', 'Bangkok'),
    (r'\bsingapore\b', 'Singapore'),
    (r'\bsantiago\b', 'Santiago'),
    (r'\bhelsinki\b', 'Helsinki'),
    (r'\bamsterdam\b', 'Amsterdam'),
    (r'\bireland\b', 'Ireland'),
]

US_STATES = {
    'alabama', 'alaska', 'arizona', 'arkansas', 'california', 'colorado', 'connecticut',
    'delaware', 'florida', 'georgia', 'hawaii', 'idaho', 'illinois', 'indiana', 'iowa',
    'kansas', 'kentucky', 'louisiana', 'maine', 'maryland', 'massachusetts', 'michigan',
    'minnesota', 'mississippi', 'missouri', 'montana', 'nebraska', 'nevada', 'new hampshire',
    'new jersey', 'new mexico', 'new york', 'north carolina', 'north dakota', 'ohio',
    'oklahoma', 'oregon', 'pennsylvania', 'rhode island', 'south carolina', 'south dakota',
    'tennessee', 'texas', 'utah', 'vermont', 'virginia', 'washington', 'west virginia',
    'wisconsin', 'wyoming', 'district of columbia',
}

GOVERNING_LAW_MAPPING = {
    'new york': 'US-NY', 'state of new york': 'US-NY',
    'pennsylvania': 'US-PA', 'commonwealth of pennsylvania': 'US-PA',
    'delaware': 'US-DE', 'state of delaware': 'US-DE',
    'california': 'US-CA', 'texas': 'US-TX',
    'illinois': 'US-IL', 'connecticut': 'US-CT',
    'ohio': 'US-OH', 'new jersey': 'US-NJ',
    'maryland': 'US-MD', 'virginia': 'US-VA',
    'massachusetts': 'US-MA', 'georgia': 'US-GA',
    'north carolina': 'US-NC', 'florida': 'US-FL',
    'minnesota': 'US-MN', 'colorado': 'US-CO',
    'washington': 'US-WA', 'oregon': 'US-OR',
    'michigan': 'US-MI', 'missouri': 'US-MO',
    'england': 'GB', 'english law': 'GB',
    'germany': 'DE', 'federal republic of germany': 'DE',
    'canada': 'CA', 'france': 'FR', 'japan': 'JP',
    'switzerland': 'CH', 'ireland': 'IE', 'australia': 'AU',
    'hong kong': 'HK', 'singapore': 'SG', 'spain': 'ES',
    'italy': 'IT', 'netherlands': 'NL', 'brazil': 'BR',
    'south korea': 'KR', 'korea': 'KR', 'sweden': 'SE',
    'norway': 'NO', 'mexico': 'MX', 'luxembourg': 'LU',
    'scotland': 'GB-SCT', 'bermuda': 'BM', 'cayman islands': 'KY',
    'ontario': 'CA-ON', 'province of ontario': 'CA-ON',
    'quebec': 'CA-QC', 'british columbia': 'CA-BC',
    'alberta': 'CA-AB', 'newfoundland and labrador': 'CA-NL',
    'province of newfoundland and labrador': 'CA-NL',
}

GOVERNING_LAW_FALSE_POSITIVES = [
    'governed by arrangements',
    'governed by such provisions',
    'governed by this base',
    'governed by the provisions',
    'governed by the terms',
    'governed by article',
    'governed by the indenture',
    'governed by the law of said state',
    'governed by the laws of such state',
    'governed by the laws of the united states or a state',
]

GOVERNING_LAW_TYPE_KEYWORDS = {
    'Subordination': [r'\bsubordination\b', r'\bsubordinated\b'],
    'Collateral': [r'\bcollateral\b(?!\s+agent)', r'\bsecurity\s+interest\b', r'\bpledge\b', r'\bguarantee[s]?\b'],
    'Disposition': [r'\bdisposition[s]?\b', r'\btransfer[s]?\s+(?:of|and)\b', r'\bassignment[s]?\b'],
}

# Note: "Company " and "File Link " retain trailing spaces to match
# the Bloomberg-provided CSV header format exactly.
EXPORT_COLUMNS = [
    "Company ", "File Date", "File Type", "File Link ", "Exhibit",
    "Security Description", "CUSIP", "ISIN",
    "Coupon Rate", "Issue Size", "Maturity Date",
    "Text", "Business Days - Standardized", "Mapping",
    "Governing Law Text", "Governing Law Type", "Governing Law", "Governing Law Code",
    "Base Indenture Reference",
]

def normalize_text(text: str) -> str:
    for old, new in [('\x93', '"'), ('\x94', '"'), ('\x91', "'"), ('\x92', "'"),
                     ('\u201c', '"'), ('\u201d', '"'), ('\u2018', "'"), ('\u2019', "'"),
                     ('\xa0', ' ')]:
        text = text.replace(old, new)
    return re.sub(r'\s+', ' ', text)


def read_html(filepath: Path) -> Optional[str]:
    if not filepath.exists():
        return None
    try:
        soup = BeautifulSoup(filepath.read_bytes().decode("utf-8", errors="replace"), "html.parser")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        return html.unescape(soup.get_text()).replace("\xa0", " ")
    except Exception as e:
        logger.warning("Failed to parse %s: %s", filepath, e)
        return None

class FieldExtractor:

    def __init__(self, config: ExtractionConfig):
        self.config = config

    def _find_legacy_table_spans(self, text: str) -> List[Tuple[int, int]]:
        normalized = re.sub(r"\s+", " ", text)
        spans = []

        TABLE_HEADER_RE = re.compile(
            r'(?:Series|Bonds?)\s+.{0,80}?'
            r'(?:Due\s+Date|Maturity|Date\s+of\s+Issue).{0,80}?'
            r'(?:Principal\s+Amount|Amount\s+(?:Issued|Outstanding)|Aggregate)',
            re.IGNORECASE
        )
        for m in TABLE_HEADER_RE.finditer(normalized):
            start = m.start()
            end_markers = [
                re.search(r'\b(?:NOW,?\s*THEREFORE|GRANTING\s+CLAUSES|ARTICLE\s+[IVX\d]|There\s+shall\s+be\s+a\s+series)\b',
                           normalized[start + 200:start + 8000], re.IGNORECASE),
            ]
            end = start + 8000
            for em in end_markers:
                if em:
                    end = start + 200 + em.start()
                    break
            spans.append((start, end))

        WHEREAS_RE = re.compile(
            r'WHEREAS.{0,100}?(?:heretofore\s+(?:issued|created|established)|'
            r'bonds?\s+(?:heretofore|previously|outstanding)|'
            r'series\s+(?:heretofore|previously|outstanding))',
            re.IGNORECASE
        )
        for m in WHEREAS_RE.finditer(normalized):
            start = m.start()
            end_match = re.search(r'\b(?:NOW,?\s*THEREFORE|WHEREAS(?!.{0,30}heretofore))\b',
                                  normalized[start + 100:start + 6000], re.IGNORECASE)
            end = start + 100 + end_match.start() if end_match else start + 6000
            spans.append((start, end))

        SCHEDULE_RE = re.compile(
            r'(?:schedule|list|table)\s+(?:of\s+)?(?:outstanding|existing|previously\s+issued)\s+'
            r'(?:bonds?|notes?|securities|series)',
            re.IGNORECASE
        )
        for m in SCHEDULE_RE.finditer(normalized):
            spans.append((m.start(), m.start() + 5000))

        return spans

    def extract_securities(self, text: str, exclude_spans: List[Tuple[int, int]] = None) -> Optional[str]:
        if not text:
            return None

        normalized = re.sub(r"\s+", " ", text)
        found, seen = [], set()

        for compiled_pat, ptype in self.config.compiled_security_patterns:
            for m in compiled_pat.finditer(normalized):
                if exclude_spans and any(s <= m.start() < e for s, e in exclude_spans):
                    continue
                desc = m.group(1)
                pct_match = re.search(r'(\d+(?:\.\d+)?)%', desc)

                if ptype == "year_no_pct":
                    year = re.search(r'(20\d{2})', desc)
                    if not year:
                        continue
                    pct_key = "floating"
                    key = (pct_key, year.group(1))
                elif not pct_match:
                    continue
                else:
                    pct_key = f"{float(pct_match.group(1)):g}"

                if ptype == "series":
                    series = re.search(r'Series\s+(\d+)', desc, re.IGNORECASE)
                    key = (pct_key, f"S{series.group(1) if series else '?'}")
                elif ptype == "asset_backed":
                    class_match = re.search(r'Class\s+([A-Z0-9-]+)', desc, re.IGNORECASE)
                    if not class_match:
                        before = normalized[max(0, m.start()-25):m.start()]
                        class_matches = list(re.finditer(r'Class\s+([A-Z0-9-]+)', before, re.IGNORECASE))
                        class_match = class_matches[-1] if class_matches else None
                    if class_match:
                        key = (pct_key, f"Class-{class_match.group(1)}")
                    else:
                        type_match = re.search(r'(Auto\s+Loan|Floating\s+Rate|Asset[- ]?[Bb]acked)', desc, re.IGNORECASE)
                        type_str = type_match.group(1).replace(" ", "-") if type_match else "AB"
                        key = (pct_key, type_str)
                else:
                    year = re.search(r'(20\d{2})', desc)
                    if not year:
                        continue
                    key = (pct_key, year.group(1))

                if key in seen:
                    continue
                if ptype == "year_no_pct":
                    year_val = key[1]
                    has_ftf = any('fixed' in s.lower() and 'float' in s.lower() and year_val in s
                                  for _, s in found)
                    if has_ftf:
                        continue
                seen.add(key)
                cleaned = self._capitalize_terms(re.sub(r"\s+", " ", desc).strip())
                found.append((key, cleaned))

        return "; ".join(s for _, s in sorted(found)) if found else None

    @staticmethod
    def _capitalize_terms(text: str) -> str:
        for old, new in [("due", "Due"), ("senior", "Senior"), ("notes", "Notes"), ("note", "Note"),
                         ("secured", "Secured"), ("subordinated", "Subordinated"), ("series", "Series"),
                         ("lien", "Lien"), ("unsecured", "Unsecured"), ("asset backed", "Asset Backed"),
                         ("asset-backed", "Asset-Backed"), ("class", "Class")]:
            text = re.sub(rf"\b{old}\b", new, text, flags=re.IGNORECASE)
        return text

    def extract_cusips(self, text: str) -> Optional[str]:
        if not text:
            return None

        normalized = re.sub(r'[\r\n]+', ' ', text.replace('\xa0', ' '))
        found, seen = [], set()

        for m in self.config.compiled_cusip_pattern.finditer(normalized):
            base, check = m.group(1).upper(), m.group(2).upper()
            cusip = base + check
            if not re.search(r"\d", base):
                continue
            if not _is_valid_cusip(cusip):
                continue

            formatted = f"{cusip[:6]} {cusip[6:]}"
            if formatted not in seen:
                seen.add(formatted)
                found.append(formatted)

            window = normalized[m.end():m.end() + 200]
            isin_boundary = re.search(r'\bISIN\b', window)
            if isin_boundary:
                window = window[:isin_boundary.start()]
            for extra in re.finditer(r'([0-9A-Z]{6})\s*([0-9A-Z]{2}[0-9])', window):
                ebase, echeck = extra.group(1).upper(), extra.group(2).upper()
                if not re.search(r'\d', ebase):
                    continue
                ecusip = ebase + echeck
                if not _is_valid_cusip(ecusip):
                    continue
                ef = f"{ecusip[:6]} {ecusip[6:]}"
                if ef not in seen:
                    seen.add(ef)
                    found.append(ef)

        return "; ".join(found) if found else None

    def extract_isins(self, text: str) -> Optional[str]:
        if not text:
            return None

        found, seen = [], set()
        for m in re.finditer(r'\b([A-Z]{2}[A-Z0-9]{9}[0-9])(?=\b|[A-Z]{2,}|$)', text):
            isin = m.group(1).upper()
            if isin not in seen and re.search(r'\d', isin[2:11]):
                seen.add(isin)
                found.append(isin)

        return "; ".join(found) if found else None

    def extract_business_day_definition(self, text: str) -> Optional[str]:
        if not text:
            return None

        normalized = normalize_text(text)

        bd_patterns = [
            r'["\u201c][\'"]?\s*Business\s+Day\s*[\'"]?\s*["\u201d]?\s*,?\s*when\s+used\s+with\s+respect\s+to[^.]*means[^.]*(?:\.[^.]*)?',
            r'["\u201c][\'"]?\s*Business\s+Day\s*[\'"]?\s*["\u201d]?\s*means[,]?\s*[^.]*(?:\.[^.]*)?(?:\.[^.]*)?',
            r'["\u201c][\'"]?\s*Business\s+Day\s*[\'"]?\s*["\u201d]?\s*shall\s+mean\s*[^.]*(?:\.[^.]*)?',
            r'(?:a|each|an?)\s+Business\s+Day\s+means\s+[^.]*\.',
        ]

        bd_text = None
        for pattern in bd_patterns:
            if match := re.search(pattern, normalized, re.IGNORECASE):
                bd_text = match.group(0).strip()
                break

        if not bd_text:
            return None

        if re.search(r'means[,]?\s*(?:any|each)\s+day\s+(?:which\s+is\s+not|other\s+than)\s+a\s+Legal\s+Holiday', bd_text, re.IGNORECASE):
            for lh_pattern in [r'"Legal Holiday"\s*means\s*[^.]*\.', r'"Legal Holiday"\s*is\s*[^.]*\.',
                               r'[Aa]\s*"Legal Holiday"\s*is\s*[^.]*\.']:
                if lh_match := re.search(lh_pattern, normalized, re.IGNORECASE):
                    return lh_match.group(0).strip()

        sentences = re.split(r'(?<=[.!?])\s+', bd_text)
        if len(sentences) > 3:
            bd_text = ' '.join(sentences[:3])

        next_def = re.search(r'[.!?]\s*"[A-Z][^"]{2,60}"\s*.{0,30}?\b(?:means|shall mean|is defined)\b', bd_text)
        if next_def:
            bd_text = bd_text[:next_def.start() + 1].rstrip()

        return bd_text[:800]

    def extract_locations_from_definition(self, definition: str) -> List[str]:
        if not definition:
            return []

        text_lower = definition.lower()
        positions = []

        for pattern, standard_name in LOCATION_PATTERNS:
            for match in re.finditer(pattern, text_lower, re.IGNORECASE):
                pos = match.start()
                overlap = any(
                    existing_pos <= pos < existing_pos + existing_len or
                    pos <= existing_pos < pos + len(match.group())
                    for existing_pos, existing_len, _ in positions
                )
                if not overlap:
                    names = standard_name if isinstance(standard_name, list) else [standard_name]
                    for name in names:
                        positions.append((pos, len(match.group()), name))

        seen, ordered = set(), []
        for _, _, name in sorted(positions, key=lambda x: x[0]):
            if name not in seen:
                seen.add(name)
                ordered.append(name)

        for city in self._extract_fallback_cities(definition, ordered):
            if city not in seen:
                seen.add(city)
                ordered.append(city)

        return ordered

    def _extract_fallback_cities(self, definition: str, already_found: List[str]) -> List[str]:
        _REAL_PERIOD = r'(?<!\bSt)(?<!\bFt)(?<!\bMt)(?<!\b[NSEW])(?<!\bD\.C)(?<!\bU\.S)[.;]'
        CITY_ANCHORS = [
            re.compile(
                r'(?:banking\s+institutions|banks|trust\s+companies)\s+(?:in|of)\s+'
                r'(?:the\s+(?:City\s+of|Borough\s+of)\s+)?'
                r'([A-Z][A-Za-z.\s,]+?)'
                r'(?=\s+(?:are|is|which|nor|not|shall|that|where)\b|' + _REAL_PERIOD + r')',
                re.DOTALL),
            re.compile(
                r'(?:open\s+for\s+business|close|closed)\s+in\s+'
                r'(?:the\s+(?:City\s+of|Borough\s+of)\s+)?'
                r'([A-Z][A-Za-z.\s,]+?)'
                r'(?=\s+(?:are|is|which|nor|not|shall|that|where|and\s+which)\b|' + _REAL_PERIOD + r')',
                re.DOTALL),
            re.compile(
                r'(?:to\s+close|authorized.{0,40}?close)\s+in\s+'
                r'(?:the\s+(?:City\s+of|Borough\s+of)\s+)?'
                r'([A-Z][A-Za-z.\s,]+?)'
                r'(?=\s+(?:nor|not|shall|that|where)\b|' + _REAL_PERIOD + r')',
                re.DOTALL | re.IGNORECASE),
        ]

        found_lower = set()
        for c in already_found:
            normalized = c.lower().replace(' city', '').strip()
            found_lower.add(normalized)
        found_lower.add('manhattan')

        all_raw = []
        for pattern in CITY_ANCHORS:
            all_raw.extend(pattern.findall(definition))

        new_cities = []
        seen = set()
        for raw in all_raw:
            parts = re.split(r'\s*(?:,\s*(?:or|and)\s*|,\s+|\s+(?:or|and)\s+)', raw.strip())
            for p in parts:
                p = p.strip().rstrip(',').strip()
                if not p or len(p) < 2 or not p[0].isupper():
                    continue
                if p.lower() in US_STATES or p.lower() in {'united kingdom', 'united states', 'united arab emirates'}:
                    continue
                if p.lower() in {'the', 'a', 'an', 'any', 'each', 'saturday', 'sunday', 'monday',
                                  'generally', 'generally,', 'otherwise', 'except', 'including',
                                  'provided', 'where', 'which', 'that', 'such', 'other'}:
                    continue
                if len(p) > 40:
                    continue
                p_norm = re.sub(r'^the\s+(?:city|borough)\s+of\s+', '', p.lower()).strip()
                if p_norm in found_lower or p.lower().replace(' city', '') in found_lower:
                    continue
                if p.lower() in seen:
                    continue
                seen.add(p.lower())
                new_cities.append(p)

        return new_cities

    def map_locations_to_codes(self, locations: List[str]) -> List[str]:
        codes, seen_codes = [], set()
        for loc in locations:
            code = self.config.location_mapping.get(loc.lower())
            if not code:
                code = ''
                logger.info("Location '%s' not in mapping, leaving blank", loc)
            if code not in seen_codes:
                codes.append(code)
                seen_codes.add(code)
        return codes

    def extract_governing_law(self, text: str) -> List[Dict[str, str]]:
        if not text:
            return []
        normalized = normalize_text(text)
        section_text = self._find_governing_law_section(normalized)
        if section_text:
            results = self._parse_governing_law_from_section(section_text)
            if results:
                return results
        bare_text = self._find_bare_governing_clauses(normalized)
        return self._parse_governing_law_from_section(bare_text) if bare_text else []

    def _find_governing_law_section(self, text: str) -> Optional[str]:
        heading_patterns = [
            r'(?:Section|SECTION)\s+[\d.]+\.?\s*(?:Governing\s+Law|GOVERNING\s+LAW|Applicable\s+Law)(?:[.;:]\s*)(.*?)(?=(?:(?<!\bthis\s)(?<!\bof\s)(?<!\bsaid\s)(?:Section|SECTION))\s+[\d.]+[.\s]+[A-Z])',
            r'\(\d+\)\s*(?:Governing\s+Law|GOVERNING\s+LAW)(?:[.;:]\s*)(.*?)(?=\(\d+\)\s*[A-Z])',
            r'\b\d+\.\s*(?:Governing\s+Law|GOVERNING\s+LAW|Applicable\s+Law)(?:[.;:]\s*)(.*?)(?=\b\d+\.\s*[A-Z])',
            r'(?:Governing\s+Law|GOVERNING\s+LAW)\s*[.\s]+((?:THIS|This|The|THE|Each|EACH)[^Ãƒâ€šÃ‚Â§]{20,}?)(?=(?:(?<!\bthis\s)(?<!\bof\s)(?:Section|SECTION))\s+[\d.]+[.\s]+[A-Z]|(?:ARTICLE\s+)|$)',
        ]

        best_match = None
        for pattern in heading_patterns:
            for m in re.finditer(pattern, text, re.IGNORECASE | re.DOTALL):
                content = m.group(1).strip()
                if len(content) < 40:
                    continue
                has_governance = re.search(r'governed|govern[s]?\s|laws?\s+of|English\s+Law|German\s+Law', content, re.IGNORECASE)
                if not has_governance:
                    continue
                if best_match is None or len(content) > len(best_match):
                    best_match = content

        return best_match[:1500] if best_match else None

    def _find_bare_governing_clauses(self, text: str) -> Optional[str]:
        clauses = []
        for m in re.finditer(
            r'((?:This|THE|The|Each|EACH)\s+(?:\w+[\s-]+)*(?:Indenture|Notes?|Securities|Security|Agreement|Supplement|Guarantee)'
            r'[^.]{0,200}?(?:shall be |be |is |are |will be )?'
            r'governed by[^.]{0,300}?\.)',
            text, re.IGNORECASE | re.DOTALL
        ):
            clause = m.group(0).strip()
            if any(fp in clause.lower() for fp in GOVERNING_LAW_FALSE_POSITIVES):
                continue
            if not re.search(r'laws?\s+of|English\s+Law|German\s+Law', clause, re.IGNORECASE):
                continue
            clauses.append(clause)

        for m in re.finditer(
            r'((?:The\s+)?laws?\s+of\s+[^.]{5,80}?shall\s+govern\s+'
            r'(?:the\s+|this\s+)?(?:Indenture|Note|Security|Agreement|Supplement|Guarantee)[^.]{0,150}?\.)',
            text, re.IGNORECASE | re.DOTALL
        ):
            clause = m.group(0).strip()
            if any(fp in clause.lower() for fp in GOVERNING_LAW_FALSE_POSITIVES):
                continue
            clauses.append(clause)

        return ' '.join(clauses) if clauses else None

    def _parse_governing_law_from_section(self, section_text: str) -> List[Dict[str, str]]:
        if not section_text:
            return []

        results = []

        sentences = re.split(r'(?<=[.])\s+', section_text)
        governed_chunks, current_chunk = [], []
        for sent in sentences:
            if re.search(r'governed\s+by|shall\s+govern|laws?\s+of|English\s+Law|German\s+Law', sent, re.IGNORECASE):
                if current_chunk:
                    governed_chunks.append(' '.join(current_chunk))
                current_chunk = [sent]
            elif current_chunk:
                current_chunk.append(sent)
        if current_chunk:
            governed_chunks.append(' '.join(current_chunk))
        if not governed_chunks:
            governed_chunks = [section_text]

        for chunk in governed_chunks:
            if any(fp in chunk.lower() for fp in GOVERNING_LAW_FALSE_POSITIVES):
                continue
            if not re.search(r'laws?\s+of|English\s+Law|German\s+Law', chunk, re.IGNORECASE):
                continue

            except_match = re.search(
                r'(.*?(?:governed\s+by|shall\s+govern).*?laws?\s+of[^,;.]+?)(?:,?\s*except\s+(?:for\s+|that\s+)?)(.*?(?:(?:shall\s+be\s+)?governed\s+by|shall\s+govern).*)',
                chunk, re.IGNORECASE | re.DOTALL
            )
            if except_match:
                self._process_chunk(results, except_match.group(1).strip(), chunk)
                self._process_chunk(results, except_match.group(2).strip(), chunk)
            else:
                self._process_chunk(results, chunk, chunk)

        seen_locations = set()
        return [r for r in results if r['location'] not in seen_locations and not seen_locations.add(r['location'])]

    def _process_chunk(self, results: List[Dict], clause: str, full_text: str):
        location = self._extract_governing_jurisdiction(clause)
        if not location:
            return
        location = location.title() if location.isupper() else location
        full_text_clean = re.sub(r'\s+', ' ', full_text).strip()[:800]
        code = GOVERNING_LAW_MAPPING.get(location.lower(), '')

        types = self._categorize_governing_law_types(clause)

        for gov_type in types:
            results.append({
                'text': full_text_clean,
                'type': gov_type,
                'location': location,
                'code': code,
            })

    def _extract_governing_jurisdiction(self, clause: str) -> Optional[str]:
        _END = r'(?:\.|,|;|\s+(?:without|but|applicable|shall|will|that|and\s+the|\()|$)'
        patterns = [
            (rf'laws?\s+of\s+(?:the\s+)?Province\s+of\s+([\w\s]+?)(?:\s+and\s+the\s+(?:federal\s+)?laws|{_END})', None),
            (rf'laws?\s+of\s+(?:the\s+)?Commonwealth\s+of\s+(\w[\w\s]*?){_END}', None),
            (r'laws?\s+of\s+(?:the\s+)?Federal\s+Republic\s+of\s+(\w+)', None),
            (r'\bEnglish\s+Law\b', 'England'),
            (r'\bGerman\s+Law\b', 'Germany'),
            (rf'laws?\s+of\s+(?:the\s+)?(?:State\s+of\s+|state\s+of\s+)([\w\s]+?){_END}', None),
            (rf'internal\s+laws?\s+of\s+(?:the\s+)?(?:State\s+of\s+)?([\w\s]+?){_END}', None),
            (rf'\blaw\s+of\s+(?:the\s+)?(?:State\s+of\s+)([\w\s]+?){_END}', None),
            (rf'laws?\s+of\s+(?:the\s+)?((?!State|Commonwealth|Federal|Province|United\s+States)[A-Z][\w\s]*?){_END}', None),
        ]

        for pattern, fixed_name in patterns:
            m = re.search(pattern, clause, re.IGNORECASE)
            if m:
                if fixed_name:
                    return fixed_name
                name = m.group(1).strip()
                name = re.sub(r'\s+(?:and|or|the|but|without|including|applicable)\s*$', '', name, flags=re.IGNORECASE).strip()
                if 1 < len(name) < 50:
                    return name
        return None

    def _categorize_governing_law_types(self, clause: str) -> List[str]:
        parts = re.split(r'governed\s+by|shall\s+govern', clause, maxsplit=1, flags=re.IGNORECASE)
        subject = parts[0].lower() if parts else clause.lower()

        types = []

        for category, kw_patterns in GOVERNING_LAW_TYPE_KEYWORDS.items():
            if category == 'Collateral':
                for pattern in kw_patterns:
                    if pattern == r'\bguarantee[s]?\b':
                        continue
                    if re.search(pattern, subject):
                        types.append(category)
                        break
            else:
                for pattern in kw_patterns:
                    if re.search(pattern, subject):
                        types.append(category)
                        break

        if not types:
            types.append('Terms and Conditions')

        if re.search(r'\bguarantee[s]?\b', subject):
            if 'Terms and Conditions' not in types:
                types.insert(0, 'Terms and Conditions')
            if 'Collateral' not in types:
                types.append('Collateral')

        return types

    @staticmethod
    def parse_coupon_rate(security_desc: str) -> Optional[str]:
        if not security_desc:
            return None
        rates, seen = [], set()
        for m in re.finditer(r'(\d+(?:\.\d+)?)\s*%', security_desc):
            rate = f"{m.group(1)}%"
            if rate not in seen:
                seen.add(rate)
                rates.append(rate)
        return "; ".join(rates) if rates else None

    @staticmethod
    def parse_maturity_date(security_desc: str) -> Optional[str]:
        if not security_desc:
            return None
        m = re.search(rf'[Dd]ue\s+({_MONTH}\s+\d{{1,2}},?\s+20\d{{2}})', security_desc)
        if m:
            return m.group(1)
        m = re.search(r'[Dd]ue\s+(20\d{2})', security_desc)
        return m.group(1) if m else None

    def extract_maturity_date_from_text(self, text: str, security_desc: str = None) -> Optional[str]:
        if not text:
            return None
        target_year = None
        if security_desc:
            yr_m = re.search(r'\b(20\d{2})\b', security_desc)
            if yr_m:
                target_year = yr_m.group(1)
        patterns = [
            rf'(?:Stated\s+)?Maturity\s+Date["\s:]*(?:is\s+|shall\s+be\s+|means?\s+)?({_MONTH}(?:\s|&nbsp;)+\d{{1,2}},?(?:\s|&nbsp;)+20\d{{2}})',
            rf'matur(?:ing|e|es)\s+(?:on\s+)?({_MONTH}(?:\s|&nbsp;)+\d{{1,2}},?(?:\s|&nbsp;)+20\d{{2}})',
            rf'\bdue\s+({_MONTH}(?:\s|&nbsp;)+\d{{1,2}},?(?:\s|&nbsp;)+20\d{{2}})',
            rf'(?:Maturity\s+Date|mature[sd]?)\b[^.\n]{{0,200}}?\bon\s+({_MONTH}(?:\s|&nbsp;)+\d{{1,2}},?(?:\s|&nbsp;)+20\d{{2}})\s*\(',
            rf'principal\b[^.\n]{{0,80}}\bpayable\s+on\s+({_MONTH}(?:\s|&nbsp;)+\d{{1,2}},?(?:\s|&nbsp;)+20\d{{2}})\b',
            rf'(?:redemption|repay(?:ment)?|(?<!interest\s)payment\s+of\s+principal)\b[^.\n]{{0,60}}(?:at|on)\s+({_MONTH}\s+\d{{1,2}},?\s+20\d{{2}})',
            rf'principal\b[^.\n]{{0,120}}\bon\s+({_MONTH}\s+\d{{1,2}},?\s+20\d{{2}})',
        ]
        for pattern in patterns:
            for m in re.finditer(pattern, text, re.IGNORECASE):
                result = re.sub(r'&nbsp;', ' ', m.group(1)).strip()
                if target_year is None or target_year in result:
                    return result
        return None

    def extract_issue_size(self, text: str) -> Optional[str]:
        if not text:
            return None
        _CUR_KNOWN = r'(?:U\.S\.\$|US\$|Cdn\$|\$|€|¥|£|C\$|A\$|HK\$|NZ\$|S\$|Mex\$|R\$|CAD|AUD|BRL|MXN|CHF|CNY|HKD|NZD|SGD|SEK|NOK|DKK|PLN|ZAR|₹|₩|kr|Rs\.?)'
        _COMMON_3 = r'(?!THE|DUE|PER|FOR|ARE|NOT|AND|BUT|ALL|ANY|HAS|HAD|ITS|MAY|OUR|OWN|SET|SUM|TAX|USE|WAS|YET)'
        _CUR_GENERIC = rf'(?:[A-Z]{{0,3}}\$|[€¥£₹₩₫₿฿₪₱₨₵₡₣₲₴₺₼₽₾]|{_COMMON_3}[A-Z]{{3}}(?=\s*[\d,]))'
        _CUR = rf'(?:{_CUR_KNOWN}|{_CUR_GENERIC})'
        patterns = [
            rf'aggregate\s+(?:initial\s+)?principal\s+amount\s+of\s+{_CUR}\s*([\d,]+(?:\.\d+)?)',
            rf'{_CUR}\s*([\d,]+(?:\.\d+)?)\s+(?:aggregate\s+)?principal\s+amount',
            rf'principal\s+amount\s+(?:of\s+)?{_CUR}\s*([\d,]+(?:\.\d+)?)',
            rf'in\s+(?:an?\s+)?aggregate\s+(?:principal\s+)?amount\s+(?:of\s+|to\s+)?{_CUR}\s*([\d,]+(?:\.\d+)?)',
            rf'aggregate\s+(?:initial\s+)?principal\s+amount\s+of\s+[\s\S]{{1,120}}(?:shall\s+be|is|equals?|of)\s+{_CUR}\s*([\d,]+(?:\.\d+)?)',
            rf'aggregate\s+(?:initial\s+)?principal\s+amount\s+[\s\S]{{1,80}}\({_CUR}\s*([\d,]+(?:\.\d+)?)\)',
            rf'(?:limited\s+(?:\w+\s+){{0,2}}to|up\s+to)\s+{_CUR}\s*([\d,]+(?:\.\d+)?)',
            rf'(?:not\s+to\s+exceed|not\s+exceeding)\s+{_CUR}\s*([\d,]+(?:\.\d+)?)',
        ]
        _NEG_CONTEXT = re.compile(
            r'(?:at\s+least|not\s+less\s+than|in\s+excess\s+of|minimum\s+denomination|'
            r'increments?\s+of|a\s+Holder\s+of|Holders?\s+of\s+at\s+least|'
            r'in\s+(?:a\s+)?minimum|exceeds?|greater\s+than|'
            r'quotations?\s+.{0,30}for|bid\s+.{0,20}for)',
            re.IGNORECASE
        )
        _NEG_COVENANT = re.compile(
            r'Indebtedness',
            re.IGNORECASE
        )
        _NEG_AFTER = re.compile(
            r'\s+or\s+(?:less|more|such\s+lesser|greater)',
            re.IGNORECASE
        )

        def _find_amounts(search_text):
            amounts = []
            for pat in patterns:
                for m in re.finditer(pat, search_text, re.IGNORECASE):
                    preceding = search_text[max(0, m.start() - 60):m.start()]
                    if _NEG_CONTEXT.search(preceding):
                        continue
                    preceding_wide = search_text[max(0, m.start() - 120):m.start()]
                    if _NEG_COVENANT.search(preceding_wide):
                        continue
                    following = search_text[m.end():m.end() + 40]
                    if _NEG_AFTER.search(following):
                        continue
                    raw = m.group(1).replace(',', '')
                    try:
                        val = float(raw)
                        if val > 0:
                            amounts.append(val)
                    except ValueError:
                        continue
            return amounts

        cover_amounts = _find_amounts(text[:2000])
        if cover_amounts:
            return f"{int(max(cover_amounts)):,}"

        cover = text[:2000]
        if re.search(r'(?:CUSIP|ISIN)', cover, re.IGNORECASE):
            for m in re.finditer(rf'{_CUR}\s*([\d,]+(?:\.\d+)?)', cover, re.IGNORECASE):
                raw = m.group(1).replace(',', '')
                try:
                    val = float(raw)
                except ValueError:
                    continue
                if val < 1_000_000:
                    continue
                preceding = cover[max(0, m.start() - 60):m.start()]
                if _NEG_CONTEXT.search(preceding):
                    continue
                nearby = cover[m.start():m.end() + 200]
                if re.search(r'(?:CUSIP|ISIN)', nearby, re.IGNORECASE):
                    return f"{int(val):,}"

        for pat in patterns:
            for m in re.finditer(pat, text, re.IGNORECASE):
                preceding = text[max(0, m.start() - 60):m.start()]
                if _NEG_CONTEXT.search(preceding):
                    continue
                preceding_wide = text[max(0, m.start() - 120):m.start()]
                if _NEG_COVENANT.search(preceding_wide):
                    continue
                following = text[m.end():m.end() + 40]
                if _NEG_AFTER.search(following):
                    continue
                raw = m.group(1).replace(',', '')
                try:
                    val = float(raw)
                    if val > 0:
                        return f"{int(val):,}"
                except ValueError:
                    continue
        return None

    def detect_base_indenture_reference(self, text: str) -> Optional[str]:
        if not text:
            return None

        normalized = re.sub(r'\s+', ' ', text[:8000])

        is_supplemental = bool(re.search(r'supplemental\s+indenture', normalized, re.IGNORECASE))
        is_note_form = bool(re.search(
            r'(?:THIS\s+(?:NOTE|CERTIFICATE|SECURITY)\s+IS\s+A\s+GLOBAL\s+(?:NOTE|SECURITY))|'
            r'(?:UNLESS\s+THIS\s+CERTIFICATE\s+IS\s+PRESENTED)|'
            r'(?:INDENTURE\s+HEREINAFTER\s+REFERRED\s+TO)|'
            r'(?:Indenture\s+referred\s+to\s+on\s+the\s+reverse)',
            normalized, re.IGNORECASE
        ))

        if not is_supplemental and not is_note_form:
            return None

        m = re.search(
            r'(?:base|original|initial)\s+indenture["\s)]*,?\s*dated\s+(?:as\s+of\s+)?'
            r'([A-Z][a-z]+\s+\d{1,2},?\s+\d{4})',
            normalized, re.IGNORECASE
        )
        if m:
            return f"Refers to Base Indenture dated {m.group(1)}"

        m = re.search(
            r'(?:under|pursuant\s+to)\s+(?:the|an)\s+indenture["\s)]*,?\s*dated\s+(?:as\s+of\s+)?'
            r'([A-Z][a-z]+\s+\d{1,2},?\s+\d{4})',
            normalized, re.IGNORECASE
        )
        if m:
            return f"Refers to Indenture dated {m.group(1)}"

        m = re.search(
            r'(?<!supplemental\s)indenture["\s)]*,?\s*dated\s+(?:as\s+of\s+)?'
            r'([A-Z][a-z]+\s+\d{1,2},?\s+\d{4})',
            normalized, re.IGNORECASE
        )
        if m:
            return f"Refers to Indenture dated {m.group(1)}"

        if is_supplemental:
            return "Supplemental indenture; base indenture reference not resolved"
        return "Note form; base indenture reference not resolved"

def find_mapping_file(root_dir: Path) -> Path:
    for path in [root_dir / "Mapping.xlsx",
                 Path(__file__).parent / "resources" / "Mapping.xlsx",
                 Path("Mapping.xlsx")]:
        if path.exists():
            return path
    raise FileNotFoundError("Mapping.xlsx not found")

def run_pipeline(root_dir: Path, mapping_xlsx: Path = None, verbose: bool = False) -> bool:
    root_dir = Path(root_dir)
    raw_csv = root_dir / f"{root_dir.name}_raw.csv"
    root_csv = root_dir / f"{root_dir.name}.csv"
    asset_csv = root_dir / "asset" / "candidates_for_extraction.csv"

    if raw_csv.exists():
        input_csv = raw_csv
    elif root_csv.exists():
        input_csv = root_csv
    elif asset_csv.exists():
        input_csv = asset_csv
    else:
        logger.error("No input CSV found in %s", root_dir)
        return False

    output_csv = root_dir / f"{root_dir.name}.csv"

    if mapping_xlsx is None:
        mapping_xlsx = find_mapping_file(root_dir)

    config = ExtractionConfig.from_mapping_file(mapping_xlsx, SECURITY_PATTERNS)
    extractor = FieldExtractor(config)

    with open(input_csv, 'r', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))

    if not rows:
        return True

    stats = {'processed': 0, 'security': 0, 'cusip': 0, 'isin': 0,
             'bdays': 0, 'gov_law': 0, 'coupon': 0, 'maturity': 0, 'issue_size': 0}
    extra_rows = []

    for row in rows:
        if not (path := row.get("_local_path")):
            continue
        if not (text := read_html(Path(path))):
            continue

        stats['processed'] += 1

        # Skip WHEREAS preamble for security extraction — it lists historical
        # securities from prior supplemental indentures, not the new issuance.
        whereas_end = re.search(r'NOW,?\s*THEREFORE|WITNESSETH\s+that', text, re.IGNORECASE)
        body_text = text[whereas_end.start():] if whereas_end else text

        cover_sec = extractor.extract_securities(text[:1200])
        if cover_sec:
            sec_joined = cover_sec
        else:
            legacy_spans = extractor._find_legacy_table_spans(body_text)
            sec_joined = extractor.extract_securities(body_text, exclude_spans=legacy_spans)
            if not sec_joined and whereas_end:
                legacy_spans = extractor._find_legacy_table_spans(text)
                sec_joined = extractor.extract_securities(text, exclude_spans=legacy_spans)
        cusip_joined = extractor.extract_cusips(text)
        isin_joined = extractor.extract_isins(text)
        issue_size = extractor.extract_issue_size(text)
        bd_text = extractor.extract_business_day_definition(text)
        gov_laws = extractor.extract_governing_law(text)

        if bd_text:
            locations = extractor.extract_locations_from_definition(bd_text)
            codes = extractor.map_locations_to_codes(locations)
            row["Text"] = bd_text
            row["Business Days - Standardized"] = "; ".join(locations) if locations else ""
            row["Mapping"] = "; ".join(codes) if codes else ""
            if not locations and not row.get("Base Indenture Reference"):
                if _BD_REF_PAT.search(bd_text):
                    _bd_pos = text.lower().find('place of payment')
                    if _bd_pos == -1:
                        _bd_pos = text.lower().find('legal holiday')
                    _bd_snip = text[max(0, _bd_pos - 200): _bd_pos + 800] if _bd_pos > -1 else bd_text
                    llm_result = extract_bd_by_reference(_bd_snip)
                    if llm_result:
                        llm_locations = llm_result.locations
                        llm_codes = extractor.map_locations_to_codes(llm_locations)
                        row["Business Days - Standardized"] = "; ".join(llm_locations)
                        row["Mapping"] = "; ".join(llm_codes) if llm_codes else ""
                        stats['bdays'] += 1
            else:
                stats['bdays'] += 1

        if not issue_size and not row.get("Base Indenture Reference"):
            llm_result = extract_issue_size(text)
            if llm_result:
                issue_size = f"{int(llm_result.amount):,}"
                row["Issue Size"] = issue_size
                stats['issue_size'] += 1
        else:
            row["Issue Size"] = issue_size
            stats['issue_size'] += 1

        if gov_laws:
            row["Governing Law Text"] = gov_laws[0]['text']
            row["Governing Law Type"] = gov_laws[0]['type']
            row["Governing Law"] = gov_laws[0]['location']
            row["Governing Law Code"] = gov_laws[0]['code']
            stats['gov_law'] += 1

        if not bd_text or not gov_laws:
            base_ref = extractor.detect_base_indenture_reference(text)
            if base_ref:
                row["Base Indenture Reference"] = base_ref

        sec_list = [s.strip() for s in sec_joined.split("; ")] if sec_joined else []
        cusip_list = [c.strip() for c in cusip_joined.split("; ")] if cusip_joined else []
        isin_list = [i.strip() for i in isin_joined.split("; ")] if isin_joined else []

        if len(sec_list) <= 1:
            if sec_joined:
                row["Security Description"] = sec_joined
                stats['security'] += 1
                if coupon := extractor.parse_coupon_rate(sec_joined):
                    row["Coupon Rate"] = coupon
                    stats['coupon'] += 1
                if maturity := extractor.parse_maturity_date(sec_joined):
                    row["Maturity Date"] = maturity
                    stats['maturity'] += 1
            if cusip_joined:
                row["CUSIP"] = cusip_joined
                stats['cusip'] += 1
            if isin_joined:
                row["ISIN"] = isin_joined
                stats['isin'] += 1
        else:
            stats['security'] += 1
            if cusip_joined:
                stats['cusip'] += 1
            if isin_joined:
                stats['isin'] += 1

            row["Security Description"] = sec_list[0]
            row["CUSIP"] = cusip_list[0] if cusip_list else ""
            row["ISIN"] = isin_list[0] if isin_list else ""
            if coupon := extractor.parse_coupon_rate(sec_list[0]):
                row["Coupon Rate"] = coupon
                stats['coupon'] += 1
            if maturity := extractor.parse_maturity_date(sec_list[0]):
                row["Maturity Date"] = maturity
                stats['maturity'] += 1

            for idx in range(1, len(sec_list)):
                sec_row = dict(row)
                sec_row["Security Description"] = sec_list[idx]
                sec_row["CUSIP"] = cusip_list[idx] if idx < len(cusip_list) else ""
                sec_row["ISIN"] = isin_list[idx] if idx < len(isin_list) else ""
                sec_row["Coupon Rate"] = extractor.parse_coupon_rate(sec_list[idx]) or ""
                _sec_maturity = extractor.parse_maturity_date(sec_list[idx]) or ""
                if _sec_maturity and _sec_maturity.isdigit():
                    _full = extractor.extract_maturity_date_from_text(text, sec_list[idx])
                    if _full:
                        _sec_maturity = _full
                sec_row["Maturity Date"] = _sec_maturity
                extra_rows.append(sec_row)

        if not row.get("Maturity Date") or row.get("Maturity Date", "").strip().isdigit():
            if full_maturity := extractor.extract_maturity_date_from_text(text, row.get("Security Description")):
                row["Maturity Date"] = full_maturity
                stats['maturity'] += 1

        if gov_laws and len(gov_laws) > 1:
            for gl in gov_laws[1:]:
                gl_row = dict(row)
                gl_row["Governing Law Text"] = gl['text']
                gl_row["Governing Law Type"] = gl['type']
                gl_row["Governing Law"] = gl['location']
                gl_row["Governing Law Code"] = gl['code']
                extra_rows.append(gl_row)

    rows.extend(extra_rows)

    file_issue_counts = Counter(
        (r.get("File Link", ""), r.get("Issue Size", ""))
        for r in rows
        if r.get("Issue Size", "")
    )
    master_indenture_keys = {
        (file, size) for (file, size), count in file_issue_counts.items()
        if count > 10
    }
    if master_indenture_keys:
        for r in rows:
            key = (r.get("File Link", ""), r.get("Issue Size", ""))
            if key in master_indenture_keys:
                r["Issue Size"] = ""

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=EXPORT_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


    logger.info("Extraction: %d docs | Sec: %d | CUSIP: %d | ISIN: %d | BDays: %d | GovLaw: %d | Coupon: %d | Maturity: %d | IssueSize: %d",
                stats['processed'], stats['security'], stats['cusip'], stats['isin'], stats['bdays'],
                stats['gov_law'], stats['coupon'], stats['maturity'], stats['issue_size'])
    return True