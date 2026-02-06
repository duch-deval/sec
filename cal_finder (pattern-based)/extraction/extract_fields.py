#!/usr/bin/env python3
import csv
import html
import re
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from bs4 import BeautifulSoup
import openpyxl

logger = logging.getLogger(__name__)

# Configuration
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
            compiled_cusip_pattern=re.compile(r"CUSIP.{0,40}?([0-9A-Z]{6})\s*([0-9A-Z]{2}[0-9])", re.IGNORECASE | re.DOTALL),
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



# Pattern tables
_MONTH = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)'
_OPT_FULL_DATE = rf'(?:{_MONTH}\s+\d{{1,2}},?\s+)?'

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
    (rf"(\d+(?:\.\d+)?%\s+[\w\s\-,]{{3,80}}?\b[Dd]ue\s+{_OPT_FULL_DATE}20\d{{2}})", "year"),
]

LOCATION_PATTERNS = [
    (r'federal reserve bank of new york', 'Federal Reserve Bank of New York'),
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
    (r'\bhong kong\b', 'Hong Kong'),
    (r'\bbangkok\b', 'Bangkok'),
    (r'\bsingapore\b', 'Singapore'),
    (r'\bsantiago\b', 'Santiago'),
    (r'\bhelsinki\b', 'Helsinki'),
    (r'\bamsterdam\b', 'Amsterdam'),
    (r'\bireland\b', 'Ireland'),
]

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
    'Collateral': [r'\bcollateral\b(?!\s+agent)', r'\bsecurity\s+interest\b', r'\bpledge\b'],
    'Disposition': [r'\bdisposition[s]?\b', r'\btransfer[s]?\s+(?:of|and)\b', r'\bassignment[s]?\b'],
}

EXPORT_COLUMNS = [
    "Company ", "File Date", "File Type", "File Link ", "Description of Exhibit", "Exhibit",
    "Security Description", "CUSIP", "ISIN",
    "Coupon Rate", "Issue Size", "Maturity Date",
    "Text", "Business Days - Standardized", "Mapping",
    "Governing Law Text", "Governing Law Type", "Governing Law", "Governing Law Code",
]

# Helpers
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

# Extractor
class FieldExtractor:

    def __init__(self, config: ExtractionConfig):
        self.config = config

    # Security Description 
    def extract_securities(self, text: str) -> Optional[str]:
        if not text:
            return None

        normalized = re.sub(r"\s+", " ", text)
        found, seen = [], set()

        for compiled_pat, ptype in self.config.compiled_security_patterns:
            for m in compiled_pat.finditer(normalized):
                desc = m.group(1)
                pct_match = re.search(r'(\d+(?:\.\d+)?)%', desc)
                if not pct_match:
                    continue

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

    # CUSIP 

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

            formatted = f"{cusip[:6]} {cusip[6:]}"
            if formatted not in seen:
                seen.add(formatted)
                found.append(formatted)

            after = normalized[m.end():]

            # Separator-delimited dual CUSIP
            if re.match(r'^[^I]{0,15}?[/;,\]]|^.{0,10}?\band\b', after):
                dual = re.match(r'.{0,20}?([0-9A-Z]{6})\s*([0-9A-Z]{2}[0-9])', after)
                if dual and re.search(r'\d', dual.group(1)):
                    dcusip = dual.group(1).upper() + dual.group(2).upper()
                    df = f"{dcusip[:6]} {dcusip[6:]}"
                    if df not in seen:
                        seen.add(df)
                        found.append(df)
            # Bare adjacent CUSIP
            elif re.match(r'^\s+[0-9A-Z]', after):
                bare = re.match(r'^\s+([0-9A-Z]{6})([0-9A-Z]{2}[0-9])\b', after)
                if bare and re.search(r'\d', bare.group(1)):
                    bcusip = bare.group(1).upper() + bare.group(2).upper()
                    bf = f"{bcusip[:6]} {bcusip[6:]}"
                    if bf not in seen:
                        seen.add(bf)
                        found.append(bf)

        return "; ".join(found) if found else None

    # ISIN
    def extract_isins(self, text: str) -> Optional[str]:
        if not text:
            return None

        found, seen = [], set()
        for m in re.finditer(r'\b([A-Z]{2}[A-Z0-9]{9}[0-9])\b', text):
            isin = m.group(1).upper()
            if isin not in seen and re.search(r'\d', isin[2:11]):
                seen.add(isin)
                found.append(isin)

        return "; ".join(found) if found else None

    #  Business Day Definition 
    def extract_business_day_definition(self, text: str) -> Optional[str]:
        if not text:
            return None

        normalized = normalize_text(text)

        bd_patterns = [
            r'"Business Day"\s*when\s+used\s+with\s+respect\s+to[^.]*means[^.]*(?:\.[^.]*)?',
            r'"Business Day"\s*means[,]?\s*[^.]*(?:\.[^.]*)?(?:\.[^.]*)?',
            r'"Business Day"\s*shall\s+mean\s*[^.]*(?:\.[^.]*)?',
            r'(?:a|each|an?)\s+Business Day\s+means\s+[^.]*\.',
        ]

        bd_text = None
        for pattern in bd_patterns:
            if match := re.search(pattern, normalized, re.IGNORECASE):
                bd_text = match.group(0).strip()
                break

        if not bd_text:
            return None

        # Follow "Legal Holiday" indirection
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
        return ordered

    def map_locations_to_codes(self, locations: List[str]) -> List[str]:
        codes, seen_codes = [], set()
        for loc in locations:
            code = self.config.location_mapping.get(loc.lower())
            if code and code not in seen_codes:
                codes.append(code)
                seen_codes.add(code)
            elif not code:
                logger.warning("Location '%s' not in mapping", loc)
        return codes

    # Governing Law 
    def extract_governing_law(self, text: str) -> List[Dict[str, str]]:
        """Returns list of dicts: {text, type, location, code}."""
        if not text:
            return []
        normalized = normalize_text(text)
        section_text = self._find_governing_law_section(normalized)
        if not section_text:
            section_text = self._find_bare_governing_clauses(normalized)
        return self._parse_governing_law_clauses(section_text) if section_text else []

    def _find_governing_law_section(self, text: str) -> Optional[str]:
        heading_patterns = [
            r'(?:Section|SECTION)\s+[\d.]+\.?\s*(?:Governing\s+Law|GOVERNING\s+LAW|Applicable\s+Law)(?:[.;:]\s*)(.*?)(?=(?:(?<!\bthis\s)(?<!\bof\s)(?<!\bsaid\s)(?:Section|SECTION))\s+[\d.]+[.\s]+[A-Z])',
            r'\(\d+\)\s*(?:Governing\s+Law|GOVERNING\s+LAW)(?:[.;:]\s*)(.*?)(?=\(\d+\)\s*[A-Z])',
            r'\b\d+\.\s*(?:Governing\s+Law|GOVERNING\s+LAW|Applicable\s+Law)(?:[.;:]\s*)(.*?)(?=\b\d+\.\s*[A-Z])',
            r'(?:Governing\s+Law|GOVERNING\s+LAW)\s*[.\s]+((?:THIS|This|The|THE|Each|EACH)[^§]{20,}?)(?=(?:(?<!\bthis\s)(?<!\bof\s)(?:Section|SECTION))\s+[\d.]+[.\s]+[A-Z]|(?:ARTICLE\s+)|$)',
        ]

        best_match = None
        for pattern in heading_patterns:
            for m in re.finditer(pattern, text, re.IGNORECASE | re.DOTALL):
                content = m.group(1).strip()
                if len(content) < 40:
                    continue
                if 'governed' not in content.lower() and 'law of' not in content.lower():
                    continue
                if best_match is None or len(content) > len(best_match):
                    best_match = content

        return best_match[:1500] if best_match else None

    def _find_bare_governing_clauses(self, text: str) -> Optional[str]:
        clauses = []
        for m in re.finditer(
            r'((?:This|THE|The|Each|EACH)\s+(?:Indenture|Note|Security|Agreement|Supplement|Guarantee)'
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
        return ' '.join(clauses) if clauses else None

    def _parse_governing_law_clauses(self, section_text: str) -> List[Dict[str, str]]:
        results = []
        sentences = re.split(r'(?<=[.])\s+', section_text)

        governed_chunks, current_chunk = [], []
        for sent in sentences:
            if re.search(r'governed\s+by|governing\s+law|laws?\s+of', sent, re.IGNORECASE):
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
                r'(.*?governed\s+by.*?laws?\s+of[^,;.]+?)(?:,?\s*except\s+(?:for\s+|that\s+)?)(.*?(?:shall\s+be\s+)?governed\s+by.*)',
                chunk, re.IGNORECASE | re.DOTALL
            )
            if except_match:
                self._add_clause_result(results, except_match.group(1).strip(), chunk)
                self._add_clause_result(results, except_match.group(2).strip(), chunk)
            else:
                self._add_clause_result(results, chunk, chunk)

        seen_locations = set()
        return [r for r in results if r['location'] not in seen_locations and not seen_locations.add(r['location'])]

    def _add_clause_result(self, results: List[Dict], clause: str, full_text: str):
        location = self._extract_governing_jurisdiction(clause)
        if not location:
            return
        location = location.title() if location.isupper() else location
        results.append({
            'text': re.sub(r'\s+', ' ', full_text).strip()[:800],
            'type': self._categorize_governing_law_type(clause),
            'location': location,
            'code': GOVERNING_LAW_MAPPING.get(location.lower(), ''),
        })

    def _extract_governing_jurisdiction(self, clause: str) -> Optional[str]:
        patterns = [
            (r'laws?\s+of\s+(?:the\s+)?Province\s+of\s+([\w\s]+?)(?:\s+and\s+the\s+(?:federal\s+)?laws|\.|,|;|$)', None),
            (r'laws?\s+of\s+(?:the\s+)?Commonwealth\s+of\s+(\w[\w\s]*?)(?:\.|,|;|\s+(?:without|but|applicable)|$)', None),
            (r'laws?\s+of\s+(?:the\s+)?Federal\s+Republic\s+of\s+(\w+)', None),
            (r'\bEnglish\s+Law\b', 'England'),
            (r'\bGerman\s+Law\b', 'Germany'),
            (r'laws?\s+of\s+(?:the\s+)?(?:State\s+of\s+|state\s+of\s+)([\w\s]+?)(?:\.|,|;|\s+(?:without|but|applicable|\()|$)', None),
            (r'internal\s+laws?\s+of\s+(?:the\s+)?(?:State\s+of\s+)?([\w\s]+?)(?:\.|,|;|\s+(?:without|but|applicable|\()|$)', None),
            (r'\blaw\s+of\s+(?:the\s+)?(?:State\s+of\s+)([\w\s]+?)(?:\.|,|;|\s+(?:without|but|applicable|\()|$)', None),
            (r'laws?\s+of\s+(?:the\s+)?((?!State|Commonwealth|Federal|Province|United\s+States)[A-Z][\w\s]*?)(?:\.|,|;|\s+(?:without|but|applicable|\()|$)', None),
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

    def _categorize_governing_law_type(self, clause: str) -> str:
        """Classify by keywords in the subject (before 'governed by')."""
        subject = re.split(r'governed\s+by', clause, maxsplit=1, flags=re.IGNORECASE)[0].lower()
        for category, kw_patterns in GOVERNING_LAW_TYPE_KEYWORDS.items():
            for pattern in kw_patterns:
                if re.search(pattern, subject):
                    return category
        return 'Terms and Conditions'

    # Coupon / Maturity / Issue Size 
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
        patterns = [
            rf'(?:Stated\s+)?Maturity\s+Date["\s:]*(?:is\s+|shall\s+be\s+|means?\s+)?({_MONTH}\s+\d{{1,2}},?\s+20\d{{2}})',
            rf'matur(?:ing|e|es)\s+(?:on\s+)?({_MONTH}\s+\d{{1,2}},?\s+20\d{{2}})',
            rf'\bdue\s+({_MONTH}\s+\d{{1,2}},?\s+20\d{{2}})',
        ]
        for pattern in patterns:
            m = re.search(pattern, text, re.IGNORECASE)
            if m:
                return m.group(1)
        return None

    def extract_issue_size(self, text: str) -> Optional[str]:
        if not text:
            return None
        patterns = [
            r'aggregate\s+(?:initial\s+)?principal\s+amount\s+of\s+[\$¥€£]?\s*([\d,]+(?:\.\d+)?)',
            r'[\$¥€£]\s*([\d,]+(?:\.\d+)?)\s+(?:aggregate\s+)?principal\s+amount',
            r'principal\s+amount\s+(?:of\s+)?[\$¥€£]\s*([\d,]+(?:\.\d+)?)',
            r'in\s+(?:an?\s+)?aggregate\s+(?:principal\s+)?amount\s+(?:of\s+|to\s+)?[\$¥€£]\s*([\d,]+(?:\.\d+)?)',
        ]
        for pattern in patterns:
            m = re.search(pattern, text, re.IGNORECASE)
            if m:
                amount = m.group(1).replace(',', '')
                try:
                    val = float(amount)
                    if val >= 1_000_000:
                        return f"{int(val):,}"
                except ValueError:
                    continue
        return None

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

        if sec := extractor.extract_securities(text):
            row["Security Description"] = sec
            stats['security'] += 1
            if coupon := extractor.parse_coupon_rate(sec):
                row["Coupon Rate"] = coupon
                stats['coupon'] += 1
            if maturity := extractor.parse_maturity_date(sec):
                row["Maturity Date"] = maturity
                stats['maturity'] += 1

        if not row.get("Maturity Date") or row.get("Maturity Date", "").strip().isdigit():
            if full_maturity := extractor.extract_maturity_date_from_text(text, row.get("Security Description")):
                row["Maturity Date"] = full_maturity
                stats['maturity'] += 1

        if cusip := extractor.extract_cusips(text):
            row["CUSIP"] = cusip
            stats['cusip'] += 1

        if isin := extractor.extract_isins(text):
            row["ISIN"] = isin
            stats['isin'] += 1

        if issue_size := extractor.extract_issue_size(text):
            row["Issue Size"] = issue_size
            stats['issue_size'] += 1

        if bd_text := extractor.extract_business_day_definition(text):
            row["Text"] = bd_text
            locations = extractor.extract_locations_from_definition(bd_text)
            codes = extractor.map_locations_to_codes(locations)
            row["Business Days - Standardized"] = "; ".join(locations) if locations else ""
            row["Mapping"] = "; ".join(codes) if codes else ""
            stats['bdays'] += 1

        gov_laws = extractor.extract_governing_law(text)
        if gov_laws:
            row["Governing Law Text"] = gov_laws[0]['text']
            row["Governing Law Type"] = gov_laws[0]['type']
            row["Governing Law"] = gov_laws[0]['location']
            row["Governing Law Code"] = gov_laws[0]['code']
            stats['gov_law'] += 1

            for gl in gov_laws[1:]:
                extra_row = dict(row)
                extra_row["Governing Law Text"] = gl['text']
                extra_row["Governing Law Type"] = gl['type']
                extra_row["Governing Law"] = gl['location']
                extra_row["Governing Law Code"] = gl['code']
                extra_rows.append(extra_row)

    rows.extend(extra_rows)

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=EXPORT_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    logger.info("Extraction: %d docs | Sec: %d | CUSIP: %d | ISIN: %d | BDays: %d | GovLaw: %d | Coupon: %d | Maturity: %d | IssueSize: %d",
                stats['processed'], stats['security'], stats['cusip'], stats['isin'], stats['bdays'],
                stats['gov_law'], stats['coupon'], stats['maturity'], stats['issue_size'])
    return True