#!/usr/bin/env python3
"""
Targeted test for the multi-series maturity upgrade fix.
Run from cal_finder/ directory:
    python3 test_maturity_fix.py

Fetches the 10 affected docs, runs extraction with the new code,
and prints before/after maturity for each series row.
No Supabase writes. No full corpus re-run.
"""
import re
import sys
import requests
from pathlib import Path
from bs4 import BeautifulSoup

# Add extraction module to path
sys.path.insert(0, str(Path(__file__).parent))

TARGET_URLS = {
    'https://www.sec.gov/Archives/edgar/data/1418135/000119312526126214/d130847dex42.htm': 'Keurig Dr Pepper',
    'https://www.sec.gov/Archives/edgar/data/318154/000119312526059490/d106293dex42.htm': 'AMGEN',
    'https://www.sec.gov/Archives/edgar/data/1114448/000110465926031037/tm268725d5_ex4-8.htm': 'NOVARTIS',
    'https://www.sec.gov/Archives/edgar/data/1160106/000095010326001941/dp241381_ex0401.htm': 'Lloyds',
    'https://www.sec.gov/Archives/edgar/data/312069/000119312526066764/d103602dex47.htm': 'BARCLAYS',
    'https://www.sec.gov/Archives/edgar/data/1996810/000114036126003735/ef20064707_ex4-2.htm': 'GE Vernova',
    'https://www.sec.gov/Archives/edgar/data/1000697/000119312526119753/d70582dex42.htm': 'Waters Corp',
    'https://www.sec.gov/Archives/edgar/data/1049502/000119312526038565/d39268dex41.htm': 'MKS INC',
    'https://www.sec.gov/Archives/edgar/data/1039765/000119312526118963/d63921dex41.htm': 'ING Groep',
    'https://www.sec.gov/Archives/edgar/data/1701605/000119312526102422/d39076dex41.htm': 'Baker Hughes',
}

HEADERS = {'User-Agent': 'Ethan Dang research@drexel.edu'}

def fetch_text(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=20)
    soup = BeautifulSoup(r.text, 'html.parser')
    text = soup.get_text(separator=' ', strip=True)
    return re.sub(r'\s+', ' ', text)

def main():
    from extraction.extract_fields import (
        ExtractionConfig, FieldExtractor, SECURITY_PATTERNS,
        _MONTH_PAT, read_html, normalize_text
    )
    from extraction.llm_fallback import extract_maturity_date

    # Load config (needs Mapping.xlsx)
    mapping = Path('Mapping.xlsx')
    if not mapping.exists():
        mapping = next(Path('.').rglob('Mapping.xlsx'), None)
    if not mapping:
        print("ERROR: Mapping.xlsx not found — run from cal_finder/")
        sys.exit(1)

    config = ExtractionConfig.from_mapping_file(mapping, SECURITY_PATTERNS)
    extractor = FieldExtractor(config)

    total_upgraded = 0
    total_year_only = 0

    for url, label in TARGET_URLS.items():
        print(f"\n{'='*60}")
        print(f"  {label}")
        print(f"  {url.split('/')[-1]}")
        try:
            text = fetch_text(url)
        except Exception as e:
            print(f"  FETCH ERROR: {e}")
            continue

        sec_joined = extractor.extract_securities(text[:1200])
        if not sec_joined:
            whereas = re.search(r'NOW,?\s*THEREFORE|WITNESSETH\s+that', text, re.IGNORECASE)
            body = text[whereas.start():] if whereas else text
            sec_joined = extractor.extract_securities(body)

        if not sec_joined:
            print("  No securities found")
            continue

        sec_list = [s.strip() for s in sec_joined.split(';') if s.strip()]
        print(f"  {len(sec_list)} series found")

        for i, sec in enumerate(sec_list):
            yr = extractor.parse_maturity_date(sec) or ''
            tag = 'primary' if i == 0 else f'extra[{i}]'

            if yr and yr.isdigit():
                total_year_only += 1
                # Try regex body upgrade
                full = extractor.extract_maturity_date_from_text(text, sec)
                if full:
                    print(f"  {tag}: {yr!r} → {full!r}  [REGEX ✓]")
                    total_upgraded += 1
                else:
                    # Try LLM gate
                    pos = text.find(yr, 200)
                    if pos != -1:
                        snip = text[max(0, pos-400): pos+400]
                        if _MONTH_PAT.search(snip):
                            result = extract_maturity_date(snip)
                            if result:
                                print(f"  {tag}: {yr!r} → {result.date_str!r}  [LLM ✓]")
                                total_upgraded += 1
                            else:
                                print(f"  {tag}: {yr!r} → (LLM returned nothing)  [STUCK]")
                        else:
                            print(f"  {tag}: {yr!r} → (no month in snippet, LLM skipped)  [STUCK-no-month]")
                    else:
                        print(f"  {tag}: {yr!r} → (year not in body)  [STUCK-no-body-hit]")
            elif yr:
                print(f"  {tag}: {yr!r}  [already full date]")
            else:
                print(f"  {tag}: (no maturity parsed from sec desc)")

    print(f"\n{'='*60}")
    print(f"SUMMARY: {total_upgraded}/{total_year_only} year-only rows upgraded to full date")
    print(f"  Regex upgrades are free. LLM upgrades cost ~$0.001 each.")

if __name__ == '__main__':
    main()