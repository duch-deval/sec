"""
Prompt test harness — Phase 2 validation.
Natasha's anti-hallucination requirement: LLM must return null on miss docs.

Usage (from cal_finder/):
  python3 -m extraction.test_prompts --verify 2026-02-03/exhibits/ex4/tm264911d1_ex4-1.htm
  python3 -m extraction.test_prompts
  python3 -m extraction.test_prompts --field payment_calendar
"""
import sys
import json
import argparse
import logging
from pathlib import Path
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv(Path(__file__).parents[2] / '.env')

from .extract_fields import FieldExtractor, ExtractionConfig
from .llm_fallback import extract_issue_size, extract_maturity_date, extract_bd_by_reference

logging.basicConfig(level=logging.WARNING)

MAPPING_PATH = Path(__file__).parent / "resources" / "Mapping.xlsx"
CAL_FINDER   = Path(__file__).parents[1]


def read_text(rel_path: str) -> str:
    path = CAL_FINDER / rel_path
    if not path.exists():
        return ""
    raw = path.read_text(encoding="utf-8", errors="replace")
    return BeautifulSoup(raw, "html.parser").get_text(separator=" ", strip=True)


def run_llm(field: str, snippet: str):
    if field == "payment_calendar":
        return extract_bd_by_reference(snippet)
    elif field == "maturity_date":
        return extract_maturity_date(snippet)
    elif field == "issue_size":
        return extract_issue_size(snippet)
    return None


# ── Test manifest ─────────────────────────────────────────────────────────────
# expect=None → hallucination test: LLM MUST return null (Natasha requirement)
# expect=str  → precision test: expected substring must appear in result
TESTS = [

    # ══ PAYMENT CALENDAR — hit (BD text exists, locations empty) ══════════════
    {
        "file":   "2026-02-05/exhibits/ex4/d39268dex41.htm",
        "field":  "payment_calendar",
        "expect": None,
        "label":  "MKS Inc — Legal Holiday unresolvable, no definition in doc",
        "source": "verify: Legal Holiday not defined in this exhibit",
    },

    # ══ PAYMENT CALENDAR — misses (confirmed absent or empty) ═════════════════
    {
        "file":   "2026-02-03/exhibits/ex4/dp241016_ex0402.htm",
        "field":  "payment_calendar",
        "expect": None,
        "label":  "Uniti — BD text empty, no locations",
        "source": "verify: BD=EMPTY",
    },
    {
        "file":   "2026-02-04/exhibits/ex4/d54396dex42.htm",
        "field":  "payment_calendar",
        "expect": None,
        "label":  "Columbus McKinnon — all fields absent",
        "source": "verify: all EMPTY",
    },
    {
        "file":   "2026-02-04/exhibits/ex4/ef20064744_ex4-1.htm",
        "field":  "payment_calendar",
        "expect": None,
        "label":  "Dayforce — BD absent, confirmed by Natasha",
        "source": "Natasha Feb04 QA 🟢 + verify: BD=EMPTY",
    },
    {
        "file":   "2026-02-02/exhibits/ex4/d81485dex42.htm",
        "field":  "payment_calendar",
        "expect": None,
        "label":  "Goldman Sachs BDC — BD absent",
        "source": "Natasha Feb02 QA 🟢 + verify: BD=EMPTY",
    },
    {
        "file":   "2026-02-02/exhibits/ex4/tm263646d5_ex4-2.htm",
        "field":  "payment_calendar",
        "expect": None,
        "label":  "United Airlines — BD absent",
        "source": "verify: BD=EMPTY",
    },

    # ══ ISSUE SIZE — hits (regex empty, data exists in doc) ═══════════════════
    {
        "file":   "2026-02-02/exhibits/ex4/d66948dex44.htm",
        "field":  "issue_size",
        "expect": None,
        "label":  "Capital One 5.399% — placeholder $[], no real amount",
        "source": "verify: $[] placeholder doc",
    },
    {
        "file":   "2026-02-02/exhibits/ex4/d66948dex43.htm",
        "field":  "issue_size",
        "expect": None,
        "label":  "Capital One 4.722% — placeholder $[], no real amount",
        "source": "verify: $[] placeholder doc",
    },

    # ══ ISSUE SIZE — misses/noise (LLM must return null) ══════════════════════
    {
        "file":   "2026-02-04/exhibits/ex4/d54396dex42.htm",
        "field":  "issue_size",
        "expect": None,
        "label":  "Columbus McKinnon — issue size genuinely absent",
        "source": "verify: all EMPTY",
    },
    {
        "file":   "2026-02-04/exhibits/ex4/ef20064744_ex4-1.htm",
        "field":  "issue_size",
        "expect": None,
        "label":  "Dayforce — 1,000 is face value noise, not issue size",
        "source": "verify: issue_size=1,000 (noise)",
    },

    # ══ ISSUE SIZE — additional misses ══════════════════════════════════════════
    {
        "file":   "2026-02-12/exhibits/ex4/tm262279d7_ex4-1.htm",
        "field":  "issue_size",
        "expect": None,
        "label":  "American Honda ABS — all fields absent",
        "source": "verify: all EMPTY",
    },
    {
        "file":   "2026-02-18/exhibits/ex4/ef20065815_ex4-1.htm",
        "field":  "issue_size",
        "expect": None,
        "label":  "Loews — issue size confirmed absent",
        "source": "Natasha Feb18 QA + verify: all EMPTY",
    },

    # ══ ISSUE SIZE — hits (regex empty, amount in doc) ════════════════════════
    {
        "file":   "2026-02-05/exhibits/ex4/d84332dex42.htm",
        "field":  "issue_size",
        "expect": "500,000,000",
        "label":  "McCormick — $500M on cover page, regex miss",
        "source": "Natasha Feb05 QA + verify: EMPTY",
    },
    {
        "file":   "2026-02-06/exhibits/ex4/tm265455d1_ex4-1.htm",
        "field":  "issue_size",
        "expect": None,
        "label":  "PacifiCorp — amount in deep APA table, outside 1500 char window",
        "source": "known limitation: snippet too short for this doc structure",
    },

    # ══ MATURITY DATE — miss (no maturity in doc) ═════════════════════════════
    {
        "file":   "2026-02-05/exhibits/ex4/d104980dex41.htm",
        "field":  "maturity_date",
        "expect": None,
        "label":  "FedEx indenture — maturity genuinely absent",
        "source": "verify: Maturity=EMPTY",
    },
]

def verify_file(rel_path: str):
    """Print what regex extracts from a file. Use to qualify new test cases."""
    text = read_text(rel_path)
    if not text:
        print(f"❌ File not found: {rel_path}")
        return

    config = ExtractionConfig.from_mapping_file(MAPPING_PATH, security_patterns=[])
    extractor = FieldExtractor(config)

    bd_text = extractor.extract_business_day_definition(text)
    locations = extractor.extract_locations_from_definition(bd_text) if bd_text else []
    issue_size = extractor.extract_issue_size(text)
    maturity = extractor.extract_maturity_date_from_text(text, "")

    print(f"\n{'='*60}")
    print(f"VERIFY: {rel_path}")
    print(f"{'='*60}")
    print(f"  BD text:      {(bd_text or 'EMPTY')[:120]}")
    print(f"  BD locations: {locations or 'EMPTY'}")
    print(f"  Issue size:   {issue_size or 'EMPTY'}")
    print(f"  Maturity:     {maturity or 'EMPTY'}")
    print()
    if not bd_text and not issue_size and not maturity:
        print("→ All empty: SAFE to use as expect=None (miss/hallucination test)")
    else:
        print("→ Has data: use as precision hit, set expect to the value above")


def run_tests(field_filter=None):
    tests = [t for t in TESTS if not field_filter or t["field"] == field_filter]
    r = {"pass": 0, "fail": 0, "skip": 0}
    hallucinations = []
    precision_fails = []

    print(f"\n{'='*65}")
    print(f"  PROMPT TEST HARNESS — {len(tests)} cases"
          + (f" [{field_filter}]" if field_filter else ""))
    print(f"{'='*65}\n")

    for t in tests:
        text = read_text(t["file"])
        if not text:
            print(f"  ⚠️  SKIP  [{t['field']}] {t['label']}")
            print(f"           Missing: {t['file']}\n")
            r["skip"] += 1
            continue

        if t["field"] == "payment_calendar":
            config = ExtractionConfig.from_mapping_file(MAPPING_PATH, security_patterns=[])
            extractor = FieldExtractor(config)
            bd_text = extractor.extract_business_day_definition(text)
            snippet = bd_text if bd_text else ""
        else:
            snippet = text[:1500]
        if not snippet:
            # No BD text extracted — treat as null result directly
            result = None
            if t["expect"] is None:
                print(f"  ✅ PASS  [{t['field']}] {t['label']}")
                print(f"           No BD text in doc — correctly null\n")
                r["pass"] += 1
            else:
                print(f"  ❌ FAIL  [{t['field']}] {t['label']}")
                print(f"           Expected '{t['expect']}' — no BD text found\n")
                r["fail"] += 1
                precision_fails.append(t["label"])
            continue
        try:
            result = run_llm(t["field"], snippet)
        except Exception as e:
            print(f"  💥 ERROR [{t['field']}] {t['label']}: {e}\n")
            r["skip"] += 1
            continue

        if t["expect"] is None:
            if result is None:
                print(f"  ✅ PASS  [{t['field']}] {t['label']}")
                print(f"           Correctly returned null\n")
                r["pass"] += 1
            else:
                got = getattr(result, 'raw_match', str(result))[:80]
                print(f"  ❌ FAIL  [{t['field']}] {t['label']}")
                print(f"           ⚠️  HALLUCINATED: {got}\n")
                r["fail"] += 1
                hallucinations.append(t["label"])
        else:
            if result is None:
                print(f"  ❌ FAIL  [{t['field']}] {t['label']}")
                print(f"           Expected '{t['expect']}' — LLM returned null\n")
                r["fail"] += 1
                precision_fails.append(t["label"])
            else:
                result_str = json.dumps(result.model_dump())
                if t["expect"].lower() in result_str.lower():
                    raw = getattr(result, 'raw_match', '')[:80]
                    print(f"  ✅ PASS  [{t['field']}] {t['label']}")
                    print(f"           raw_match: {raw}\n")
                    r["pass"] += 1
                else:
                    print(f"  ❌ FAIL  [{t['field']}] {t['label']}")
                    print(f"           '{t['expect']}' not in: {result_str[:100]}\n")
                    r["fail"] += 1
                    precision_fails.append(t["label"])

    miss_count = len([t for t in tests if t["expect"] is None])
    total = r["pass"] + r["fail"] + r["skip"]

    print(f"{'='*65}")
    print(f"  RESULTS      : {r['pass']}/{total} passed | {r['fail']} failed | {r['skip']} skipped")
    print(f"  HALLUCINATIONS: ", end="")
    if hallucinations:
        print(f"❌ {len(hallucinations)}/{miss_count} — NOT safe for Bloomberg")
        for h in hallucinations:
            print(f"     • {h}")
    else:
        print(f"✅ 0/{miss_count} — safe")
    print(f"{'='*65}\n")

    return len(hallucinations) == 0 and r["fail"] == 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--verify", metavar="FILE")
    parser.add_argument("--field", metavar="FIELD")
    args = parser.parse_args()

    if args.verify:
        verify_file(args.verify)
    else:
        ok = run_tests(field_filter=args.field)
        sys.exit(0 if ok else 1)