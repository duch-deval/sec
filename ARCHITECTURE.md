# Architecture Overview — Corporate Issuance Calendar Finder

**Last updated**: April 1, 2026
**Status**: Spring 2026 — Phase 3 complete, Phase 4 in progress

---

## Project Summary

A hybrid regex + LLM extraction pipeline that processes SEC EDGAR corporate bond indenture exhibits and outputs structured fields into a Supabase database and 19-column Bloomberg-formatted CSV. Bloomberg provides curated exhibit URLs from their internal data feed; the pipeline extracts payment calendar, governing law, and bond terms fields to automate Natasha and Justin's manual extraction work.

---

## Repository Layout

```
duch-deval/sec/
├── cal_finder/
│   ├── corpus_builder/              # Development tool only — NOT a deliverable
│   │   ├── __main__.py              # CLI: python -m corpus_builder YYYY-MM-DD
│   │   ├── build_corpus.py          # Pipeline orchestrator (phases 1–4)
│   │   ├── sec_discovery.py         # Phase 1 — SEC EDGAR FTS API querying
│   │   ├── classification.py        # Phase 2 — Exhibit classification
│   │   ├── download.py              # Phase 3 — Document download
│   │   └── annotation.py            # Phase 4 — Annotation & filtering
│   │
│   └── extraction/                  # PRIMARY DELIVERABLE
│       ├── __main__.py              # CLI: python -m extraction YYYY-MM-DD
│       ├── extract_fields.py        # Layer 1: Regex extraction engine (1100+ lines)
│       ├── llm_fallback.py          # Layer 2: LLM fallback (Claude Haiku via LiteLLM)
│       ├── models.py                # Pydantic validators for LLM outputs
│       ├── test_prompts.py          # Test harness — 15/15 passing, 0 hallucinations
│       └── prompts/                 # Prompt files — Bloomberg deliverable + runtime
│           ├── issue_size.md
│           ├── maturity_date.md
│           └── payment_calendar.md
│
├── .env                             # SUPABASE_URL, ANTHROPIC_API_KEY, LLM_FALLBACK_ENABLED
├── requirements.txt
└── pyproject.toml
```

---

## High-Level Architecture

```
Bloomberg Internal Data Feed
(curated SEC exhibit URLs)
          │
          ▼
┌─────────────────────────┐
│   Layer 1 — Regex        │  extract_fields.py
│   BeautifulSoup + 70+    │  Deterministic, zero cost
│   regex patterns         │  Handles 90–100% per field
└──────────┬──────────────┘
           │
     field empty or
     known-weak result?
           │ YES (field-specific gate)
           ▼
┌─────────────────────────┐
│   Layer 2 — LLM Fallback │  llm_fallback.py
│   Claude Haiku via       │  T=0.0, Instructor + Pydantic
│   LiteLLM               │  Only 3 target fields
│   Snippet ≤1500 chars    │  raw_match mandatory
└──────────┬──────────────┘
           │
     passes Pydantic
     validation?
     ┌─────┴─────┐
    YES          NO
     │            │
     ▼            ▼
  Output       Blank field
   row         (never wrong
               value)
     │
     ▼
┌─────────────────────────┐
│   Supabase              │  bloomberg.extractions table
│   + CSV/XLSX output     │  27 columns including LLM tracking
└─────────────────────────┘
```

---

## Layer 1 — Regex Extraction Engine (`extract_fields.py`)

### Key Components

| Component | Type | Responsibility |
|---|---|---|
| `ExtractionConfig` | frozen dataclass | Loads `Mapping.xlsx`, compiles ~70 security patterns, CUSIP pattern |
| `FieldExtractor` | class | Stateless extractor — one instance per document |
| `run_pipeline()` | function | Orchestrates full extraction for a date folder |
| `read_html()` | function | BeautifulSoup HTML → clean text |
| `normalize_text()` | function | Normalizes smart quotes, `&nbsp;`, whitespace |
| `EXPORT_COLUMNS` | constant | 19-column Bloomberg output schema |

### Field Extraction Methods

| Field | Method | Accuracy | Notes |
|---|---|---|---|
| Security Description | `extract_securities()` | ~100% | 70 regex patterns for debt instrument formats |
| Coupon Rate | `parse_coupon_rate()` | ~92% | Derived from security description |
| CUSIP | `extract_cusips()` | ~94% | Luhn mod-10 checksum validation |
| ISIN | `extract_isins()` | ~90% | Format validation (2-char country + 10 chars) |
| Business Day Text | `extract_business_day_definition()` | ~90% | Section-level text extraction, 800 char limit |
| BD Locations | `extract_locations_from_definition()` | ~47%* | 40+ city/system patterns + fallback |
| CDR Codes | `map_locations_to_codes()` | — | Via `Mapping.xlsx` (32 mappings) |
| Governing Law | `extract_governing_law()` | ~98% | Text, type, jurisdiction, ISO 3166 code |
| Maturity Date | `parse_maturity_date()` + `extract_maturity_date_from_text()` | ~88% | Year-only or full date |
| Issue Size | `extract_issue_size()` | ~65%* | Multi-tier pattern matching |
| Base Indenture Ref | `detect_base_indenture_reference()` | ~95% | Flags supplemental indentures |

*Coverage depressed by corpus composition (~62% supplementals). Real accuracy on Bloomberg's curated input is materially higher.

### Multi-Series Row Splitting

When a single document contains multiple bond series, `run_pipeline()` splits into one row per series (lines 1042–1066). Each row gets its own security description, CUSIP, ISIN, coupon rate, and maturity date. BD text, locations, governing law, and issue size are shared across series from the same document.

### Master Indenture Post-Processing

If a single file generates >10 rows with identical issue_size, the issue_size is nulled out. Catches master indentures (e.g. Duke Energy Florida, 43 legacy series) where a trust balance was incorrectly being used as per-series issue size.

### Key Regex Patterns (Maturity Date)

```python
# Full date patterns — tried in order
rf'(?:Stated\s+)?Maturity\s+Date["\s:]*...'         # "Maturity Date: May 1, 2031"
rf'matur(?:ing|e|es)\s+(?:on\s+)?...'              # "maturing on May 1, 2031"
rf'\bdue\s+({_MONTH}...)'                           # "due May 1, 2031"
rf'(?:Maturity\s+Date|mature[sd]?)\b...\bon\s+...'  # "Maturity Date...on May 1, 2031"
rf'principal\b[^.\n]{0,80}\bpayable\s+on\s+...'    # "principal...payable on May 1, 2031"

# Year-only fallback (triggers LLM gate)
rf'[Dd]ue\s+({_MONTH}\s+\d{1,2},?\s+20\d{2})'     # from security description
```

---

## Layer 2 — LLM Fallback (`llm_fallback.py`)

### Design Principles

- **Only 3 fields** have LLM fallback — issue size, maturity date, BD by reference
- **Temperature 0.0** — no sampling randomness
- **Instructor + Pydantic** — constrained structured output only, never free-form
- **`raw_match` mandatory** — LLM must return verbatim substring of input snippet
- **Snippet only** — max 1500 chars sent to LLM, never full document
- **`sleep(3)`** between all LLM calls to respect rate limits

### LLM Trigger Gates

| Field | Gate Condition | Why |
|---|---|---|
| Issue size | `extract_issue_size()` returns `None` | Complex formatting, bridged amounts |
| Maturity date | Regex returns year-only (4-digit), NOT empty | Full date exists in body but not at standard position |
| BD by reference | BD text contains "Legal Holiday" with no city resolved | **NOT YET WIRED** (Bug 7) |

### Maturity Date Gate Logic

```python
# Only fire LLM if full date might exist in body
_body_pos = text.find(_yr, 200)  # search from pos 200, skip title area
if _body_pos == -1:
    llm_result = None  # year only in title, no body hit
else:
    _snip = text[max(0, _body_pos - 400): _body_pos + 400]
    llm_result = extract_maturity_date(_snip)
```

### Snippet Cleaner

`_clean_maturity_snippet()` strips `"Dated as of Month DD, YYYY"` lines before sending to LLM — prevents model grabbing indenture execution date instead of maturity date.

### Prompt Files (`extraction/prompts/*.md`)

Prompt files serve dual purpose:
1. **Runtime** — loaded and markdown-stripped by `llm_fallback.py`, injected as system prompt
2. **Bloomberg deliverable** — human-readable business rules Natasha/Justin can review and Bloomberg can use with any LLM

```
prompts/
├── issue_size.md        # Business rules: no minimum, all currencies, reject "at least"
├── maturity_date.md     # Business rules: full date preferred, year-only rejected
└── payment_calendar.md  # Business rules: city extraction, CDR mapping, TARGET handling
```

### Model Roadmap

| Phase | Model | Cost | Status |
|---|---|---|---|
| Prototype (now) | Claude Haiku via LiteLLM | $3.30 spent, ~$1.70 remaining | Active |
| Week 8 delivery | NuExtract via Ollama | Free, local, MIT licensed | Planned |

LiteLLM ensures zero code changes on model swap — only the model string in `.env` changes.

---

## Pydantic Validation Models (`models.py`)

Every LLM output is validated before reaching the database. Invalid outputs are silently discarded — pipeline returns blank, never a wrong value.

| Model | Key Validators |
|---|---|
| `IssueSizeExtraction` | `amount > 0`, `amount >= 100,000`, no bracket placeholders, `raw_match` contains currency/number |
| `MaturityDateExtraction` | 4-digit year present, no coupon rate in `date_str`, `due` + year-only rejected, `raw_match` verified |
| `BDReferenceExtraction` | location < 60 chars, no BD clause words, no placeholders, `raw_match` is substring of snippet |

---

## Supabase Integration

**Project**: `xkwforssmtaeesmkckod`
**Schema**: `bloomberg`
**Table**: `extractions` (27 columns)

### Key Columns Beyond 19-Column Schema

| Column | Purpose |
|---|---|
| `llm_used` | Boolean — whether LLM fired for this row |
| `llm_field` | Which field LLM extracted (`issue_size`, `maturity_date`, `bd_by_reference`) |
| `llm_raw_match` | Verbatim substring LLM matched — verifiable against source |
| `review_required` | Flag for human review queue |
| `updated_at` | Timestamp for corpus run isolation queries |

### Current State (April 1, 2026)

| Corpus | Rows | Notes |
|---|---|---|
| Feb 2026 | 194 | LLM-enabled |
| March 2026 | 357 | LLM-enabled, 02–31 (missing 28/29) |
| **Total** | **551** | |

---

## 19-Column Output Schema

```python
EXPORT_COLUMNS = [
    "Company",                      # Issuer name
    "File Date",                    # SEC filing date
    "File Type",                    # 8-K or 6-K
    "File Link",                    # SEC EDGAR exhibit URL
    "Exhibit",                      # Exhibit number (4.1, 4.2, etc.)
    "Security Description",         # extract_securities() — 70 patterns
    "CUSIP",                        # extract_cusips() — Luhn validated
    "ISIN",                         # extract_isins()
    "Coupon Rate",                  # parse_coupon_rate() — from sec desc
    "Issue Size",                   # extract_issue_size() + LLM fallback
    "Maturity Date",                # parse_maturity_date() + LLM fallback
    "Text",                         # BD definition raw text
    "Business Days - Standardized", # Semicolon-separated city/system names
    "Mapping",                      # Semicolon-separated CDR codes
    "Governing Law Text",           # Raw governing law clause
    "Governing Law Type",           # Terms/Subordination/Collateral/Disposition
    "Governing Law",                # Normalized jurisdiction name
    "Governing Law Code",           # ISO 3166 code (US-NY, GB, CA-ON, etc.)
    "Base Indenture Reference",     # Supplemental flag + base indenture date
]
```

**Column ownership:**
- **Natasha** (payment calendar): Company → ISIN + Text → Mapping
- **Justin** (governing law + bond terms): Coupon Rate → Base Indenture Reference

---

## CLI Usage

```bash
# Corpus builder (development only)
python -m corpus_builder 2026-03-01            # single date
python -m corpus_builder 2026-03-01..2026-03-31  # date range

# Extraction
python -m extraction 2026-03-01                # single date
python -m extraction 2026-03-01 2026-03-31     # date range

# Test harness
python3 -m extraction.test_prompts             # all 15 cases
python3 -m extraction.test_prompts --field payment_calendar
python3 -m extraction.test_prompts --verify path/to/file.htm

# Regex-only run (no API cost)
LLM_FALLBACK_ENABLED=false python -m extraction 2026-03-01
```

---

## Corpus Builder (Development Tool)

**Not a deliverable.** Bloomberg feeds the pipeline clean, curated URLs from their internal data feed. The corpus builder simulates that feed for development and testing only.

```
Phase 1: SEC Discovery    sec_discovery.py   → filings.csv, exhibits.csv
Phase 2: Classification   classification.py  → exhibits_classified.csv
Phase 3: Download         download.py        → exhibits/ex4/*.htm
Phase 4: Annotation       annotation.py      → candidates_for_extraction.csv
```

### Annotation Reject Patterns (Key Classes)

| Pattern | Category | Description |
|---|---|---|
| `APPENDIX\s+2A` | asx_disclosure | ASX securities quotation notices |
| `ORDINARY\s+FULLY\s+PAID` | asx_equity | ASX equity shares |
| `BLANK\s+CHECK\s+COMPANY` | spac_trust | SPAC trust account filings |
| `\d{2,3}(?:ST\|ND\|RD\|TH)\s+SUPPLEMENTAL\s+INDENTURE` | sce_mortgage | SCE numbered mortgage collateral |
| `CASH\s+AND\s+INVESTMENTS\s+HELD\s+IN\s+TRUST` | spac_trust | SPAC trust accounts |

---

## External Dependencies

| Package | Purpose |
|---|---|
| `beautifulsoup4` | HTML parsing and text extraction |
| `requests` | HTTP to SEC EDGAR |
| `openpyxl` | Mapping.xlsx + Excel output |
| `instructor` | Pydantic enforcement over LLM outputs |
| `litellm` | Model-agnostic LLM calls (swap Haiku → NuExtract without code changes) |
| `pydantic` | Field validation schemas with `raw_match` verification |
| `supabase` | Database write integration |
| `python-dotenv` | `.env` loading |

---

## Key Design Decisions

1. **Regex-first, LLM fallback only on 3 fields** — LLM fires on issue size, maturity date, and BD-by-reference only. All other fields are at or above threshold.
2. **Temperature 0.0 always** — no sampling randomness in LLM calls.
3. **`raw_match` mandatory** — every LLM extraction must return verbatim source text. If not a substring of input, output is discarded.
4. **Blank over wrong** — pipeline returns blank rather than a confident wrong answer. Never accept hallucinated data.
5. **Prompts as deliverables** — `.md` prompt files are simultaneously runtime components and Bloomberg deliverables. Bloomberg can swap the LLM; prompts stay valid.
6. **Snippet only** — max 1500 chars per LLM call. Never full document.
7. **Supplementals flagged, not failed** — `Base Indenture Reference` column flags supplemental indentures where fields live in the base document. Blank fields on supplementals are expected behavior.
8. **Model-agnostic via LiteLLM** — switching from Claude Haiku to NuExtract (Week 8) requires only a model string change in `.env`.
9. **No issue size minimum** — Bloomberg confirmed no floor. `val > 0` only.
10. **Corpus builder noise ≠ pipeline failure** — Bloomberg's feed is clean. Coverage metrics on dev corpus are a lower bound.
