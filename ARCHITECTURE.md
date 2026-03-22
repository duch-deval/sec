# Architecture Overview — Corporate Issuance Calendar Finder

> Auto-generated from source scan on 2026-02-11

---

## Project Summary

A data pipeline for discovering, classifying, downloading, and extracting structured data from SEC EDGAR corporate debt issuance filings (indentures). The system targets **6-K** and **8-K** filings, identifies exhibit documents (EX-4, EX-99), and extracts key financial and legal fields (CUSIP, ISIN, coupon rate, maturity date, governing law, etc.).

---

## Repository Layout

```
drexel-senior-project-2025-corpsissuance/
├── cal_finder/
│   ├── corpus_builder/          # Module 1: Data collection pipeline
│   │   ├── __init__.py          # Exports run_data_collection()
│   │   ├── __main__.py          # CLI entry point
│   │   ├── build_corpus.py      # Pipeline orchestrator (phases 1–4)
│   │   ├── sec_discovery.py     # Phase 1 — SEC EDGAR querying
│   │   ├── classification.py   # Phase 2 — Exhibit classification
│   │   ├── download.py          # Phase 3 — Document download
│   │   └── annotation.py       # Phase 4 — Annotation & filtering
│   │
│   └── extraction/              # Module 2: Field extraction
│       ├── __init__.py          # Exports run_pipeline()
│       ├── __main__.py          # CLI entry point (dual-mode)
│       ├── extract_fields.py    # Main extraction engine
│       ├── csv_xlsx_download.py # Bloomberg sample downloader
│       └── resources/
│           └── Mapping.xlsx     # Location → code mapping reference
│
├── Drexel Payment Calendar Examples_*/  # Sample data (Nov 2025, Feb 2026)
├── pyproject.toml               # Pylint configuration
├── requirements.txt             # Python dependencies
└── README
```

---

## Module 1: Corpus Builder

Orchestrated by `build_corpus.py`, the corpus builder runs a **four-phase sequential pipeline** for each target date (or date range):

```
┌──────────────────────────────────────────────────────────┐
│  Phase 1: SEC Discovery  (sec_discovery.py)              │
│  • Query SEC EDGAR Full-Text Search (FTS) API            │
│  • Parse filing indices (HTML & JSON)                    │
│  • Score & filter exhibits via regex heuristics           │
│  → filings.csv, exhibits.csv                             │
├──────────────────────────────────────────────────────────┤
│  Phase 2: Classification  (classification.py)            │
│  • Categorize each exhibit:                              │
│      ex4_indenture | ex99_indenture | ex4_warrant        │
│      ex99_uncertain | noise | other                      │
│  • Assign priority (1–3) and action (download/skip/hold) │
│  → exhibits_classified.csv                               │
├──────────────────────────────────────────────────────────┤
│  Phase 3: Download  (download.py)                        │
│  • Fetch HTML documents with exponential-backoff retry    │
│  • Resolve & download dependencies (images, CSS)         │
│  • Rewrite HTML paths to local references                │
│  → exhibits/{ex4, ex99, holding_queue}/*.html            │
├──────────────────────────────────────────────────────────┤
│  Phase 4: Annotation  (annotation.py)                    │
│  • Extract text preview (first 8 000 chars)              │
│  • Apply 29 reject patterns + 20 accept patterns         │
│  • Require trustee mention for acceptance                 │
│  • Move rejected docs to rejected/ folder                │
│  → candidates_for_extraction.csv, phase4_results.csv     │
└──────────────────────────────────────────────────────────┘
```

### CLI Usage

```bash
python -m corpus_builder 2025-12-01           # single date
python -m corpus_builder 2025-12-01..2025-12-31  # date range
```

---

## Module 2: Extraction

Takes the output of the corpus builder (or a Bloomberg sample spreadsheet) and extracts structured fields from each HTML indenture document.

### Dual-Mode CLI

```bash
python -m extraction 2025-12-01        # process corpus output for a date
python -m extraction sample.xlsx       # process Bloomberg sample file
```

### Extraction Engine (`extract_fields.py`)

| Component | Responsibility |
|---|---|
| `ExtractionConfig` (dataclass) | Loads `Mapping.xlsx`; compiles ~70 security regex patterns and CUSIP pattern |
| `FieldExtractor` | Stateless extractor with methods for each field type |

#### Extracted Fields

| Field | Method | Notes |
|---|---|---|
| Security Description | `extract_securities()` | 70 regex patterns for debt instrument formats |
| Coupon Rate | `parse_coupon_rate()` | Extracted from security description (%) |
| CUSIP | `extract_cusips()` | Handles dual CUSIPs |
| ISIN | `extract_isins()` | Standard ISIN pattern |
| Business Day Definition | `extract_business_day_definition()` | Section-level text extraction |
| Payment Locations | `extract_locations_from_definition()` | 40+ location patterns |
| Location Codes | `map_locations_to_codes()` | Via `Mapping.xlsx` |
| Governing Law | `extract_governing_law()` | Text, type, jurisdiction, ISO code |
| Maturity Date | `extract_maturity_date_from_text()` | Year or full date |
| Issue Size | `extract_issue_size()` | Principal amount |

### Output CSV Schema (17 columns)

```
Company, File Date, File Type, File Link, Exhibit,
Security Description, CUSIP, ISIN,
Coupon Rate, Issue Size, Maturity Date,
Text, Business Days - Standardized, Mapping,
Governing Law Text, Governing Law Type, Governing Law, Governing Law Code
```

---

## Data Flow

```
                     SEC EDGAR FTS API
                           │
                    ┌──────▼──────┐
                    │  sec_discovery │  Phase 1
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │ classification │  Phase 2
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │   download    │  Phase 3
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │  annotation   │  Phase 4
                    └──────┬──────┘
                           │
          ┌────────────────▼────────────────┐
          │                                 │
   Corpus Output                   Bloomberg Sample
   (date folder)                   (CSV / XLSX)
          │                                 │
          │         ┌───────────┐           │
          └────────►│ extraction │◄──────────┘
                    └─────┬─────┘
                          │
                    Standardized CSV
```

---

## External Dependencies

| Package | Version | Purpose |
|---|---|---|
| `requests` | >= 2.31.0 | HTTP requests to SEC EDGAR |
| `beautifulsoup4` | >= 4.12.0 | HTML parsing & text extraction |
| `openpyxl` | (implicit) | Excel file reading (`Mapping.xlsx`, samples) |

---

## External Services & APIs

| Service | Endpoint | Usage |
|---|---|---|
| SEC EDGAR FTS | `https://efts.sec.gov/LATEST/search-index` | Full-text search for filings |
| SEC EDGAR Web | `https://www.sec.gov/Archives/edgar/data/...` | Filing index pages & document download |

### Rate Limiting

- **FTS API**: 0.25 s between pages (`PAGE_SLEEP`)
- **Document download**: 0.15 s between requests (`REQUEST_DELAY`)
- **Retry**: Exponential backoff, max 3 attempts

### User Agent

Configured via `SEC_USER_AGENT` env var. Default: `"Bloomberg-Drexel-Capstone (dhd37@drexel.edu)"`

---

## Key Design Decisions

1. **Phased pipeline** — Each phase writes intermediate CSVs, enabling re-runs from any phase and debugging
2. **Pattern-based classification** — Regex heuristics over ML for exhibit classification, avoiding training data requirements
3. **Dual-input extraction** — Same extraction logic handles both internally-collected corpus and externally-provided Bloomberg samples
4. **Graceful degradation** — Pipeline continues even if individual phases fail; missing modules are logged and skipped
5. **Local asset rewriting** — Downloaded HTML has asset paths rewritten to local copies, enabling offline extraction
6. **Frozen configuration** — `ExtractionConfig` is a frozen dataclass, ensuring patterns are compiled once and shared immutably

---

## Output Artifacts (per date)

```
YYYY-MM-DD/
├── asset/
│   ├── filings.csv                  # Phase 1 output
│   ├── exhibits.csv                 # Phase 1 output
│   ├── exhibits_classified.csv      # Phase 2 output
│   ├── candidates_for_extraction.csv # Phase 4 output
│   └── phase4_results.csv           # Phase 4 detailed results
├── exhibits/
│   ├── ex4/                         # High-priority indentures
│   ├── ex99/                        # Secondary indentures
│   └── holding_queue/               # Uncertain documents
├── rejected/                        # Phase 4 rejected docs
└── YYYY-MM-DD_raw.csv              # Consolidated raw output
```
