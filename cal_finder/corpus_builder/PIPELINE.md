# Corpus Builder Pipeline

Four-phase pipeline that discovers, classifies, downloads, and filters SEC EDGAR indenture filings (6-K, 8-K) on a per-date basis.

```
SEC EDGAR FTS API
       |
  Phase 1: SEC Discovery        -> filings.csv, exhibits.csv
       |
  Phase 2: Classification       -> exhibits_classified.csv
       |
  Phase 3: Download             -> exhibits/{ex4, ex99, holding_queue}/
       |
  Phase 4: Annotation           -> candidates_for_extraction.csv
       |
  Cleanup                       -> {date}_raw.csv (final)
```

## CLI

```bash
python -m corpus_builder 2025-12-01               # single date
python -m corpus_builder 2025-12-01..2025-12-31    # date range
```

Each date gets its own directory. The pipeline runs all four phases per date, then cleans up intermediate artifacts if all phases succeed.

---

## Phase 1: SEC Discovery (`sec_discovery.py`)

Queries the SEC EDGAR Full-Text Search API and extracts exhibit metadata from filing index pages.

### FTS Query

- **Endpoint:** `https://efts.sec.gov/LATEST/search-index`
- **Forms:** `6-K`, `8-K` (configurable)
- **Pagination:** Batches of 100, 0.25s between pages
- **Retry:** Up to 5 attempts with exponential backoff (0.5s base, max 16s)

### Filing Metadata Extraction

Each FTS hit yields: `cik`, `accession`, `form`, `filed`, `company`. The function tries multiple key names per field to handle API response variations.

### Exhibit Extraction

For each filing, the pipeline fetches the index page (`{adsh}-index.html`) and parses its HTML table (falls back to JSON if HTML fails). Each row contains a document path, type, and description.

### Exhibit Scoring (mode="both")

Documents are matched against regex patterns for EX-4 and EX-99 type/filename detection:

| Condition | Score | Action |
|---|---|---|
| EX-4 match | 10 | Keep |
| EX-99 + indenture signal keywords | 8 | Keep |
| EX-99 + filing also has EX-4 | 6 | Keep |
| EX-99 + no strong signals | 4 | Keep (candidate) |
| EX-99 + noise keywords (press release, earnings, etc.) | - | Skip |

**Indenture signal keywords:** supplemental indenture, base indenture, trust indenture, senior notes, subordinated notes, debentures, debt securities, guarantee, exchange offer, consent solicitation, redemption notice, etc.

**Noise keywords:** press release, earnings release, earnings call, investor presentation, transcript, financial results, etc.

### Asset Detection

For HTML documents, the pipeline counts image/CSS assets (`.jpg`, `.png`, `.gif`, `.css`, `.svg`, `.webp`, `.bmp`) referenced in the filing index to flag image-heavy documents.

### Output

- `asset/filings.csv` -- one row per filing
- `asset/exhibits.csv` -- one row per exhibit with columns: `cik`, `accession`, `form`, `filed`, `company`, `exhibit_index_url`, `doc_url`, `doc_name`, `doc_description`, `doc_type`, `match_score`, `matched_by`, `filing_has_ex4`, `has_dependencies`, `asset_count`

---

## Phase 2: Classification (`classification.py`)

Categorizes each exhibit from Phase 1 using keyword matching against the combined text of `doc_type`, `doc_name`, and `doc_description`.

### Exhibit Number Parsing

Extracts `(major, minor)` from `doc_type`, e.g. `EX-4.1` -> `(4, 1)`, `EX-99` -> `(99, 0)`.

### Keyword Lists

| List | Purpose | Examples |
|---|---|---|
| `STRONG_INDENTURE_KW` | High-confidence indenture signals | supplemental indenture, base indenture, trust indenture |
| `INDENTURE_KW` | General indenture signals | indenture, senior notes, trustee, paying agent, global note |
| `WARRANT_KW` | Warrant detection | warrant, warrant agreement, warrant agent |
| `NOISE_KW` | Noise filtering | press release, earnings call, investor presentation, transcript |
| `CONTEXTUAL_KW` | Weak indenture signals | redemption, consent, amendment, maturity, coupon, noteholder |

### Classification Rules

Evaluated top-to-bottom, first match wins:

| Rule | Category | Action | Priority |
|---|---|---|---|
| Noise keywords detected | `noise` | `skip` | 999 |
| EX-4 + warrant keywords | `ex4_warrant` | `holding_queue` | 10 |
| EX-4 (no warrant) | `ex4_indenture` | `download` | 1 |
| EX-99.1 on 8-K filing | `ex99_8k_excluded` | `skip` | 999 |
| EX-99 + strong indenture keywords | `ex99_indenture` | `download` | 2 |
| EX-99 + indenture keywords | `ex99_indenture` | `download` | 3 |
| EX-99 + contextual keywords | `ex99_uncertain` | `holding_queue` | 5 |
| EX-99 + no keyword signals | `ex99_uncertain` | `holding_queue` | 6 |
| Other exhibit type | `other` | `skip` | 999 |

**Image detection:** Documents with >= 50 referenced assets are flagged as `is_image_based = "yes"`.

### Output

- `asset/exhibits_classified.csv` -- original columns plus `category`, `download_action`, `confidence`, `reason`, `priority`, `is_image_based`

---

## Phase 3: Download (`download.py`)

Downloads the HTML/PDF documents and their embedded assets (images, CSS) from SEC EDGAR.

### Download Filtering

Only documents matching these criteria are fetched:

| Action | Allowed Categories |
|---|---|
| `download` | `ex4_indenture`, `ex99_indenture` |
| `holding_queue` | `ex4_warrant`, `ex99_uncertain` |

### HTTP Configuration

- **Request delay:** 0.15s between requests
- **Timeout:** 30s
- **Retry:** Up to 3 attempts with exponential backoff (1s base)
- **User-Agent:** `SEC_USER_AGENT` env var, default `"Bloomberg-Drexel-Capstone (dhd37@drexel.edu)"`

### Document Processing

1. **Fetch** the document HTML/PDF
2. **For image-based HTML documents:** parse the HTML for `<img src>` tags and CSS `url()` references, download each asset, save into a `{docname}_files/` subfolder
3. **Rewrite HTML paths** so `<img src>` attributes point to the local assets folder (`./docname_files/filename.jpg`)
4. **Save** to the appropriate folder with collision handling (`name.htm`, `name_1.htm`, `name_2.htm`, ...)

### Output Directory

```
exhibits/
  ex4/              <- ex4_indenture documents
  ex99/             <- ex99_indenture and ex99_uncertain documents
  holding_queue/    <- ex4_warrant documents
```

### Return Value

Returns `False` if any download failed, `True` otherwise.

---

## Phase 4: Annotation (`annotation.py`)

Final content-based filter. Reads the actual HTML text of each downloaded document and applies pattern-based scoring to separate real indentures from false positives.

### Text Extraction

Reads the first 8,000 characters of each `.htm`/`.html` file, strips HTML tags, and collapses whitespace.

### Hard Reject Patterns (34 patterns)

Checked against uppercased text. If any match, the document is immediately rejected with `confidence = "high"`. Categories include:

| Category | Example Patterns |
|---|---|
| `warrant` | COMMON STOCK PURCHASE WARRANT, PRE-FUNDED WARRANT, WARRANT TO PURCHASE, THIS WARRANT CERTIFIES |
| `warrant_amendment` | AMENDMENT TO WARRANT |
| `promissory_note` | PROMISSORY NOTE (excluding PROMISSORY NOTE INDENTURE) |
| `purchase_agreement` | PRE-PAID ADVANCE AGREEMENT, PREPAID PURCHASE |
| `reinvestment_plan` | DISTRIBUTION REINVESTMENT PLAN |
| `redemption_program` | SHARE REDEMPTION PROGRAM |
| `cmbs_psa` | POOLING AND SERVICING AGREEMENT |
| `deposit_agreement` | DEPOSIT AGREEMENT |
| `no_indenture_note` | HAS NOT ENTERED INTO AN INDENTURE |
| `rights_agreement` | SHAREHOLDER RIGHTS AGREEMENT |
| `preferred_stock` | CERTIFICATE OF DESIGNATION(S) + PREFERRED |
| `stock_exchange_announcement` | VOLUNTARY ANNOUNCEMENT, STOCK EXCHANGE OF HONG KONG |
| `cmbs_tsa` | TRUST AND SERVICING AGREEMENT |

### Accept Pattern Scoring (22 patterns)

Each pattern has an associated weight. All matches are summed to produce an accept score.

| Weight | Patterns |
|---|---|
| **15** | "as Trustee", "as Indenture Trustee", "Trustee and Collateral Agent", BANK/TRUST + Trustee |
| **12** | SUPPLEMENTAL INDENTURE, BASE INDENTURE |
| **10** | "the Trustee", "the Indenture Trustee", "as trustee under", TRUST INDENTURE, comma + Trustee |
| **8** | INDENTURE dated, GLOBAL SECURITY, INDENTURE HEREINAFTER REFERRED TO |
| **6** | Senior/Subordinated/Secured Notes due 20XX |
| **4** | aggregate principal amount, Events of Default, REGISTERED IN THE NAME OF, CUSIP No./Number |
| **3** | Paying Agent |

### Decision Logic

```
ACCEPT_THRESHOLD = 20
TRUSTEE_REQUIRED = True
```

| Condition | Result | Confidence |
|---|---|---|
| Text unreadable | Rejected | high |
| Hard reject pattern matched | Rejected | high |
| No trustee reference found | Rejected | medium |
| Score < 20 | Rejected | low |
| Score >= 20, trustee found | **Accepted** | high (score >= 30) or medium |

### Metadata Enrichment

Accepted documents are matched back to `exhibits_classified.csv` metadata to populate: company name, file date, form type, file link, exhibit number. Collision-renamed files (e.g. `ex4-1_2.htm`) are resolved back to their original name for lookup.

### File Operations

- **Rejected documents** are moved from `exhibits/` to `rejected/`
- **Accepted documents** stay in `exhibits/ex4/` or `exhibits/ex99/`

### Output

- `asset/candidates_for_extraction.csv` -- accepted documents with metadata columns: `Company`, `File Date`, `File Type`, `File Link`, `Exhibit`, `Security Description`, `CUSIP`, `ISIN`, `Text`, `Business Days - Standardized`, `Mapping`, plus internal fields `_local_path`, `_category`, `_accept_score`
- `asset/phase4_results.csv` -- all documents with classification details and score breakdowns

---

## Cleanup

After all four phases succeed for a date, `cleanup()` runs:

1. Renames `asset/candidates_for_extraction.csv` to `{date}_raw.csv` in the root date directory
2. Deletes intermediate CSVs (`filings.csv`, `exhibits.csv`, `exhibits_classified.csv`, `phase4_results.csv`)
3. Removes the `asset/` and `rejected/` directories
4. Removes `exhibits/holding_queue/` if empty

### Final Directory Structure

```
{date}/
  {date}_raw.csv
  exhibits/
    ex4/*.htm
    ex99/*.htm
    holding_queue/       (if non-empty)
```

---

## Error Handling

Each phase is wrapped in a try/except in the orchestrator (`build_corpus.py`). If any phase raises an exception or returns `False`, the pipeline stops for that date (cleanup is skipped) and moves to the next date. Individual document failures within a phase are handled gracefully -- the pipeline continues processing remaining documents.

## Configuration

| Setting | Location | Default |
|---|---|---|
| Form types | `build_corpus.py` config dict | `["6-K", "8-K"]` |
| User agent | `SEC_USER_AGENT` env var | `"Bloomberg-Drexel-Capstone (dhd37@drexel.edu)"` |
| Accept threshold | `annotation.py` | 20 |
| Trustee required | `annotation.py` | True |
| Image asset threshold | `classification.py` | 50 |
| FTS page sleep | `sec_discovery.py` | 0.25s |
| Download request delay | `download.py` | 0.15s |
