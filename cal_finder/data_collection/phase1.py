#!/usr/bin/env python3
import csv
import os
import random
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin, urlparse, urlunparse
import logging

import requests

logger = logging.getLogger(__name__)

try:
    from bs4 import BeautifulSoup
except ImportError:
    logger.error("ERROR: BeautifulSoup required.")
    sys.exit(1)

UA = os.getenv("SEC_USER_AGENT", "Bloomberg-Drexel-Capstone (dhd37@drexel.edu)")
FTS_BASE = "https://efts.sec.gov/LATEST/search-index"
PAGE_SLEEP = 0.25
RETRY_MAX = 5
RETRY_BASE_DELAY = 0.5

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": UA,
        "Accept": "application/json,text/html,*/*",
        "Referer": "https://www.sec.gov/edgar/search/",
    }
)

EX4_TYPE_RE = re.compile(
    r"^EX\s*[-_ ]?\s*4(?:[.\-]?\d+)?(?:[A-Za-z()\-/\s]*)?$", re.IGNORECASE
)
EX99_TYPE_RE = re.compile(
    r"^EX\s*[-_ ]?\s*99(?:[.\-]?\d+)?(?:[A-Za-z()\-/\s]*)?$", re.IGNORECASE
)
EX4_NAME_RE = re.compile(
    r"(?:^|/)[^/]*ex[-_. ]?4(?:[.-]?\d+)?\.(?:htm|html|pdf|txt)$", re.IGNORECASE
)
EX99_NAME_RE = re.compile(
    r"(?:^|/)[^/]*ex[-_. ]?99(?:[.-]?\d+)?\.(?:htm|html|pdf|txt)$", re.IGNORECASE
)
INDENTURE_HINT_RE = re.compile(
    r"(?i)\b("
    r"supplemental\s+indenture"
    r"|base\s+indenture"
    r"|indenture(?:\s+no\.?\s*\d+)?"
    r"|trust\s+indenture"
    r"|senior\s+notes?"
    r"|subordinated\s+notes?"
    r"|debentures?"
    r"|debt\s+securities?"
    r"|guarantee(?:d)?\s+(?:senior|subordinated|notes?|debentures?)"
    r"|guaranty\s+agreement"
    r"|exchange\s+offer"
    r"|offer\s+to\s+exchange"
    r"|consent\s+solicitation"
    r"|notice\s+of\s+redemption"
    r"|redemption\s+notice"
    r")\b"
)
NEG_NOISE_RE = re.compile(
    r"(?i)\b("
    r"press\s+release"
    r"|earnings\s+(?:release|call)"
    r"|slides?"
    r"|presentation"
    r"|investor\s+(?:presentation|slides)"
    r"|transcript"
    r"|financial\s+(?:results|statements?)"
    r")\b"
)


def _get_json_with_retry(params: Dict[str, str]) -> Dict[str, Any]:
    delay = RETRY_BASE_DELAY
    last_err = None

    for attempt in range(1, RETRY_MAX + 1):
        try:
            r = SESSION.get(FTS_BASE, params=params, timeout=30)

            if r.status_code == 200:
                ct = r.headers.get("Content-Type", "")
                if "application/json" in ct:
                    return r.json()

            if r.status_code in (429, 500, 502, 503, 504):
                last_err = f"HTTP {r.status_code}"
                jitter = random.uniform(0, delay * 0.3)
                time.sleep(delay + jitter)
                delay = min(delay * 2, 16.0)
                continue

            r.raise_for_status()
            return r.json()

        except requests.exceptions.Timeout:
            last_err = "Timeout"
            jitter = random.uniform(0, delay * 0.3)
            time.sleep(delay + jitter)
            delay = min(delay * 2, 16.0)

        except Exception as e:
            last_err = str(e)
            time.sleep(delay)
            delay = min(delay * 2, 16.0)

    raise RuntimeError(f"FTS API failed after {RETRY_MAX} retries: {last_err}")


def _get_with_retry(url: str, accept_json: bool = False) -> requests.Response:
    delay = RETRY_BASE_DELAY
    last_err = None

    for attempt in range(1, RETRY_MAX + 1):
        try:
            r = SESSION.get(url, timeout=45)

            if r.status_code == 200:
                if accept_json:
                    ct = r.headers.get("Content-Type", "")
                    if "json" in ct.lower():
                        return r
                else:
                    return r

            if r.status_code in (429, 500, 502, 503, 504):
                last_err = f"HTTP {r.status_code}"
                time.sleep(delay)
                delay = min(delay * 2, 8.0)
                continue

            r.raise_for_status()
            return r

        except Exception as e:
            last_err = str(e)
            if attempt < RETRY_MAX:
                time.sleep(delay)
                delay = min(delay * 2, 8.0)

    raise RuntimeError(f"Fetch failed after {RETRY_MAX} attempts: {url} ({last_err})")


def _normalize_url(url: str) -> str:
    p = urlparse(url)
    p2 = p._replace(
        params="",
        query="",
        fragment="",
        scheme=p.scheme.lower(),
        netloc=p.netloc.lower(),
    )
    return urlunparse(p2)


def _build_abs_url(base_dir: str, href: str) -> str:
    return urljoin(base_dir if base_dir.endswith("/") else base_dir + "/", href)


def _to_base_dir(cik: str, adsh: str) -> str:
    cik_nolead = str(int(cik))
    adsh_nodash = adsh.replace("-", "")
    return f"/Archives/edgar/data/{cik_nolead}/{adsh_nodash}"


def _alt_json_url(index_url: str) -> Optional[str]:
    if index_url.lower().endswith(".html"):
        return index_url[:-5] + ".json"
    if index_url.lower().endswith(".htm"):
        return index_url[:-4] + ".json"
    return None


def query_fts(
    start_date: str,
    end_date: str,
    forms: Tuple[str, ...] = ("6-K", "8-K"),
    query: str = "",
) -> Iterable[Dict[str, Any]]:

    forms_csv = ",".join(f.strip() for f in forms if f.strip())
    offset = 0
    batch_size = 100
    consecutive_failures = 0

    while consecutive_failures < 3:
        params = {
            "dateRange": "custom",
            "category": "custom",
            "forms": forms_csv,
            "startdt": start_date,
            "enddt": end_date,
            "from": str(offset),
            "size": str(batch_size),
        }
        if query.strip():
            params["q"] = query

        try:
            data = _get_json_with_retry(params)
            consecutive_failures = 0

        except Exception:
            consecutive_failures += 1
            if consecutive_failures >= 3:
                break
            offset += batch_size
            time.sleep(2.0)
            continue

        hits = None
        if isinstance(data, dict):
            if (
                "hits" in data
                and isinstance(data["hits"], dict)
                and "hits" in data["hits"]
            ):
                hits = data["hits"]["hits"]
            elif "results" in data and isinstance(data["results"], list):
                hits = data["results"]

        if not hits:
            break

        for hit in hits:
            yield hit

        if len(hits) < batch_size:
            break

        offset += batch_size
        time.sleep(PAGE_SLEEP)


def extract_filing_meta(hit: Dict[str, Any]) -> Tuple[str, str, str, str, str]:
    src = hit.get("_source", hit)

    def pick(*keys):
        for k in keys:
            v = src.get(k)
            if v:
                return v
        return None

    cik = pick("cik", "CIK", "ciks", "cik_str")
    if isinstance(cik, list):
        cik = cik[0]
    if cik and isinstance(cik, int):
        cik = str(cik)

    adsh = pick("adsh", "accessionNo", "accession_no", "accessionNumber", "accession")
    form = pick("formType", "form", "form_type")
    filed = pick("filedAt", "filed", "file_date", "filed_date")
    company = pick("display_names", "companyName", "company_name", "entity")

    if isinstance(company, list):
        company = company[0]

    return (cik or "", adsh or "", form or "", filed or "", company or "")


def parse_index_table(html: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    rows: List[Dict[str, str]] = []

    def clean(text):
        return (text or "").strip().replace("\xa0", " ")

    for tbl in tables:
        headers = [clean(th.get_text()) for th in tbl.find_all("th")]
        if not headers:
            first_tr = tbl.find("tr")
            if first_tr:
                headers = [
                    clean(td.get_text()) for td in first_tr.find_all(["th", "td"])
                ]

        header_l = [h.lower() for h in headers]
        if "document" not in header_l or "type" not in header_l:
            continue

        col_idx = {h.lower(): i for i, h in enumerate(headers)}
        doc_i = col_idx.get("document")
        type_i = col_idx.get("type")
        desc_i = col_idx.get("description")

        for tr in tbl.find_all("tr"):
            tds = tr.find_all("td")
            if not tds or doc_i is None or type_i is None:
                continue

            doc_cell = tds[doc_i] if doc_i < len(tds) else None
            if (
                doc_cell
                and not doc_cell.find("a")
                and doc_cell.get_text(strip=True).lower() == "document"
            ):
                continue

            type_cell = tds[type_i] if type_i < len(tds) else None
            desc_cell = (
                tds[desc_i] if (desc_i is not None and desc_i < len(tds)) else None
            )

            a = doc_cell.find("a") if doc_cell else None
            document = (
                clean(a.get("href"))
                if (a and a.has_attr("href"))
                else clean(doc_cell.get_text() if doc_cell else "")
            )

            if not document:
                continue

            rows.append(
                {
                    "description": clean(desc_cell.get_text() if desc_cell else ""),
                    "document": document,
                    "type": clean(type_cell.get_text() if type_cell else ""),
                }
            )

    return rows


def parse_index_json(js: dict) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    directory = (js or {}).get("directory", {})
    items = directory.get("item", []) or []

    for item in items:
        rows.append(
            {
                "description": item.get("desc") or "",
                "document": item.get("href", "") or "",
                "type": item.get("type", "") or "",
            }
        )

    return rows


def looks_like_ex4(row: Dict[str, str]) -> bool:
    t = (row.get("type", "") or "").strip()
    d = (row.get("document", "") or "").strip()
    norm = t.replace(" ", "").upper()
    return bool(
        EX4_TYPE_RE.search(t)
        or norm.startswith(("EX-4", "EX4"))
        or EX4_NAME_RE.search(d)
    )


def looks_like_ex99(row: Dict[str, str]) -> bool:
    t = (row.get("type", "") or "").strip()
    d = (row.get("document", "") or "").strip()
    norm = t.replace(" ", "").upper()
    return bool(
        EX99_TYPE_RE.search(t)
        or norm.startswith(("EX-99", "EX99"))
        or EX99_NAME_RE.search(d)
    )


def has_indenture_signal(row: Dict[str, str]) -> bool:
    haystack = " ".join(
        [row.get("description", ""), row.get("document", ""), row.get("type", "")]
    )

    if INDENTURE_HINT_RE.search(haystack):
        if not NEG_NOISE_RE.search(haystack):
            return True

    return False


def is_noisy(row: Dict[str, str]) -> bool:
    haystack = " ".join([row.get("description", ""), row.get("document", "")])
    return bool(NEG_NOISE_RE.search(haystack))


def extract_exhibits(cik: str, adsh: str, mode: str = "both") -> List[Dict[str, str]]:
    base_dir_path = _to_base_dir(cik, adsh)
    base_url = f"https://www.sec.gov{base_dir_path}"
    index_url = f"{base_url}/{adsh}-index.html"

    time.sleep(PAGE_SLEEP)
    rows: List[Dict[str, str]] = []
    html_content = None
    try:
        r = _get_with_retry(index_url)
        html_content = r.text
        rows = parse_index_table(html_content)
    except Exception:
        pass

    if not rows:
        json_url = _alt_json_url(index_url)
        if json_url:
            try:
                rj = _get_with_retry(json_url, accept_json=True)
                rows = parse_index_json(rj.json())
            except Exception:
                pass

    if not rows:
        return []

    referenced_files = set()
    if html_content:
        try:
            soup = BeautifulSoup(html_content, "html.parser")
            for a in soup.find_all("a"):
                href = a.get("href", "").lower()
                if any(
                    ext in href
                    for ext in [
                        ".jpg",
                        ".jpeg",
                        ".png",
                        ".gif",
                        ".css",
                        ".svg",
                        ".webp",
                    ]
                ):
                    referenced_files.add(href.split("/")[-1])
        except Exception:
            pass

    saw_any_ex4 = any(looks_like_ex4(r) for r in rows)
    main_exhibits = {}
    for row in rows:
        doc = row.get("document", "")
        if not doc:
            continue

        url = _build_abs_url(base_url, doc)
        is_ex4 = looks_like_ex4(row)
        is_ex99 = looks_like_ex99(row)
        ind_ok = has_indenture_signal(row)
        noisy = is_noisy(row)

        keep = False
        score = "0"
        matched_by = ""

        if mode == "ex4":
            keep = is_ex4
        elif mode == "ex99":
            keep = is_ex99 and (ind_ok or not noisy)
        elif mode == "both":
            if is_ex4:
                keep, score, matched_by = True, "10", "type:EX-4"
            elif is_ex99:
                if noisy:
                    keep = False
                elif ind_ok:
                    keep, score, matched_by = True, "8", "type:EX-99|indenture"
                elif saw_any_ex4:
                    keep, score, matched_by = True, "6", "type:EX-99|filing_has_ex4"
                else:
                    keep, score, matched_by = True, "4", "type:EX-99|candidate"
        elif mode == "indenture":
            if is_ex4 or ind_ok:
                keep = True

        if keep and score == "0":
            score = (
                "10"
                if is_ex4
                else ("8" if (is_ex99 and ind_ok) else ("6" if is_ex99 else "5"))
            )
            matched_by = ("type:EX-4" if is_ex4 else "type:EX-99") + (
                "|indenture" if ind_ok else ("" if score != "4" else "|candidate")
            )

        if keep:
            doc_name = doc.split("/")[-1]
            main_exhibits[_normalize_url(url)] = {
                "doc_url": url,
                "doc_name": doc_name,
                "doc_description": row.get("description", ""),
                "doc_type": row.get("type", ""),
                "match_score": score,
                "matched_by": matched_by,
            }

    for url, exhibit in main_exhibits.items():
        doc_name = exhibit["doc_name"].lower()
        if doc_name.endswith((".htm", ".html")):
            asset_count = 0
            base_name = doc_name.rsplit(".", 1)[0]
            for row in rows:
                asset_doc = row.get("document", "").lower()
                if not asset_doc:
                    continue
                asset_name = asset_doc.split("/")[-1]
                is_asset = any(
                    ext in asset_name
                    for ext in [
                        ".jpg",
                        ".jpeg",
                        ".png",
                        ".gif",
                        ".css",
                        ".svg",
                        ".webp",
                        ".bmp",
                    ]
                )
                if is_asset:
                    asset_base = asset_name.rsplit(".", 1)[0]
                    if (
                        base_name in asset_base
                        or asset_base in base_name
                        or asset_name in referenced_files
                    ):
                        asset_count += 1

            exhibit["has_dependencies"] = "yes" if asset_count > 0 else "no"
            exhibit["asset_count"] = str(asset_count)
        else:
            exhibit["has_dependencies"] = "no"
            exhibit["asset_count"] = "0"

    return list(main_exhibits.values())


def run_pipeline(
    start_date: str,
    end_date: str,
    root_dir: Path,
    mode: str = "both",
    forms: Tuple[str, ...] = ("6-K", "8-K"),
    query: str = "",
):

    asset_dir = root_dir / "asset"
    asset_dir.mkdir(parents=True, exist_ok=True)

    filings_csv = asset_dir / "filings.csv"
    exhibits_csv = asset_dir / "exhibits.csv"

    filings: Dict[Tuple[str, str], Dict[str, str]] = {}
    for hit in query_fts(start_date, end_date, forms=forms, query=query):
        cik, adsh, form, filed, company = extract_filing_meta(hit)
        if not cik or not adsh:
            continue

        key = (cik, adsh)
        base_dir_path = _to_base_dir(cik, adsh)
        base_url = f"https://www.sec.gov{base_dir_path}"
        index_url = f"{base_url}/{adsh}-index.html"
        filings[key] = {
            "cik": cik,
            "accession": adsh,
            "form": form,
            "filed": filed,
            "company": company,
            "exhibit_index_url": index_url,
            "filing_base_dir": base_url,
        }

    if not filings:
        return

    filing_rows = sorted(
        filings.values(), key=lambda r: (r.get("filed", ""), r.get("accession", ""))
    )

    with open(filings_csv, "w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "cik",
            "accession",
            "form",
            "filed",
            "company",
            "exhibit_index_url",
            "filing_base_dir",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(filing_rows)
    logger.info("Found %d filings", len(filing_rows))
    exhibit_rows: List[Dict[str, str]] = []

    for filing in filing_rows:
        cik = filing["cik"]
        adsh = filing["accession"]
        try:
            exhibits = extract_exhibits(cik, adsh, mode=mode)
            for exhibit in exhibits:
                exhibit_rows.append(
                    {
                        "cik": cik,
                        "accession": adsh,
                        "form": filing["form"],
                        "filed": filing["filed"],
                        "company": filing["company"],
                        "exhibit_index_url": filing["exhibit_index_url"],
                        "doc_url": exhibit["doc_url"],
                        "doc_name": exhibit["doc_name"],
                        "doc_description": exhibit.get("doc_description", ""),
                        "doc_type": exhibit.get("doc_type", ""),
                        "match_score": exhibit["match_score"],
                        "matched_by": exhibit["matched_by"],
                        "has_dependencies": exhibit.get("has_dependencies", "no"),
                        "asset_count": exhibit.get("asset_count", "0"),
                    }
                )
        except Exception:
            pass

    if exhibit_rows:
        with open(exhibits_csv, "w", newline="", encoding="utf-8") as f:
            fieldnames = list(exhibit_rows[0].keys())
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(exhibit_rows)

        unique_filings = len({(r["cik"], r["accession"]) for r in exhibit_rows})
        logger.info(
            "Extracted %d exhibits from %d filings", len(exhibit_rows), unique_filings
        )
    SESSION.close()
