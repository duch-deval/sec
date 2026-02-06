#!/usr/bin/env python3
import csv
import os
import random
import re
import time
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

UA = os.getenv("SEC_USER_AGENT", "Bloomberg-Drexel-Capstone (dhd37@drexel.edu)")
FTS_BASE = "https://efts.sec.gov/LATEST/search-index"
PAGE_SLEEP, RETRY_MAX, RETRY_BASE_DELAY = 0.25, 5, 0.5

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": UA, "Accept": "application/json,text/html,*/*", "Referer": "https://www.sec.gov/edgar/search/"})

EX4_TYPE_RE = re.compile(r"^EX\s*[-_ ]?\s*4(?:[.\-]?\d+)?(?:[A-Za-z()\-/\s]*)?$", re.IGNORECASE)
EX99_TYPE_RE = re.compile(r"^EX\s*[-_ ]?\s*99(?:[.\-]?\d+)?(?:[A-Za-z()\-/\s]*)?$", re.IGNORECASE)
EX4_NAME_RE = re.compile(r"(?:^|/)[^/]*ex[-_. ]?4(?:[.-]?\d+)?\.(?:htm|html|pdf|txt)$", re.IGNORECASE)
EX99_NAME_RE = re.compile(r"(?:^|/)[^/]*ex[-_. ]?99(?:[.-]?\d+)?\.(?:htm|html|pdf|txt)$", re.IGNORECASE)

INDENTURE_HINT_RE = re.compile(
    r"(?i)\b(supplemental\s+indenture|base\s+indenture|indenture(?:\s+no\.?\s*\d+)?|trust\s+indenture"
    r"|senior\s+notes?|subordinated\s+notes?|debentures?|debt\s+securities?"
    r"|guarantee(?:d)?\s+(?:senior|subordinated|notes?|debentures?)|guaranty\s+agreement"
    r"|exchange\s+offer|offer\s+to\s+exchange|consent\s+solicitation|notice\s+of\s+redemption|redemption\s+notice)\b")

NEG_NOISE_RE = re.compile(
    r"(?i)\b(press\s+release|earnings\s+(?:release|call)|slides?|presentation"
    r"|investor\s+(?:presentation|slides)|transcript|financial\s+(?:results|statements?))\b")


def _get_json_with_retry(params: Dict[str, str]) -> Dict[str, Any]:
    delay = RETRY_BASE_DELAY
    for _ in range(RETRY_MAX):
        try:
            r = SESSION.get(FTS_BASE, params=params, timeout=30)
            if r.status_code == 200 and "application/json" in r.headers.get("Content-Type", ""):
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(delay + random.uniform(0, delay * 0.3))
                delay = min(delay * 2, 16.0)
                continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.Timeout:
            time.sleep(delay + random.uniform(0, delay * 0.3))
            delay = min(delay * 2, 16.0)
        except Exception:
            time.sleep(delay)
            delay = min(delay * 2, 16.0)
    raise RuntimeError(f"FTS API failed after {RETRY_MAX} retries")


def _get_with_retry(url: str, accept_json: bool = False) -> requests.Response:
    delay = RETRY_BASE_DELAY
    for attempt in range(RETRY_MAX):
        try:
            r = SESSION.get(url, timeout=45)
            if r.status_code == 200:
                if not accept_json or "json" in r.headers.get("Content-Type", "").lower():
                    return r
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(delay)
                delay = min(delay * 2, 8.0)
                continue
            r.raise_for_status()
            return r
        except Exception:
            if attempt < RETRY_MAX - 1:
                time.sleep(delay)
                delay = min(delay * 2, 8.0)
    raise RuntimeError(f"Fetch failed: {url}")


def _normalize_url(url: str) -> str:
    p = urlparse(url)
    return urlunparse(p._replace(params="", query="", fragment="", scheme=p.scheme.lower(), netloc=p.netloc.lower()))


def _build_abs_url(base_dir: str, href: str) -> str:
    return urljoin(base_dir if base_dir.endswith("/") else base_dir + "/", href)


def _to_base_dir(cik: str, adsh: str) -> str:
    return f"/Archives/edgar/data/{int(cik)}/{adsh.replace('-', '')}"


def _alt_json_url(index_url: str) -> Optional[str]:
    if index_url.lower().endswith(".html"):
        return index_url[:-5] + ".json"
    if index_url.lower().endswith(".htm"):
        return index_url[:-4] + ".json"
    return None


def query_fts(start_date: str, end_date: str, forms: Tuple[str, ...] = ("6-K", "8-K"), query: str = "") -> Iterable[Dict[str, Any]]:
    forms_csv = ",".join(f.strip() for f in forms if f.strip())
    offset, batch_size, consecutive_failures = 0, 100, 0

    while consecutive_failures < 3:
        params = {"dateRange": "custom", "category": "custom", "forms": forms_csv,
                  "startdt": start_date, "enddt": end_date, "from": str(offset), "size": str(batch_size)}
        if query.strip():
            params["q"] = query
        try:
            data = _get_json_with_retry(params)
            consecutive_failures = 0
        except Exception:
            consecutive_failures += 1
            offset += batch_size
            time.sleep(2.0)
            continue

        hits = data.get("hits", {}).get("hits") if isinstance(data, dict) else None
        if not hits:
            hits = data.get("results", []) if isinstance(data, dict) else None
        if not hits:
            break

        yield from hits
        if len(hits) < batch_size:
            break
        offset += batch_size
        time.sleep(PAGE_SLEEP)


def extract_filing_meta(hit: Dict[str, Any]) -> Tuple[str, str, str, str, str]:
    src = hit.get("_source", hit)
    def pick(*keys):
        for k in keys:
            if v := src.get(k):
                return v[0] if isinstance(v, list) else (str(v) if isinstance(v, int) else v)
        return ""
    return (pick("cik", "CIK", "ciks", "cik_str"), pick("adsh", "accessionNo", "accession_no", "accessionNumber", "accession"),
            pick("formType", "form", "form_type"), pick("filedAt", "filed", "file_date", "filed_date"),
            pick("display_names", "companyName", "company_name", "entity"))


def parse_index_table(html: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    clean = lambda t: (t or "").strip().replace("\xa0", " ")

    for tbl in soup.find_all("table"):
        headers = [clean(th.get_text()) for th in tbl.find_all("th")]
        if not headers and (first_tr := tbl.find("tr")):
            headers = [clean(td.get_text()) for td in first_tr.find_all(["th", "td"])]
        header_l = [h.lower() for h in headers]
        if "document" not in header_l or "type" not in header_l:
            continue

        col_idx = {h.lower(): i for i, h in enumerate(headers)}
        doc_i, type_i, desc_i = col_idx.get("document"), col_idx.get("type"), col_idx.get("description")

        for tr in tbl.find_all("tr"):
            tds = tr.find_all("td")
            if not tds or doc_i is None or type_i is None:
                continue
            doc_cell = tds[doc_i] if doc_i < len(tds) else None
            if doc_cell and not doc_cell.find("a") and doc_cell.get_text(strip=True).lower() == "document":
                continue
            type_cell = tds[type_i] if type_i < len(tds) else None
            desc_cell = tds[desc_i] if desc_i is not None and desc_i < len(tds) else None
            a = doc_cell.find("a") if doc_cell else None
            document = clean(a.get("href")) if a and a.has_attr("href") else clean(doc_cell.get_text() if doc_cell else "")
            if document:
                rows.append({"description": clean(desc_cell.get_text() if desc_cell else ""),
                             "document": document, "type": clean(type_cell.get_text() if type_cell else "")})
    return rows


def parse_index_json(js: dict) -> List[Dict[str, str]]:
    return [{"description": item.get("desc", ""), "document": item.get("href", ""), "type": item.get("type", "")}
            for item in (js or {}).get("directory", {}).get("item", [])]


def looks_like_ex4(row: Dict[str, str]) -> bool:
    t, d = (row.get("type", "") or "").strip(), (row.get("document", "") or "").strip()
    return bool(EX4_TYPE_RE.search(t) or t.replace(" ", "").upper().startswith(("EX-4", "EX4")) or EX4_NAME_RE.search(d))


def looks_like_ex99(row: Dict[str, str]) -> bool:
    t, d = (row.get("type", "") or "").strip(), (row.get("document", "") or "").strip()
    return bool(EX99_TYPE_RE.search(t) or t.replace(" ", "").upper().startswith(("EX-99", "EX99")) or EX99_NAME_RE.search(d))


def has_indenture_signal(row: Dict[str, str]) -> bool:
    haystack = f"{row.get('description', '')} {row.get('document', '')} {row.get('type', '')}"
    return bool(INDENTURE_HINT_RE.search(haystack) and not NEG_NOISE_RE.search(haystack))


def is_noisy(row: Dict[str, str]) -> bool:
    return bool(NEG_NOISE_RE.search(f"{row.get('description', '')} {row.get('document', '')}"))


def extract_exhibits(cik: str, adsh: str, mode: str = "both") -> List[Dict[str, str]]:
    base_url = f"https://www.sec.gov{_to_base_dir(cik, adsh)}"
    index_url = f"{base_url}/{adsh}-index.html"
    time.sleep(PAGE_SLEEP)

    rows, html_content = [], None
    try:
        r = _get_with_retry(index_url)
        html_content, rows = r.text, parse_index_table(r.text)
    except Exception:
        pass
    if not rows and (json_url := _alt_json_url(index_url)):
        try:
            rows = parse_index_json(_get_with_retry(json_url, accept_json=True).json())
        except Exception:
            pass
    if not rows:
        return []

    referenced_files = set()
    if html_content:
        try:
            for a in BeautifulSoup(html_content, "html.parser").find_all("a"):
                href = a.get("href", "").lower()
                if any(ext in href for ext in [".jpg", ".jpeg", ".png", ".gif", ".css", ".svg", ".webp"]):
                    referenced_files.add(href.split("/")[-1])
        except Exception:
            pass

    saw_any_ex4 = any(looks_like_ex4(r) for r in rows)
    main_exhibits = {}

    for row in rows:
        if not (doc := row.get("document", "")):
            continue
        url = _build_abs_url(base_url, doc)
        is_ex4, is_ex99, ind_ok, noisy = looks_like_ex4(row), looks_like_ex99(row), has_indenture_signal(row), is_noisy(row)

        keep, score, matched_by = False, "0", ""
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
            keep = is_ex4 or ind_ok

        if keep and score == "0":
            score = "10" if is_ex4 else ("8" if is_ex99 and ind_ok else ("6" if is_ex99 else "5"))
            matched_by = ("type:EX-4" if is_ex4 else "type:EX-99") + ("|indenture" if ind_ok else ("" if score != "4" else "|candidate"))

        if keep:
            doc_name = doc.split("/")[-1]
            main_exhibits[_normalize_url(url)] = {"doc_url": url, "doc_name": doc_name, "doc_description": row.get("description", ""),
                                                   "doc_type": row.get("type", ""), "match_score": score, "matched_by": matched_by}

    for exhibit in main_exhibits.values():
        exhibit["filing_has_ex4"] = "yes" if saw_any_ex4 else "no"
        doc_name = exhibit["doc_name"].lower()
        if doc_name.endswith((".htm", ".html")):
            base_name = doc_name.rsplit(".", 1)[0]
            asset_count = sum(1 for r in rows if (an := r.get("document", "").lower().split("/")[-1]) and
                              any(ext in an for ext in [".jpg", ".jpeg", ".png", ".gif", ".css", ".svg", ".webp", ".bmp"]) and
                              (base_name in an.rsplit(".", 1)[0] or an.rsplit(".", 1)[0] in base_name or an in referenced_files))
            exhibit["has_dependencies"], exhibit["asset_count"] = ("yes" if asset_count > 0 else "no"), str(asset_count)
        else:
            exhibit["has_dependencies"], exhibit["asset_count"] = "no", "0"

    return list(main_exhibits.values())


def run_pipeline(start_date: str, end_date: str, root_dir: Path, mode: str = "both",
                 forms: Tuple[str, ...] = ("6-K", "8-K"), query: str = ""):
    asset_dir = root_dir / "asset"
    asset_dir.mkdir(parents=True, exist_ok=True)

    filings = {}
    for hit in query_fts(start_date, end_date, forms=forms, query=query):
        cik, adsh, form, filed, company = extract_filing_meta(hit)
        if cik and adsh:
            base_url = f"https://www.sec.gov{_to_base_dir(cik, adsh)}"
            filings[(cik, adsh)] = {"cik": cik, "accession": adsh, "form": form, "filed": filed, "company": company,
                                    "exhibit_index_url": f"{base_url}/{adsh}-index.html", "filing_base_dir": base_url}
    if not filings:
        return

    filing_rows = sorted(filings.values(), key=lambda r: (r.get("filed", ""), r.get("accession", "")))
    with open(asset_dir / "filings.csv", "w", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=["cik", "accession", "form", "filed", "company", "exhibit_index_url", "filing_base_dir"]).writeheader()
        csv.DictWriter(f, fieldnames=["cik", "accession", "form", "filed", "company", "exhibit_index_url", "filing_base_dir"]).writerows(filing_rows)
    logger.info("Found %d filings", len(filing_rows))

    exhibit_rows = []
    for filing in filing_rows:
        try:
            for exhibit in extract_exhibits(filing["cik"], filing["accession"], mode=mode):
                exhibit_rows.append({**{k: filing[k] for k in ["cik", "accession", "form", "filed", "company", "exhibit_index_url"]}, **exhibit})
        except Exception:
            pass

    if exhibit_rows:
        with open(asset_dir / "exhibits.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(exhibit_rows[0].keys()))
            w.writeheader()
            w.writerows(exhibit_rows)
        logger.info("Extracted %d exhibits from %d filings", len(exhibit_rows), len({(r["cik"], r["accession"]) for r in exhibit_rows}))

    SESSION.close()