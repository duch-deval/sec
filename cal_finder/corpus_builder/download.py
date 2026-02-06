#!/usr/bin/env python3
import csv
import os
import re
import time
import logging
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

USER_AGENT = os.getenv("SEC_USER_AGENT", "Bloomberg-Drexel-Capstone (dhd37@drexel.edu)")
REQUEST_TIMEOUT, RETRY_MAX, RETRY_BASE_DELAY, REQUEST_DELAY = 30, 3, 1.0, 0.15

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml,*/*"})

DOWNLOAD_CATEGORIES = {'ex4_indenture', 'ex99_indenture'}
HOLDING_QUEUE_CATEGORIES = {'ex4_warrant', 'ex99_uncertain'}
CATEGORY_FOLDERS = {'ex4_indenture': 'ex4', 'ex4_warrant': 'holding_queue', 'ex99_indenture': 'ex99', 'ex99_uncertain': 'ex99'}


def fetch_with_retry(url: str) -> Optional[bytes]:
    for attempt in range(RETRY_MAX):
        try:
            time.sleep(REQUEST_DELAY)
            resp = SESSION.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.content
        except requests.exceptions.RequestException:
            if attempt < RETRY_MAX - 1:
                time.sleep(RETRY_BASE_DELAY * (2 ** attempt))
    return None


def extract_dependencies(html_content: bytes, base_url: str) -> List[str]:
    try:
        soup = BeautifulSoup(html_content, "html.parser")
        urls = [urljoin(base_url, img.get("src")) for img in soup.find_all("img") if img.get("src")]
        for elem in soup.find_all(style=re.compile("background")):
            for match in re.findall(r'url\(["\']?([^"\'()]+)["\']?\)', elem.get("style", "")):
                urls.append(urljoin(base_url, match))
        return urls
    except Exception:
        return []


def save_asset(url: str, content: bytes, assets_dir: Path) -> Optional[str]:
    try:
        safe_name = re.sub(r"[^\w\-.]", "_", Path(urlparse(url).path).name) or "asset.bin"
        local_path = assets_dir / safe_name
        counter = 1
        while local_path.exists():
            name, ext = (safe_name.rsplit(".", 1) + [""])[:2]
            safe_name = f"{name}_{counter}.{ext}" if ext else f"{name}_{counter}"
            local_path = assets_dir / safe_name
            counter += 1
        local_path.write_bytes(content)
        return safe_name
    except Exception:
        return None


def rewrite_paths(html_content: bytes, url_to_filename: Dict[str, str], doc_name: str) -> bytes:
    try:
        text = html_content.decode("utf-8", errors="replace")
        base_name = doc_name.rsplit(".", 1)[0] if "." in doc_name else doc_name
        assets_folder = f"{base_name}_files"

        def replace_src(match):
            original_filename = Path(match.group(2)).name
            for url, filename in url_to_filename.items():
                if Path(urlparse(url).path).name == original_filename:
                    return f'{match.group(1)}"./{assets_folder}/{filename}"'
            return match.group(0)

        return re.sub(r'(<img[^>]+src=)["\']?([^\s"\'><]+)["\']?', replace_src, text, flags=re.IGNORECASE).encode("utf-8", errors="replace")
    except Exception:
        return html_content


def get_output_path(root_dir: Path, category: str) -> Path:
    folder = root_dir / "exhibits" / CATEGORY_FOLDERS.get(category, 'other')
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def get_assets_dir(root_dir: Path, doc_name: str, category: str) -> Path:
    base_name = doc_name.rsplit(".", 1)[0] if "." in doc_name else doc_name
    assets_dir = get_output_path(root_dir, category) / f"{base_name}_files"
    assets_dir.mkdir(parents=True, exist_ok=True)
    return assets_dir


def save_document(content: bytes, doc_name: str, root_dir: Path, category: str) -> Path:
    output_folder = get_output_path(root_dir, category)
    target_path = output_folder / doc_name
    counter = 1
    while target_path.exists():
        name, ext = (doc_name.rsplit(".", 1) + [""])[:2]
        target_path = output_folder / (f"{name}_{counter}.{ext}" if ext else f"{name}_{counter}")
        counter += 1
    target_path.write_bytes(content)
    return target_path


def process_document(doc: Dict, root_dir: Path, stats: Dict) -> Optional[Path]:
    doc_url, doc_name = doc.get("doc_url", "").strip(), doc.get("doc_name", "unknown").strip()
    category, is_image_based = doc.get("category", "other").strip(), doc.get("is_image_based", "no") == "yes"

    if not doc_url:
        stats["failed"] += 1
        return None

    content = fetch_with_retry(doc_url)
    if not content:
        stats["failed"] += 1
        return None

    if is_image_based:
        dependencies = extract_dependencies(content, doc_url)
        if dependencies:
            assets_dir = get_assets_dir(root_dir, doc_name, category)
            url_to_filename = {}
            for dep_url in dependencies:
                if (dep_content := fetch_with_retry(dep_url)) and (saved := save_asset(dep_url, dep_content, assets_dir)):
                    url_to_filename[dep_url] = saved
                    stats["assets_saved"] += 1
            content = rewrite_paths(content, url_to_filename, doc_name)

    try:
        saved_path = save_document(content, doc_name, root_dir, category)
        stats["downloaded"] += 1
        stats["by_category"][category] += 1
        return saved_path
    except Exception:
        stats["failed"] += 1
        return None


def run_pipeline(root_dir: Path, verbose: bool = False) -> bool:
    input_csv = root_dir / "asset" / "exhibits_classified.csv"
    if not input_csv.exists():
        logger.error("Phase 2 output not found: %s", input_csv)
        return False

    documents = []
    with open(input_csv, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            category, action = row.get("category", ""), row.get("download_action", "")
            if (action == "download" and category in DOWNLOAD_CATEGORIES) or \
               (action == "holding_queue" and category in HOLDING_QUEUE_CATEGORIES):
                documents.append(row)

    if not documents:
        return True

    stats = {"total": len(documents), "downloaded": 0, "failed": 0, "assets_saved": 0, "by_category": defaultdict(int)}
    logger.info("Download: Downloading %d documents", len(documents))

    for i, doc in enumerate(documents):
        process_document(doc, root_dir, stats)
        if verbose and (i + 1) % 10 == 0:
            logger.info("Progress: %d/%d", i + 1, len(documents))

    logger.info("Download: %d downloaded, %d failed, %d assets", stats["downloaded"], stats["failed"], stats["assets_saved"])
    return stats["failed"] == 0