#!/usr/bin/env python3
import csv
import os
import re
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse
import logging

import requests

logger = logging.getLogger(__name__)

try:
    from bs4 import BeautifulSoup
except ImportError:
    logger.error("ERROR: BeautifulSoup required.")
    import sys

    sys.exit(1)

UA = os.getenv("SEC_USER_AGENT", "Bloomberg-Drexel-Capstone (dhd37@drexel.edu)")
REQUEST_TIMEOUT = 30
RETRY_MAX = 3
RETRY_BASE_DELAY = 1.0
DOWNLOAD_BATCH_SIZE = 25

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,*/*",
    }
)


class AssetManager:
    def __init__(self, root_dir: Path):
        self.root_dir = root_dir
        self.url_to_local: Dict[str, str] = {}

    def save_image(self, url: str, content: bytes, assets_dir: Path) -> Optional[str]:
        try:
            parsed_url = urlparse(url)
            original_name = Path(parsed_url.path).name

            safe_name = re.sub(r"[^\w\-.]", "_", original_name)
            if not safe_name:
                safe_name = "image.bin"

            local_path = assets_dir / safe_name
            counter = 1
            while local_path.exists():
                name, ext = (
                    safe_name.rsplit(".", 1) if "." in safe_name else (safe_name, "")
                )
                safe_name = f"{name}_{counter}.{ext}" if ext else f"{name}_{counter}"
                local_path = assets_dir / safe_name
                counter += 1

            with open(local_path, "wb") as f:
                f.write(content)

            return safe_name

        except Exception as e:
            logger.exception("Failed to save image %s: %s", url, e)
            return None


class DocumentDownloader:
    def __init__(self):
        pass

    def fetch_with_retry(
        self, url: str, max_retries: int = RETRY_MAX
    ) -> Optional[bytes]:
        for attempt in range(max_retries):
            try:
                resp = SESSION.get(url, timeout=REQUEST_TIMEOUT)
                resp.raise_for_status()
                return resp.content
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    delay = RETRY_BASE_DELAY * (2**attempt)
                    logger.warning(
                        "Fetch attempt %d failed for %s (%s). Retrying in %.1fs",
                        attempt + 1,
                        url,
                        e,
                        delay,
                    )
                    time.sleep(delay)
                else:
                    logger.exception("Download failed: %s", url)
                    return None

    def extract_dependencies(self, htm_content: bytes, base_url: str) -> List[str]:
        try:
            soup = BeautifulSoup(htm_content, "html.parser")
            urls = []

            for img in soup.find_all("img"):
                src = img.get("src")
                if src:
                    urls.append(urljoin(base_url, src))

            for elem in soup.find_all(style=re.compile("background")):
                style = elem.get("style", "")
                matches = re.findall(r'url\(["\']?([^"\'()]+)["\']?\)', style)
                for match in matches:
                    urls.append(urljoin(base_url, match))

            return urls

        except Exception as e:
            logger.exception("Failed to extract dependencies: %s", e)
            return []

    def rewrite_paths(
        self, htm_content: bytes, url_to_filename: Dict[str, str], doc_name: str
    ) -> bytes:
        try:
            text = htm_content.decode("utf-8", errors="replace")

            base_name = doc_name.rsplit(".", 1)[0] if "." in doc_name else doc_name
            assets_folder = f"{base_name}_files"

            def replace_src(match):
                prefix = match.group(1)
                original_path = match.group(2)

                original_filename = Path(original_path).name
                saved_filename = None

                for url, filename in url_to_filename.items():
                    url_filename = Path(urlparse(url).path).name
                    if url_filename == original_filename:
                        saved_filename = filename
                        break

                if saved_filename:
                    return f'{prefix}"./{assets_folder}/{saved_filename}"'
                else:
                    return match.group(0)

            text = re.sub(
                r'(<img[^>]+src=)["\']?([^\s"\'><]+)["\']?',
                replace_src,
                text,
                flags=re.IGNORECASE,
            )

            return text.encode("utf-8", errors="replace")

        except Exception as e:
            logger.exception("Path rewriting failed: %s", e)
            return htm_content


class DocumentOrganizer:
    def __init__(self, root_dir: Path):
        self.root_dir = root_dir
        self.buckets = {
            "indenture": root_dir / "indentures",
            "image_based_indenture": root_dir / "image_based_indentures",
        }

    def get_bucket_path(self, category: str) -> Path:
        if category not in self.buckets:
            raise ValueError(f"Unknown category: {category}")
        return self.buckets[category]

    def get_assets_subdir(self, doc_name: str, category: str) -> Path:
        bucket_path = self.get_bucket_path(category)
        bucket_path.mkdir(parents=True, exist_ok=True)

        base_name = doc_name.rsplit(".", 1)[0] if "." in doc_name else doc_name
        assets_dir = bucket_path / f"{base_name}_files"
        assets_dir.mkdir(parents=True, exist_ok=True)
        return assets_dir

    def save_document(self, htm_content: bytes, doc_name: str, category: str) -> Path:
        bucket_path = self.get_bucket_path(category)
        bucket_path.mkdir(parents=True, exist_ok=True)

        target_path = bucket_path / doc_name
        counter = 1
        while target_path.exists():
            name, ext = doc_name.rsplit(".", 1) if "." in doc_name else (doc_name, "")
            new_name = f"{name}_{counter}.{ext}" if ext else f"{name}_{counter}"
            target_path = bucket_path / new_name
            counter += 1

        with open(target_path, "wb") as f:
            f.write(htm_content)

        return target_path


class Phase3Pipeline:
    def __init__(self, root_dir: Path, verbose: bool = False):
        self.root_dir = Path(root_dir)
        self.asset_dir = self.root_dir / "asset"
        self.verbose = verbose

        self.downloader = DocumentDownloader()
        self.asset_mgr = AssetManager(self.root_dir)
        self.organizer = DocumentOrganizer(self.root_dir)

        self.stats = {
            "total": 0,
            "downloaded": 0,
            "failed": 0,
            "images_saved": 0,
            "by_category": defaultdict(int),
        }

    def run(self):
        classified_csv = self.asset_dir / "exhibits_classified.csv"
        if not classified_csv.exists():
            logger.error("exhibits_classified.csv not found")
            return False

        documents = self._load_documents(classified_csv)
        if not documents:
            logger.error("No documents to download")
            return False

        logger.info("Downloading %d documents...", len(documents))

        for batch in self._batch_documents(documents, DOWNLOAD_BATCH_SIZE):
            for doc in batch:
                self._process_document(doc)

        logger.info(
            "Done: %d docs, %d images",
            self.stats["downloaded"],
            self.stats["images_saved"],
        )

        return self.stats["failed"] == 0

    def _load_documents(self, csv_path: Path) -> List[Dict]:
        documents = []

        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                action = row.get("download_action", "")
                category = row.get("category", "")

                if action in ["download", "download_with_assets"] and category in [
                    "indenture",
                    "image_based_indenture",
                ]:
                    documents.append(row)
                    self.stats["total"] += 1

        return documents

    def _batch_documents(self, documents: List[Dict], batch_size: int):
        for i in range(0, len(documents), batch_size):
            yield documents[i : i + batch_size]

    def _process_document(self, doc: Dict):
        doc_url = doc.get("doc_url", "").strip()
        doc_name = doc.get("doc_name", "unknown").strip()
        category = doc.get("category", "uncertain").strip()

        if not doc_url:
            logger.error("Missing doc_url for %s (%s)", doc_name, category)
            self.stats["failed"] += 1
            return

        if self.stats["total"] > 0 and self.stats["total"] % 10 == 0:
            logger.info("  %d processed...", self.stats["total"])

        htm_content = self.downloader.fetch_with_retry(doc_url)
        if not htm_content:
            logger.error("Failed to fetch %s", doc_url)
            self.stats["failed"] += 1
            return

        if category == "image_based_indenture":
            dependencies = self.downloader.extract_dependencies(htm_content, doc_url)
            if dependencies:
                assets_dir = self.organizer.get_assets_subdir(doc_name, category)
                url_to_filename = {}

                for dep_url in dependencies:
                    dep_content = self.downloader.fetch_with_retry(dep_url)
                    if dep_content:
                        saved_filename = self.asset_mgr.save_image(
                            dep_url, dep_content, assets_dir
                        )
                        if saved_filename:
                            url_to_filename[dep_url] = saved_filename
                            self.stats["images_saved"] += 1

                htm_content = self.downloader.rewrite_paths(
                    htm_content, url_to_filename, doc_name
                )

        try:
            self.organizer.save_document(htm_content, doc_name, category)
            self.stats["downloaded"] += 1
            self.stats["by_category"][category] += 1
        except Exception as e:
            logger.exception("Failed to save %s: %s", doc_name, e)
            self.stats["failed"] += 1


def run_pipeline(root_dir: Path, verbose: bool = False) -> bool:
    try:
        pipeline = Phase3Pipeline(root_dir, verbose)
        return pipeline.run()
    except Exception as e:
        logger.exception("Phase 3 error: %s", e)
        return False
