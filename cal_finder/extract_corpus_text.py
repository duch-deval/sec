import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from bs4 import BeautifulSoup
import html as html_module


def extract_text(html_path: Path, date_label: str = None) -> dict:
    try:
        soup = BeautifulSoup(html_path.read_bytes().decode("utf-8", errors="replace"), "html.parser")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        text = html_module.unescape(soup.get_text()).replace("\xa0", " ")
        result = {
            "filename": html_path.name,
            "path": str(html_path.relative_to(html_path.parents[1])),
            "size_bytes": html_path.stat().st_size,
            "text": text,
        }
        if date_label:
            result["date"] = date_label
        return result
    except Exception as e:
        result = {
            "filename": html_path.name,
            "path": str(html_path.relative_to(html_path.parents[1])),
            "error": str(e),
        }
        if date_label:
            result["date"] = date_label
        return result


def collect_html_files(root_dir: Path):
    exhibits_dir = root_dir / "exhibits"
    if not exhibits_dir.exists():
        return []
    html_files = []
    for folder in ["ex4", "ex99", "holding_queue"]:
        folder_path = exhibits_dir / folder
        if folder_path.exists():
            html_files.extend(folder_path.glob("*.htm"))
            html_files.extend(folder_path.glob("*.html"))
    return html_files


def process_single_date(root_dir: Path, date_label: str = None):
    html_files = collect_html_files(root_dir)
    if not html_files:
        return []
    corpus = []
    for html_file in html_files:
        corpus.append(extract_text(html_file, date_label=date_label))
    return corpus


def parse_date_range(arg: str):
    start_s, end_s = arg.split("..")
    start = datetime.strptime(start_s.strip(), "%Y-%m-%d")
    end = datetime.strptime(end_s.strip(), "%Y-%m-%d")
    dates = []
    cur = start
    while cur <= end:
        dates.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    label = f"{start_s.strip()}..{end_s.strip()}"
    return dates, label


def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python extract_corpus_text.py <date_dir>")
        print("  python extract_corpus_text.py <YYYY-MM-DD>..<YYYY-MM-DD>")
        sys.exit(1)

    arg = sys.argv[1]

    if ".." in arg:
        dates, range_label = parse_date_range(arg)
        print(f"Date range: {range_label} ({len(dates)} calendar days)")

        corpus = []
        for date in dates:
            root_dir = Path(date)
            if not root_dir.exists():
                continue
            entries = process_single_date(root_dir, date_label=date)
            if entries:
                print(f"  {date}: {len(entries)} files")
                corpus.extend(entries)

        if not corpus:
            print("No HTML files found across date range")
            sys.exit(1)

        output_file = Path(f"{range_label}.json")
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(corpus, f, indent=2, ensure_ascii=False)

        print(f"\n✓ Saved to {output_file}")
        print(f"  Total files: {len(corpus)}")
        print(f"  Total size: {output_file.stat().st_size / 1024:.1f} KB")

    else:
        root_dir = Path(arg)
        if not (root_dir / "exhibits").exists():
            print(f"Error: {root_dir / 'exhibits'} not found")
            sys.exit(1)

        corpus = process_single_date(root_dir)
        if not corpus:
            print("No HTML files found")
            sys.exit(1)

        print(f"Extracting text from {len(corpus)} files...")

        output_file = root_dir / f"{root_dir.name}.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(corpus, f, indent=2, ensure_ascii=False)

        print(f"\n✓ Saved to {output_file}")
        print(f"  Total files: {len(corpus)}")
        print(f"  Total size: {output_file.stat().st_size / 1024:.1f} KB")


if __name__ == "__main__":
    main()