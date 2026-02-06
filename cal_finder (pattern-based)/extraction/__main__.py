#!/usr/bin/env python3
import sys
import logging
from pathlib import Path
from .extract_fields import run_pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python -m extraction <YYYY-MM-DD>          # corpus mode")
        print("  python -m extraction <file.xlsx|file.csv>   # csv mode")
        sys.exit(1)
    
    arg = sys.argv[1]
    
    if arg.endswith(('.xlsx', '.csv')):
        input_path = Path(arg)
        if not input_path.exists():
            sys.exit(f"File not found: {input_path}")
        
        output_dir = Path(input_path.stem)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        from .csv_xlsx_download import run as download_samples
        download_samples(input_path, output_dir)
        
        root = output_dir
    else:
        root = Path(arg)
    
    mapping = Path(sys.argv[2]) if len(sys.argv) > 2 else None
    success = run_pipeline(root, mapping, verbose=True)
    sys.exit(0 if success else 1)