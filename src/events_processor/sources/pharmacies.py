import csv
from pathlib import Path
from typing import Dict, Iterable


def load_pharmacies_csv(files: Iterable[Path]) -> Dict[str, str]:
    """
    Reads pharmacy snapshot from CSV files with headers like: chain,npi
    Returns: npi -> chain
    If duplicates exist, last one wins.
    """
    out: Dict[str, str] = {}

    for fp in files:
        with fp.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            # expected columns: chain, npi
            for row in reader:
                chain = (row.get("chain") or "").strip()
                npi = (row.get("npi") or "").strip()
                if not chain or not npi:
                    continue
                out[npi] = chain
    return out
