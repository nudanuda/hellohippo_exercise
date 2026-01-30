from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List


@dataclass(frozen=True, slots=True)
class DiscoveredFiles:
    json_files: List[Path]
    csv_files: List[Path]


def discover_files(dirs: Iterable[str], recursive: bool = True) -> DiscoveredFiles:
    json_files: List[Path] = []
    csv_files: List[Path] = []

    for d in dirs:
        p = Path(d)
        if not p.exists():
            continue
        if p.is_file():
            if p.suffix.lower() == ".json":
                json_files.append(p)
            elif p.suffix.lower() == ".csv":
                csv_files.append(p)
            continue

        pattern = "**/*" if recursive else "*"
        for f in p.glob(pattern):
            if not f.is_file():
                continue
            suf = f.suffix.lower()
            if suf == ".json":
                json_files.append(f)
            elif suf == ".csv":
                csv_files.append(f)

    json_files.sort()
    csv_files.sort()
    return DiscoveredFiles(json_files=json_files, csv_files=csv_files)
