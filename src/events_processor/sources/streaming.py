from pathlib import Path
from typing import Iterable, List, Tuple, Set

from .discover import discover_files


class FileStreamWatcher:
    """
    Tracks newly appeared JSON files in claims and reverts directories.
    Intended for --streaming mode.
    """

    def __init__(self, claim_dirs: Iterable[Path], revert_dirs: Iterable[Path]):
        self.claim_dirs = {p.resolve() for p in claim_dirs}
        self.revert_dirs = {p.resolve() for p in revert_dirs}

        self._seen_claim_files: Set[Path] = set()
        self._seen_revert_files: Set[Path] = set()

    def discover_new_files(self) -> Tuple[List[Path], List[Path]]:
        discovered = discover_files(
            [str(p) for p in self.claim_dirs | self.revert_dirs]
        )

        new_claims: List[Path] = []
        new_reverts: List[Path] = []

        for fp in discovered.json_files:
            fp = fp.resolve()

            if fp.parent in self.claim_dirs and fp not in self._seen_claim_files:
                new_claims.append(fp)
                self._seen_claim_files.add(fp)

            elif fp.parent in self.revert_dirs and fp not in self._seen_revert_files:
                new_reverts.append(fp)
                self._seen_revert_files.add(fp)

        return new_claims, new_reverts
