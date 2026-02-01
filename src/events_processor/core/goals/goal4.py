from collections import defaultdict
from typing import Dict, DefaultDict

from ..models import ClaimRecord


class Goal4Quantity:
    def __init__(self) -> None:
        self._counts: DefaultDict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))

    def on_claim(self, cr: ClaimRecord) -> None:
        self._counts[cr.ndc][cr.quantity_key] += 1

    def on_revert(self, cr: ClaimRecord) -> None:
        m = self._counts.get(cr.ndc)
        if not m:
            return
        m[cr.quantity_key] -= 1
        if m[cr.quantity_key] <= 0:
            del m[cr.quantity_key]
        if not m:
            del self._counts[cr.ndc]

    def snapshot(self) -> Dict[str, Dict[str, int]]:
        return self._counts
