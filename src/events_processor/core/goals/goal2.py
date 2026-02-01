from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, Tuple

from ..models import ClaimRecord


@dataclass(slots=True)
class MetricsAgg:
    fills: int = 0
    reverted: int = 0
    active_cnt: int = 0
    active_unit_price_sum: Decimal = Decimal("0")
    active_total_price_sum: Decimal = Decimal("0")


class Goal2Metrics:
    def __init__(self) -> None:
        self._by_npi_ndc: Dict[Tuple[str, str], MetricsAgg] = {}

    def on_claim(self, cr: ClaimRecord) -> None:
        key = (cr.npi, cr.ndc)
        agg = self._by_npi_ndc.get(key)
        if agg is None:
            agg = MetricsAgg()
            self._by_npi_ndc[key] = agg

        agg.fills += 1
        agg.active_cnt += 1
        agg.active_unit_price_sum += cr.unit_price
        agg.active_total_price_sum += cr.price

    def on_revert(self, cr: ClaimRecord) -> None:
        key = (cr.npi, cr.ndc)
        agg = self._by_npi_ndc.get(key)
        if agg is None:
            agg = MetricsAgg()
            self._by_npi_ndc[key] = agg

        agg.reverted += 1
        # subtract active contributions
        agg.active_cnt -= 1
        agg.active_unit_price_sum -= cr.unit_price
        agg.active_total_price_sum -= cr.price

    def snapshot(self) -> Dict[Tuple[str, str], MetricsAgg]:
        return self._by_npi_ndc
