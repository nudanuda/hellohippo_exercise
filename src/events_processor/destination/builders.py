from __future__ import annotations

from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List

Q2 = Decimal("0.01")


def _d2(x: Decimal) -> float:
    return float(x.quantize(Q2, rounding=ROUND_HALF_UP))


def build_goal2_metrics(state) -> List[dict]:
    rows = []
    for (npi, ndc), agg in state.goal2.snapshot().items():
        if agg.active_cnt > 0:
            avg = agg.active_unit_price_sum / Decimal(agg.active_cnt)
            avg_price = _d2(avg)
        else:
            avg_price = 0.0

        rows.append(
            {
                "npi": npi,
                "ndc": ndc,
                "fills": int(agg.fills),  # все claims
                "reverted": int(agg.reverted),
                "avg_price": avg_price,  # active-only
                "total_price": float(agg.active_total_price_sum),  # active-only
            }
        )

    rows.sort(key=lambda r: (r["npi"], r["ndc"]))
    return rows


def build_goal3_top2_chains(state) -> List[dict]:
    """
    Goal3 derived view: compute top-2 cheapest chains per ndc
    from Goal2 aggregates + pharmacies snapshot.
    """

    # (ndc, chain) -> [active_cnt, active_unit_price_sum]
    by_ndc_chain = defaultdict(lambda: [0, Decimal("0")])

    for (npi, ndc), agg in state.goal2.snapshot().items():
        if agg.active_cnt <= 0:
            continue

        chain = state.pharmacy_chain_by_npi.get(npi)
        if not chain:
            continue

        bucket = by_ndc_chain[(ndc, chain)]
        bucket[0] += agg.active_cnt
        bucket[1] += agg.active_unit_price_sum

    # ndc -> list[(chain, avg_unit_price)]
    by_ndc = defaultdict(list)
    for (ndc, chain), (cnt, unit_sum) in by_ndc_chain.items():
        avg = unit_sum / Decimal(cnt)
        by_ndc[ndc].append((chain, avg))

    result = []
    for ndc, items in by_ndc.items():
        items.sort(key=lambda t: (t[1], t[0]))  # avg asc, chain name asc
        top2 = items[:2]
        result.append(
            {
                "ndc": ndc,
                "chain": [{"name": ch, "avg_price": _d2(avg)} for ch, avg in top2],
            }
        )

    result.sort(key=lambda r: r["ndc"])
    return result


def build_goal4_top_quantities(state) -> List[dict]:
    out = []

    def q_to_num(qs: str) -> Decimal:
        try:
            return Decimal(qs)
        except Exception:
            return Decimal("0")

    for ndc, qmap in state.goal4.snapshot().items():
        items = [(q, c) for q, c in qmap.items() if c > 0]
        items.sort(key=lambda t: (-t[1], q_to_num(t[0])))  # count desc, qty asc
        top5 = items[:5]

        out.append(
            {
                "ndc": ndc,
                "most_prescribed_quantity": [float(q_to_num(q)) for q, _ in top5],
            }
        )

    out.sort(key=lambda r: r["ndc"])
    return out
