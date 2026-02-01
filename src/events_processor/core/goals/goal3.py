from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Tuple

Q2 = Decimal("0.01")

def _d2(x: Decimal) -> float:
    return float(x.quantize(Q2, rounding=ROUND_HALF_UP))


def build_goal3_from_goal2(
    goal2_snapshot: Dict[Tuple[str, str], any],
    pharmacy_chain_by_npi: Dict[str, str],
) -> List[dict]:
    """
    Build Goal 3 (top 2 cheapest chains per drug)
    from Goal 2 aggregates.
    """

    # (ndc, chain) -> [cnt, sum_unit_price]
    by_ndc_chain = defaultdict(lambda: [0, Decimal("0")])

    for (npi, ndc), agg in goal2_snapshot.items():
        if agg.active_cnt <= 0:
            continue

        chain = pharmacy_chain_by_npi.get(npi)
        if not chain:
            continue

        bucket = by_ndc_chain[(ndc, chain)]
        bucket[0] += agg.active_cnt
        bucket[1] += agg.active_unit_price_sum

    # ndc -> list of (chain, avg)
    by_ndc = defaultdict(list)
    for (ndc, chain), (cnt, s) in by_ndc_chain.items():
        avg = s / Decimal(cnt)
        by_ndc[ndc].append((chain, avg))

    result = []
    for ndc, items in by_ndc.items():
        items.sort(key=lambda x: (x[1], x[0]))  # cheapest first
        top2 = items[:2]

        result.append(
            {
                "ndc": ndc,
                "chain": [
                    {"name": chain, "avg_price": _d2(avg)}
                    for chain, avg in top2
                ],
            }
        )

    result.sort(key=lambda r: r["ndc"])
    return result