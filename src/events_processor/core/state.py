from __future__ import annotations

from dataclasses import dataclass, field
from collections import defaultdict
from typing import Dict, Set, DefaultDict

from .models import ClaimRecord
from .goals.goal2 import Goal2Metrics
from .goals.goal4 import Goal4Quantity


@dataclass(slots=True)
class InMemoryState:
    # pharmacy snapshot: npi -> chain
    pharmacy_chain_by_npi: Dict[str, str] = field(default_factory=dict)

    # claim store (only for known pharmacies)
    claims: Dict[str, ClaimRecord] = field(default_factory=dict)

    # dedup sets
    seen_claim_ids: Set[str] = field(default_factory=set)
    seen_revert_ids: Set[str] = field(default_factory=set)

    # revert-before-claim: claim_id -> count
    pending_reverts: DefaultDict[str, int] = field(default_factory=lambda: defaultdict(int))

    # goals
    goal2: Goal2Metrics = field(default_factory=Goal2Metrics)
    goal4: Goal4Quantity = field(default_factory=Goal4Quantity)
