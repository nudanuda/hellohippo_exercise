from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from .events import ClaimEvent, RevertEvent
from .models import ClaimRecord
from .state import InMemoryState


@dataclass(slots=True)
class Counters:
    """
    Runtime counters describing data quality and event stream behaviour.
    These counters are informational and do not affect business logic.
    They help to understand characteristics of the input data such as
    duplicates, missing references and event ordering issues.
    """
    duplicate_claims: int = 0
    duplicate_reverts: int = 0
    unknown_pharmacy_claims: int = 0
    orphan_reverts: int = 0
    already_reverted: int = 0


class EventProcessor:
    """
    Stateful event processor for claim and revert events.

    The processor is idempotent and order-agnostic:
    - duplicate events are ignored
    - reverts arriving before claims are handled correctly
    - only claims from known pharmacies are processed
    """
    def __init__(self, state: InMemoryState) -> None:
        self.state = state
        self.counters = Counters()

    def handle(self, event) -> None:
        if isinstance(event, ClaimEvent):
            self._on_claim(event)
        elif isinstance(event, RevertEvent):
            self._on_revert(event)

    def _on_claim(self, e: ClaimEvent) -> None:
        if e.id in self.state.seen_claim_ids:
            self.counters.duplicate_claims += 1
            return
        self.state.seen_claim_ids.add(e.id)

        chain = self.state.pharmacy_chain_by_npi.get(e.npi)
        if chain is None:
            self.counters.unknown_pharmacy_claims += 1
            return

        quantity_key = self._normalize_decimal_key(e.quantity)

        cr = ClaimRecord(
            claim_id=e.id,
            npi=e.npi,
            ndc=e.ndc,
            chain=chain,
            price=e.price,
            quantity_key=quantity_key,
            unit_price=e.unit_price,
            is_reverted=False,
        )
        self.state.claims[e.id] = cr

        # apply as active claim
        self.state.goal2.on_claim(cr)
        #self.state.goal3.on_claim(cr)
        self.state.goal4.on_claim(cr)

        # if revert came before claim
        pending = self.state.pending_reverts.pop(e.id, 0)
        if pending > 0:
            self._revert_claim_if_active(e.id)
            if pending > 1:
                # extra reverts for same claim_id that arrived before claim
                self.counters.already_reverted += (pending - 1)

    def _on_revert(self, e: RevertEvent) -> None:
        if e.id in self.state.seen_revert_ids:
            self.counters.duplicate_reverts += 1
            return
        self.state.seen_revert_ids.add(e.id)

        if e.claim_id in self.state.claims:
            ok = self._revert_claim_if_active(e.claim_id)
            if not ok:
                self.counters.already_reverted += 1
        else:
            self.state.pending_reverts[e.claim_id] += 1

    def _revert_claim_if_active(self, claim_id: str) -> bool:
        cr = self.state.claims.get(claim_id)
        if cr is None:
            self.counters.orphan_reverts += 1
            return False
        if cr.is_reverted:
            return False

        cr.is_reverted = True
        self.state.goal2.on_revert(cr)
        #self.state.goal3.on_revert(cr)
        self.state.goal4.on_revert(cr)
        return True

    @staticmethod
    def _normalize_decimal_key(x: Decimal) -> str:
        s = format(x, "f")
        if "." in s:
            s = s.rstrip("0").rstrip(".")
        return s
