from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class ClaimEvent:
    id: str
    npi: str
    ndc: str
    price: Decimal
    quantity: Decimal
    unit_price: Decimal
    timestamp: datetime


@dataclass(frozen=True, slots=True)
class RevertEvent:
    id: str
    claim_id: str
    timestamp: datetime
