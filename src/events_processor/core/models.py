from dataclasses import dataclass
from decimal import Decimal


@dataclass(slots=True)
class ClaimRecord:
    claim_id: str
    npi: str
    ndc: str
    chain: str
    price: Decimal
    quantity_key: str  # normalized string for quantity (stable dict keys)
    unit_price: Decimal
    is_reverted: bool = False
