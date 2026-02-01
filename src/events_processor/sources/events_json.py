import json
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, Iterable, Iterator, Optional, Any

from ..core.events import ClaimEvent, RevertEvent


def iter_claim_events(json_files: Iterable[Path]) -> Iterator[ClaimEvent]:
    for fp in json_files:
        for obj in _iter_json_objects(fp):
            ev = _parse_claim(obj)
            if ev is not None:
                yield ev


def iter_revert_events(json_files: Iterable[Path]) -> Iterator[RevertEvent]:
    for fp in json_files:
        for obj in _iter_json_objects(fp):
            ev = _parse_revert(obj)
            if ev is not None:
                yield ev


def _iter_json_objects(fp: Path) -> Iterator[Dict[str, Any]]:
    """
    Iterates over JSON objects in a file.
    Supports:
    - JSON lines (one object per line)
    - JSON arrays
    - Single JSON object files
    Malformed files or records are skipped.
    """
    try:
        with fp.open("r", encoding="utf-8") as f:
            first_char = None
            pos = f.tell()
            while True:
                ch = f.read(1)
                if ch == "":
                    break
                if not ch.isspace():
                    first_char = ch
                    break
            f.seek(pos)

            if first_char == "{":
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(obj, dict):
                        yield obj
                return

            try:
                data = json.load(f)
            except json.JSONDecodeError:
                return

            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        yield item
            elif isinstance(data, dict):
                yield data

    except OSError:
        return


def _parse_claim(obj: Dict[str, Any]) -> Optional[ClaimEvent]:
    try:
        claim_id = str(obj.get("id", "")).strip()
        npi = str(obj.get("npi", "")).strip()
        ndc = str(obj.get("ndc", "")).strip()
        ts_raw = obj.get("timestamp")

        if not claim_id or not npi or not ndc or ts_raw is None:
            return None

        ts = _parse_iso_datetime(ts_raw)
        if ts is None:
            return None

        quantity = _parse_decimal(obj.get("quantity"))
        price = _parse_decimal(obj.get("price"))
        if quantity is None or price is None:
            return None
        if quantity <= 0 or price < 0:
            return None

        unit_price = price / quantity

        return ClaimEvent(
            id=claim_id,
            npi=npi,
            ndc=ndc,
            price=price,
            quantity=quantity,
            unit_price=unit_price,
            timestamp=ts,
        )
    except Exception:
        return None


def _parse_revert(obj: Dict[str, Any]) -> Optional[RevertEvent]:
    try:
        revert_id = str(obj.get("id", "")).strip()
        claim_id = str(obj.get("claim_id", "")).strip()
        ts_raw = obj.get("timestamp")

        if not revert_id or not claim_id or ts_raw is None:
            return None

        ts = _parse_iso_datetime(ts_raw)
        if ts is None:
            return None

        return RevertEvent(
            id=revert_id,
            claim_id=claim_id,
            timestamp=ts,
        )
    except Exception:
        return None


def _parse_decimal(x: Any) -> Optional[Decimal]:
    if x is None:
        return None
    try:
        return Decimal(str(x))
    except (InvalidOperation, ValueError):
        return None


def _parse_iso_datetime(x: Any) -> Optional[datetime]:
    if x is None:
        return None
    if isinstance(x, datetime):
        return x
    try:
        return datetime.fromisoformat(str(x))
    except ValueError:
        return None
