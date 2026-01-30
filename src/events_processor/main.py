from __future__ import annotations

import argparse
from pathlib import Path

from events_processor.core.state import InMemoryState
from events_processor.core.processor import EventProcessor

from events_processor.sources.discover import discover_files
from events_processor.sources.pharmacies import load_pharmacies_csv
from events_processor.sources.events_json import iter_claim_events, iter_revert_events

from events_processor.destination.builders import (
    build_goal2_metrics,
    build_goal3_top2_chains,
    build_goal4_top_quantities,
)
from events_processor.destination.writer import write_json_atomic


def main() -> None:
    parser = argparse.ArgumentParser("claims-processor")
    parser.add_argument("--pharmacies", nargs="+", required=True, help="Dirs with pharmacy CSV files")
    parser.add_argument("--claims", nargs="+", required=True, help="Dirs with claims JSON files")
    parser.add_argument("--reverts", nargs="+", required=True, help="Dirs with reverts JSON files")
    parser.add_argument("--out", required=True, help="Output directory")
    args = parser.parse_args()

    # get data
    pharm_files = discover_files(args.pharmacies).csv_files
    claim_files = discover_files(args.claims).json_files
    revert_files = discover_files(args.reverts).json_files

    # add pharmacies to state
    state = InMemoryState()
    state.pharmacy_chain_by_npi = load_pharmacies_csv(pharm_files)

    # process events
    processor = EventProcessor(state)

    for ev in iter_claim_events(claim_files):
        processor.handle(ev)

    for ev in iter_revert_events(revert_files):
        processor.handle(ev)

    # write to destination
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    write_json_atomic(out_dir / "metrics_by_npi_ndc.json", build_goal2_metrics(state))
    write_json_atomic(out_dir / "top2_chain_per_ndc.json", build_goal3_top2_chains(state))
    write_json_atomic(out_dir / "most_common_qty_per_ndc.json", build_goal4_top_quantities(state))

    # stats
    print("Done.")
    print("Counters:", processor.counters)


if __name__ == "__main__":
    main()
