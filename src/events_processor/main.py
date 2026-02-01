import argparse
import time
from pathlib import Path

from events_processor.core.state import InMemoryState
from events_processor.core.processor import EventProcessor

from events_processor.sources.discover import discover_files
from events_processor.sources.pharmacies import load_pharmacies_csv
from events_processor.sources.events_json import iter_claim_events, iter_revert_events
from events_processor.sources.streaming import FileStreamWatcher

from events_processor.destination.builders import (
    build_goal2_metrics,
    build_goal3_top2_chains,
    build_goal4_top_quantities,
)
from events_processor.destination.writer import write_json_atomic


def process_files(processor: EventProcessor, claim_files: list[Path], revert_files: list[Path]) -> None:
    for ev in iter_claim_events(claim_files):
        processor.handle(ev)

    for ev in iter_revert_events(revert_files):
        processor.handle(ev)


def write_outputs(out_dir: Path, state: InMemoryState) -> None:
    write_json_atomic(out_dir / "metrics_by_npi_ndc.json", build_goal2_metrics(state))
    write_json_atomic(out_dir / "top2_chain_per_ndc.json", build_goal3_top2_chains(state))
    write_json_atomic(out_dir / "most_common_qty_per_ndc.json", build_goal4_top_quantities(state))


def main() -> None:
    parser = argparse.ArgumentParser("claims-processor")
    parser.add_argument("--pharmacies", nargs="+", required=True, help="Dirs with pharmacy CSV files")
    parser.add_argument("--claims", nargs="+", required=True, help="Dirs with claims JSON files")
    parser.add_argument("--reverts", nargs="+", required=True, help="Dirs with reverts JSON files")
    parser.add_argument("--out", required=True, help="Output directory")
    parser.add_argument("--streaming", action="store_true", help="Watch directories for new files")
    parser.add_argument("--poll-interval", type=int, default=120, help="Polling interval seconds for --streaming")

    args = parser.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # load pharmacy snapshot
    pharm_files = discover_files(args.pharmacies).csv_files
    state = InMemoryState()
    state.pharmacy_chain_by_npi = load_pharmacies_csv(pharm_files)

    processor = EventProcessor(state)

    # batch mode
    if not args.streaming:
        claim_files = discover_files(args.claims).json_files
        revert_files = discover_files(args.reverts).json_files

        process_files(processor, claim_files, revert_files)
        write_outputs(out_dir, state)

        print("Done.")
        print("Counters:", processor.counters)
        return

    # streaming mode
    watcher = FileStreamWatcher(
        claim_dirs=[Path(p) for p in args.claims],
        revert_dirs=[Path(p) for p in args.reverts],
    )

    print("Running in --streaming mode (Ctrl+C to stop)")
    try:
        while True:
            new_claim_files, new_revert_files = watcher.discover_new_files()

            if new_claim_files or new_revert_files:
                process_files(processor, new_claim_files, new_revert_files)
                write_outputs(out_dir, state)
                print(f"Processed new files: claims={len(new_claim_files)}, reverts={len(new_revert_files)}")

            time.sleep(args.poll_interval)

    except KeyboardInterrupt:
        print("Final counters:", processor.counters)


if __name__ == "__main__":
    main()
