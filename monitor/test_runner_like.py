"""
monitor/test_runner_like.py

A lightweight test runner that mimics the core behavior of monitor/run_all.py
without invoking the actual scrapers or sending notifications.

It:
- Detects awards with at least two CSV snapshots in `snapshots/<award_id>/`
- Loads the latest two, normalizes, computes diffs
- Writes diff CSVs next to the latest snapshot
- Logs and prints a concise summary per award

Usage examples:
- python monitor/test_runner_like.py            # auto-detect awards with >=2 snapshots
- python monitor/test_runner_like.py 3008 2023  # specify award IDs explicitly

Exit codes:
- 0 if at least one award was processed successfully
- 1 if none were processed, or if an unexpected exception occurs
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Iterable, List

import pandas as pd

# Add project root to sys.path for absolute imports
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from monitor.diff_utils import (
    SNAPSHOTS_DIR,
    list_snapshots,
    load_latest_two,
    compute_diff,
    diff_summary_str,
    write_diff_csvs,
)


# Keep defaults in sync with run_all.py where relevant
DEFAULT_IGNORE_FIELDS = ["location"]


def configure_logging() -> None:
    log_dir = ROOT / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        filename=log_dir / "test_runs.log",
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def summarize_diff_paths(curr_path: Path) -> str:
    base = curr_path.with_suffix("")
    parts: List[str] = []
    for suffix in ("__added.csv", "__removed.csv", "__modified.csv"):
        p = base.with_name(base.name + suffix)
        if p.exists() and p.stat().st_size > 0:
            parts.append(p.name)
    return ", ".join(parts) if parts else "(no diff files)"


def auto_awards_with_two_snaps() -> List[str]:
    snaps_root = Path(SNAPSHOTS_DIR)
    if not snaps_root.exists():
        return []
    award_ids: List[str] = []
    for child in snaps_root.iterdir():
        if not child.is_dir():
            continue
        snaps = list_snapshots(child.name, base=SNAPSHOTS_DIR)
        if len(snaps) >= 2:
            award_ids.append(child.name)
    return sorted(award_ids)


def process_award(award_id: str, ignore_fields: Iterable[str]) -> str:
    prev, curr, prev_path, curr_path = load_latest_two(
        award_id=award_id,
        base=SNAPSHOTS_DIR,
        ignore_fields=list(ignore_fields),
    )

    if curr_path is None:
        msg = f"- {award_id}: no snapshots found"
        logging.warning(msg)
        return msg

    if prev_path is None:
        msg = f"- {award_id}: first snapshot {curr_path.name} (no diff)"
        logging.info(msg)
        return msg

    diff = compute_diff(prev, curr, ignore_fields=list(ignore_fields))
    summary = diff_summary_str(diff)
    written = write_diff_csvs(diff, curr_path.with_suffix(""))
    _ = written  # not used beyond summarizing
    written_str = summarize_diff_paths(curr_path)

    msg = f"- {award_id}: {curr_path.name}  {summary}  {written_str}"
    logging.info(msg)
    return msg


def main(argv: List[str]) -> int:
    configure_logging()

    # Accept award IDs as CLI args, else auto-detect those with >= 2 snapshots
    awards = [a for a in argv if a.strip()]
    if awards:
        logging.info("Using awards from CLI: %s", awards)
    else:
        awards = auto_awards_with_two_snaps()
        logging.info("Auto-detected awards with >=2 snapshots: %s", awards)

    if not awards:
        msg = "No awards with >=2 snapshots found. Nothing to do."
        logging.warning(msg)
        print(msg)
        return 1

    lines: List[str] = []
    for aid in awards:
        try:
            line = process_award(aid, ignore_fields=DEFAULT_IGNORE_FIELDS)
            lines.append(line)
        except Exception as ex:  # keep going across awards
            logging.exception("Error processing award %s: %s", aid, ex)
            lines.append(f"- {aid}: ERROR: {ex}")

    # Print summary to console for quick inspection
    summary_body = "\n".join(lines)
    print(summary_body)

    # Successful if at least one award produced a line (even if some had errors)
    ok = any(not s.lower().startswith("- ") or "error" not in s.lower() for s in lines)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

