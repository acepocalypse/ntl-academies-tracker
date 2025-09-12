# monitor/run_all.py
from __future__ import annotations

import logging
import subprocess
import sys
from pathlib import Path
from datetime import datetime

import pandas as pd

# Py 3.11+: tomllib is stdlib; fallback to tomli for 3.10/3.9
try:
    import tomllib  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    import tomli as tomllib  # type: ignore[no-redef]

# Add project root to sys.path for absolute imports
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from monitor.diff_utils import (
    load_latest_two,
    compute_diff,
    diff_summary_str,
    write_diff_csvs,
    SNAPSHOTS_DIR,
)
from monitor.notify import email_notify

# Ensure SNAPSHOTS_DIR is a Path object
if isinstance(SNAPSHOTS_DIR, str):
    SNAPSHOTS_DIR = Path(SNAPSHOTS_DIR)

# ----------------------------
# Config
# ----------------------------
# ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOG_DIR / "runs.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# Map award_id -> scraper module path (python -m <module>)
AWARD_MODULES = {
    "1909": "scrapers.nam",  # NAM
    "2023": "scrapers.nas",  # NAS
    "3008": "scrapers.nae",  # NAE
}

# Map award_id -> human-friendly academy name
AWARD_NAMES = {
    "1909": "NAM",
    "2023": "NAS",
    "3008": "NAE",
}

DEFAULT_IGNORE_FIELDS = ["location"]  # tweak as needed


# ----------------------------
# Helpers
# ----------------------------
def read_settings() -> dict:
    """Load settings.toml if it exists, else return defaults."""
    cfg_path = ROOT / "monitor" / "settings.toml"
    if not cfg_path.exists():
        logging.warning("settings.toml not found; using default settings.")
        return {
            "general": {"awards": list(AWARD_MODULES.keys())},
            "diff": {"ignore_fields": DEFAULT_IGNORE_FIELDS},
            "notify": {"method": "none"},  # disables notifications
        }
    with cfg_path.open("rb") as f:
        return tomllib.load(f)


def run_scraper(module_name: str) -> int:
    """
    Run a scraper module as: python -m <module_name>
    Returns the process return code.
    """
    logging.info("Running scraper module: %s", module_name)
    rc = subprocess.call([sys.executable, "-m", module_name], cwd=ROOT)
    logging.info("Scraper module finished (%s) rc=%d", module_name, rc)
    return rc


def send_notification(settings: dict, title: str, body: str, attachments: list[str] = None) -> None:
    method = settings.get("notify", {}).get("method", "none")
    try:
        if method == "email":
            e = settings.get("email", {})
            to_addrs = e.get("to", [])
            if isinstance(to_addrs, str):
                to_addrs = [to_addrs]
            email_notify(
                subject=title,
                body=body,
                to_addrs=to_addrs,
                attachments=attachments or [],
            )
        elif method == "none":
            pass  # No notification
        else:
            logging.warning("Unknown notify method '%s'; skipping notification.", method)
    except Exception as ex:
        logging.exception("Notification failed: %s", ex)


def summarize_diff_paths(award_id: str, base_name: str) -> str:
    """
    Return a short string with links/paths to any diff CSVs written for this snapshot.
    Each file is listed on its own line.
    """
    diff_dir = SNAPSHOTS_DIR / "diffs"
    parts = []
    for suffix in ("added", "removed", "modified"):
        p = diff_dir / f"{award_id}__{base_name}__{suffix}.csv"
        if p.exists() and p.stat().st_size > 0:
            parts.append(p.name)
    # List each file on its own line, or show (no diff files)
    return "\n".join(parts) if parts else "(no diff files)"


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    settings = read_settings()
    award_ids = settings.get("general", {}).get("awards", list(AWARD_MODULES.keys()))
    ignore_fields = settings.get("diff", {}).get("ignore_fields", DEFAULT_IGNORE_FIELDS)

    run_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logging.info("=== Weekly run start: %s ===", run_ts)

    lines: list[str] = [f"Run at {run_ts}\n"]

    any_failures = False
    all_diff_files: list[str] = []

    for aid in award_ids:
        module = AWARD_MODULES.get(aid)
        academy_name = AWARD_NAMES.get(aid, aid)
        if not module:
            msg = f"• {aid} ({academy_name}): no scraper module mapping found"
            logging.error(msg)
            lines.append(msg)
            any_failures = True
            continue

        # 1) Execute scraper
        rc = run_scraper(module)
        if rc != 0:
            msg = f"• {aid}: scraper FAILED (rc={rc})"
            logging.error(msg)
            lines.append(msg)
            any_failures = True
            continue

        # 2) Diff latest two snapshots
        prev, curr, prev_path, curr_path = load_latest_two(
            award_id=aid,
            base=str(SNAPSHOTS_DIR),
            ignore_fields=ignore_fields,
        )

        if curr_path is None:
            msg = f"• {aid} ({academy_name}): no snapshots found after run"
            logging.warning(msg)
            lines.append(msg)
            continue

        # Skip if curr_path is not a valid snapshot (e.g., is a diff file or log file)
        if any(curr_path.name.endswith(suffix) for suffix in ("__removed.csv", "__added.csv", "__modified.csv", ".log")):
            msg = f"• {aid} ({academy_name}): skipping non-snapshot file {curr_path.name}"
            logging.warning(msg)
            lines.append(msg)
            continue

        if prev_path is None:
            msg = f"• {aid} ({academy_name}): first snapshot {curr_path.name} (no diff yet)"
            logging.info(msg)
            lines.append(msg)
            continue

        diff = compute_diff(prev, curr, ignore_fields=ignore_fields)
        summary = diff_summary_str(diff)
        # 3) Write diff CSVs (if non-empty)
        diff_dir = SNAPSHOTS_DIR / "diffs"
        diff_dir.mkdir(parents=True, exist_ok=True)

        # --- New: Use clean output file naming ---
        # Extract timestamp from curr_path.name (assume format: YYYYMMDD_HHMMSS or similar)
        # If not present, use current time
        # Remove any extension from curr_path.name
        base_name = curr_path.stem
        # Try to extract timestamp from curr_path.name, fallback to now
        # If curr_path.name is like 20250910_140441.csv, base_name is 20250910_140441
        # If not, fallback to datetime.now()
        try:
            datetime.strptime(base_name[:15], "%Y%m%d_%H%M%S")
            timestamp = base_name[:15]
        except Exception:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = timestamp

        diff_files = {}
        for key in ("added", "removed", "modified"):
            diff_files[key] = diff_dir / f"{aid}__{base_name}__{key}.csv"

        written = write_diff_csvs(diff, diff_dir / f"{aid}__{base_name}")
        written_str = summarize_diff_paths(aid, base_name)

        # Collect diff file paths for attachments
        for key in ("added", "removed", "modified"):
            p = diff_files[key]
            if p.exists() and p.stat().st_size > 0:
                all_diff_files.append(str(p))

        msg = f"• {aid} ({academy_name}): {base_name} — {summary}"
        if written_str != "(no diff files)":
            msg += "\n" + written_str
        logging.info(msg)
        # Add a blank line before each award except the first
        if len(lines) > 1:
            lines.append("")
        lines.append(msg)

    # 4) Notification summary
    title = "National Academies Membership Tracker — Weekly run complete"
    body = "\n".join(lines)
    logging.info(body)
    send_notification(settings, title, body, attachments=all_diff_files)

    # Also print to console for local runs
    print(body)

    # Exit code reflects failures (useful for CI/schedulers)
    if any_failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
