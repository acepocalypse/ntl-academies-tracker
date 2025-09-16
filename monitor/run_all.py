# monitor/run_all.py
from __future__ import annotations

import logging
import subprocess
import sys
from pathlib import Path
from datetime import datetime
import glob
import os

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
    "3008": "scrapers.nae",  # NAE
    "1909": "scrapers.nam",  # NAM
    "2023": "scrapers.nas",  # NAS

}

# Map award_id -> human-friendly academy name
AWARD_NAMES = {
    "3008": "NAE",
    "1909": "NAM",
    "2023": "NAS",
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
    
    # Log current working directory and environment
    logging.info("Working directory: %s", ROOT)
    logging.info("Python executable: %s", sys.executable)
    
    # Set timeout for up to 8 hours for any scraper
    timeout_hours = 8
    timeout_seconds = timeout_hours * 3600
    
    try:
        result = subprocess.run(
            [sys.executable, "-m", module_name], 
            cwd=ROOT,
            timeout=timeout_seconds,
            stdout=None,  # Allow output to terminal
            stderr=None   # Allow errors to terminal
        )
        rc = result.returncode
    except subprocess.TimeoutExpired:
        logging.error("Scraper module %s timed out after %d hours", module_name, timeout_hours)
        return 1
    except Exception as e:
        logging.error("Failed to run scraper module %s: %s", module_name, e)
        return 1
    
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


def validate_scraper_output(award_id: str, academy_name: str) -> tuple[bool, str]:
    """
    Validate that the scraper actually produced a CSV file.
    Returns (success, diagnostic_message)
    """
    # Look for CSV files in snapshots/{award_id}/ directory structure
    award_dir = SNAPSHOTS_DIR / award_id
    if not award_dir.exists():
        msg = f"No snapshots directory found for award {award_id} at {award_dir}"
        logging.error(msg)
        return False, msg
    
    # Look for timestamped CSV files (YYYYMMDD_HHMMSS.csv pattern)
    csv_files = list(award_dir.glob("*.csv"))
    
    if not csv_files:
        msg = f"No CSV files found for award {award_id} in {award_dir}"
        logging.error(msg)
        return False, msg
    
    # Check the most recent file by modification time
    latest_file = max(csv_files, key=lambda p: p.stat().st_mtime)
    
    # Check file age (should be recent)
    file_age = datetime.now().timestamp() - latest_file.stat().st_mtime
    file_size = latest_file.stat().st_size
    
    logging.info("Latest CSV for %s (%s): %s (size: %d bytes, age: %.1f seconds)", 
                award_id, academy_name, latest_file.name, file_size, file_age)
    
    # Set validation window for up to 9 hours (slightly longer than max run time)
    max_age_hours = 9
    max_age_seconds = max_age_hours * 3600
    
    if file_age > max_age_seconds:
        msg = f"Latest CSV for {award_id} is too old: {file_age:.1f} seconds ({file_age/3600:.1f} hours)"
        logging.warning(msg)
        return False, msg
    
    if file_size == 0:
        msg = f"Latest CSV for {award_id} is empty: {latest_file}"
        logging.error(msg)
        return False, msg
    
    # Try to read the CSV to validate format
    try:
        df = pd.read_csv(latest_file)
        row_count = len(df)
        col_count = len(df.columns)
        logging.info("CSV validation for %s: %d rows, %d columns", award_id, row_count, col_count)
        
        if row_count == 0:
            msg = f"CSV for {award_id} has no data rows"
            logging.warning(msg)
            return False, msg
            
    except Exception as e:
        msg = f"Failed to read CSV for {award_id}: {e}"
        logging.error(msg)
        return False, msg
    
    return True, f"Valid CSV with {row_count} rows, {col_count} columns"


def log_snapshots_directory_status():
    """Log the current state of the snapshots directory for debugging."""
    logging.info("Snapshots directory status: %s", SNAPSHOTS_DIR)
    
    if not SNAPSHOTS_DIR.exists():
        logging.error("Snapshots directory does not exist!")
        return
    
    # List all award subdirectories and their contents
    for award_dir in SNAPSHOTS_DIR.iterdir():
        if award_dir.is_dir() and award_dir.name.isdigit():
            csv_files = list(award_dir.glob("*.csv"))
            logging.info("Award %s directory: %d CSV files", award_dir.name, len(csv_files))
            for csv_file in sorted(csv_files, key=lambda p: p.stat().st_mtime, reverse=True)[:3]:  # Show 3 most recent
                size = csv_file.stat().st_size
                mtime = datetime.fromtimestamp(csv_file.stat().st_mtime)
                logging.info("  %s (size: %d, modified: %s)", csv_file.name, size, mtime)


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    settings = read_settings()
    award_ids = settings.get("general", {}).get("awards", list(AWARD_MODULES.keys()))
    ignore_fields = settings.get("diff", {}).get("ignore_fields", DEFAULT_IGNORE_FIELDS)

    run_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logging.info("=== Weekly run start: %s ===", run_ts)
    logging.info("Award IDs to process: %s", award_ids)
    
    # Log initial state
    log_snapshots_directory_status()

    lines: list[str] = [f"Run at {run_ts}\n"]

    any_failures = False
    all_diff_files: list[str] = []

    for aid in award_ids:
        module = AWARD_MODULES.get(aid)
        academy_name = AWARD_NAMES.get(aid, aid)
        
        logging.info("Processing award %s (%s) with module %s", aid, academy_name, module)
        print(f"Starting {academy_name} ({aid}) scraper...")  # Add console output
        
        if not module:
            msg = f"• {aid} ({academy_name}): no scraper module mapping found"
            logging.error(msg)
            lines.append(msg)
            any_failures = True
            continue

        # 1) Execute scraper
        rc = run_scraper(module)
        print(f"Scraper {academy_name} ({aid}) finished with return code: {rc}")  # Add console output
        
        # 2) Validate scraper output regardless of return code
        output_valid, validation_msg = validate_scraper_output(aid, academy_name)
        print(f"Validation for {academy_name} ({aid}): {validation_msg}")  # Add console output
        
        if rc != 0:
            msg = f"• {aid}: scraper FAILED (rc={rc}) - {validation_msg}"
            logging.error(msg)
            lines.append(msg)
            any_failures = True
            continue
        elif not output_valid:
            msg = f"• {aid}: scraper completed but output invalid - {validation_msg}"
            logging.error(msg)
            lines.append(msg)
            any_failures = True
            continue
        else:
            logging.info("Scraper for %s (%s) completed successfully: %s", aid, academy_name, validation_msg)

        # 3) Diff latest two snapshots
        try:
            prev, curr, prev_path, curr_path = load_latest_two(
                award_id=aid,
                base=str(SNAPSHOTS_DIR),
                ignore_fields=ignore_fields,
            )
        except Exception as e:
            msg = f"• {aid} ({academy_name}): failed to load snapshots - {e}"
            logging.error(msg)
            lines.append(msg)
            any_failures = True
            continue

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
