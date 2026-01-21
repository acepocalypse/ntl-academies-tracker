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

try:
    from monitor.diff_utils import (
        load_latest_two,
        compute_diff,
        diff_summary_str,
        write_diff_csvs,
        SNAPSHOTS_DIR,
    )
    from monitor.notify import email_notify
    from monitor.removal_verifier import verify_removed_rows
except ImportError as e:
    print(f"FATAL: Failed to import required modules - {e}")
    print("Please ensure all dependencies are installed: pip install -r requirements.txt")
    sys.exit(1)

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
    
    try:
        result = subprocess.run(
            [sys.executable, "-m", module_name], 
            cwd=ROOT,
            stdout=None,  # Allow output to terminal
            stderr=None   # Allow errors to terminal
        )
        rc = result.returncode
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
    if not diff_dir.exists():
        return "(no diff files)"

    pattern = f"{award_id}__{base_name}__*.csv"
    files = sorted(
        p.name
        for p in diff_dir.glob(pattern)
        if p.is_file() and p.stat().st_size > 0
    )
    return "\n".join(files) if files else "(no diff files)"


def validate_scraper_output(award_id: str, academy_name: str, scraper_start_time: datetime, scraper_rc: int = None) -> tuple[bool, str]:
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
    
    # Check file age based on scraper success
    file_mtime = datetime.fromtimestamp(latest_file.stat().st_mtime)
    file_size = latest_file.stat().st_size
    
    logging.info("Latest CSV for %s (%s): %s (size: %d bytes, modified: %s)", 
                award_id, academy_name, latest_file.name, file_size, file_mtime)
    
    # If scraper succeeded, require CSV to be newer than scraper start
    if scraper_rc == 0:
        # CSV must be created during this scraper run (within 1 hour tolerance for clock skew)
        time_diff = (scraper_start_time - file_mtime).total_seconds()
        if time_diff > 3600:  # More than 1 hour old
            msg = f"Latest CSV for {award_id} was created before scraper run started: {file_mtime} < {scraper_start_time}"
            logging.error(msg)
            return False, msg
    
    # If scraper failed, we still proceed with the latest CSV available (no age check)
    
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
        scraper_start_time = datetime.now()
        rc = run_scraper(module)
        print(f"Scraper {academy_name} ({aid}) finished with return code: {rc}")  # Add console output
        
        # 2) Validate scraper output regardless of return code
        output_valid, validation_msg = validate_scraper_output(aid, academy_name, scraper_start_time, rc)
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

        # Verify removals but don't create extra staging files for email
        removed_verified, removed_still_present, removed_errors = verify_removed_rows(
            aid,
            diff.get("removed"),
        )
        diff["removed"] = removed_verified

        # Build removal verification notes for logging only
        removal_notes: list[str] = []
        if not removed_still_present.empty:
            removal_notes.append(f"{len(removed_still_present)} still present")
        if not removed_errors.empty:
            removal_notes.append(f"{len(removed_errors)} verification errors")

        summary = diff_summary_str(diff)
        
        # Write main diff CSVs to diffs directory
        diff_dir = SNAPSHOTS_DIR / "diffs"
        diff_dir.mkdir(parents=True, exist_ok=True)

        # Use clean timestamp-based naming
        base_name = curr_path.stem
        try:
            datetime.strptime(base_name[:15], "%Y%m%d_%H%M%S")
            timestamp = base_name[:15]
        except Exception:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = timestamp

        diff_prefix = diff_dir / f"{aid}__{base_name}"
        written_diff_files = write_diff_csvs(diff, diff_prefix)

        # Only attach the main diff files (added/removed/modified) to email
        for path in written_diff_files:
            if path.exists() and path.stat().st_size > 0:
                all_diff_files.append(str(path))

        # Build concise email message
        msg = f"{academy_name}: {summary}"
        if removal_notes:
            msg += f" ({'; '.join(removal_notes)})"
        
        logging.info(f"Award {aid} ({academy_name}): {base_name} - {summary}")
        if removal_notes:
            logging.info(f"  Removal verification: {'; '.join(removal_notes)}")
        
        lines.append(msg)

    # 4) Notification summary
    title = "National Academies Tracker - Updates"
    body = "\n\n".join(lines)
    
    # Add summary footer
    if all_diff_files:
        body += f"\n\nAttached: {len(all_diff_files)} diff file(s)"
    
    logging.info("Email body:\n" + body)
    send_notification(settings, title, body, attachments=all_diff_files)

    # Also print to console for local runs
    print(body)

    # Exit code reflects failures (useful for CI/schedulers)
    if any_failures:
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except ImportError as e:
        print(f"FATAL ERROR: Missing required package - {e}")
        print("Please install all requirements: pip install -r requirements.txt")
        logging.error(f"Import error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"FATAL ERROR: {e}")
        logging.exception("Unhandled exception in main()")
        sys.exit(1)
