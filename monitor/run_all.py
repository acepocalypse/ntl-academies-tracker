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
from monitor.notify import email_notify, discord_notify

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


def send_notification(settings: dict, title: str, body: str) -> None:
    method = settings.get("notify", {}).get("method", "discord")
    try:
        if method == "discord":
            url = settings.get("discord", {}).get("webhook_url", "")
            if not url:
                logging.warning("Discord webhook URL missing; skipping notification.")
                return
            discord_notify(url, f"**{title}**\n{body}")
        elif method == "email":
            e = settings.get("email", {})
            email_notify(
                subject=title,
                body=body,
                smtp_host=e["smtp_host"],
                smtp_port=int(e["smtp_port"]),
                username=e["username"],
                password=e["password"],
                to_addrs=e["to"],
            )
        else:
            logging.warning("Unknown notify method '%s'; skipping notification.", method)
    except Exception as ex:
        logging.exception("Notification failed: %s", ex)


def summarize_diff_paths(award_id: str, curr_path: Path) -> str:
    """
    Return a short string with links/paths to any diff CSVs written for this snapshot.
    """
    base = curr_path.with_suffix("")
    parts = []
    for suffix in ("__added.csv", "__removed.csv", "__modified.csv"):
        p = base.with_name(base.name + suffix)
        if p.exists() and p.stat().st_size > 0:
            parts.append(p.name)
    return ", ".join(parts) if parts else "(no diff files)"


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    settings = read_settings()
    award_ids = settings.get("general", {}).get("awards", list(AWARD_MODULES.keys()))
    ignore_fields = settings.get("diff", {}).get("ignore_fields", DEFAULT_IGNORE_FIELDS)

    run_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logging.info("=== Weekly run start: %s ===", run_ts)

    lines: list[str] = [f"Run at {run_ts}"]
    any_failures = False

    for aid in award_ids:
        module = AWARD_MODULES.get(aid)
        if not module:
            msg = f"• {aid}: no scraper module mapping found"
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
            msg = f"• {aid}: no snapshots found after run"
            logging.warning(msg)
            lines.append(msg)
            continue

        if prev_path is None:
            msg = f"• {aid}: first snapshot {curr_path.name} (no diff yet)"
            logging.info(msg)
            lines.append(msg)
            continue

        diff = compute_diff(prev, curr, ignore_fields=ignore_fields)
        summary = diff_summary_str(diff)
        # 3) Write diff CSVs (if non-empty)
        written = write_diff_csvs(diff, curr_path.with_suffix(""))
        written_str = summarize_diff_paths(aid, curr_path)

        msg = f"• {aid}: {curr_path.name}  {summary}  {written_str}"
        logging.info(msg)
        lines.append(msg)

    # 4) Notification summary
    title = "Awards monitor — weekly run complete"
    body = "\n".join(lines)
    logging.info(body)
    send_notification(settings, title, body)

    # Also print to console for local runs
    print(body)

    # Exit code reflects failures (useful for CI/schedulers)
    if any_failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
