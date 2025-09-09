"""
Orchestrator module for monitoring system.
This module handles task timing and execution flow
"""

import pathlib
import logging
import sys
import traceback
import datetime as dt

BASE = pathlib.Path(__file__).resolve().parent
DATA = BASE / "data"
OUT  = BASE / "out"
LOGS = BASE / "logs"
for p in (DATA, OUT, LOGS): p.mkdir(exist_ok=True)

# --- configure logging ---
stamp = dt.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_path = LOGS / f"weekly_run_{stamp}.log"
logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("weekly")

def run_step(fn, name):
    log.info(f"START {name}")
    try:
        fn()
        log.info(f"OK {name}")
    except Exception:
        log.exception(f"FAIL {name}")
        raise

# --- import your existing steps (put real logic inside these functions) ---
def scrape_all():
    # e.g. import scrape1, scrape2, scrape3
    # scrape1.run(); scrape2.run(); scrape3.run()
    pass

def compare_and_mark_changes():
    # load previous snapshot from DATA, compare to new, mark diffs
    # write a single "changes_YYYY-MM-DD.csv" to OUT
    pass

def email_report():
    # import your send_email() from earlier
    # attach OUT / latest changes csv or inline link
    pass

def main():
    log.info("=== Weekly job kicked off ===")
    run_step(scrape_all, "scrape_all")
    run_step(compare_and_mark_changes, "compare_and_mark_changes")
    run_step(email_report, "email_report")
    log.info("=== Weekly job finished ===")

if __name__ == "__main__":
    try:
        main()
        print(f"Log: {log_path}")  # visible if you run manually
    except Exception:
        # make sure failures are visible in Task Scheduler “Last Run Result”
        traceback.print_exc()
        sys.exit(1)
