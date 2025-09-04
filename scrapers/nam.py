# scrapers/nam.py
from __future__ import annotations

import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException

# ----------------------------
# Constants / Config
# ----------------------------
AID        = "1909"
AWARD      = "NAM Member"
GOVID      = "202"
GOVNAME    = "National Academy of Medicine"
BASE_URL   = (
    "https://nam.edu/membership/members/directory/"
    "?lastName&firstName&parentInstitution&yearStart&yearEnd"
    "&presence=0&jsf=epro-posts:content-feed&tax=health_status:include_all"
)
WAIT_SEC   = 10
PAGE_PAUSE = 2.0

# ----------------------------
# Helpers
# ----------------------------
def new_driver() -> webdriver.Chrome:
    """Create a reasonably stealthy Chrome driver."""
    opts = webdriver.ChromeOptions()
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--no-sandbox")
    # Uncomment to run headless in CI:
    # opts.add_argument("--headless=new")
    return webdriver.Chrome(options=opts)

def norm_text(s: str) -> str:
    """Basic normalization: strip, collapse internal whitespace."""
    if s is None:
        return ""
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    return s

def clean_name(name: str) -> str:
    """Remove common prefixes/suffixes and normalize whitespace."""
    name = norm_text(name)
    prefixes = ["Dr. ", "Dr ", "Mr. ", "Mr ", "Ms. ", "Ms ", "Mrs. ", "Mrs ", "Prof. ", "Professor "]
    suffixes = [" Jr.", " Jr", " Sr.", " Sr", " II", " III", " IV", ", PhD", ", MD", ", DSc"]
    for p in prefixes:
        if name.startswith(p):
            name = name[len(p):]
    for s in suffixes:
        if name.endswith(s):
            name = name[: -len(s)]
    return norm_text(name)

def first_href_in(card) -> str:
    """Best-effort: fetch a member profile URL from various possible anchors."""
    # Try common places first
    selectors = [
        "a.elementor-post__thumbnail__link",
        "h3.elementor-heading-title a",
        "a.elementor-post__read-more",
        "header a",
        "a",  # fallback: any anchor
    ]
    for sel in selectors:
        try:
            el = card.find_element(By.CSS_SELECTOR, sel)
            href = el.get_attribute("href") or ""
            href = href.strip()
            if href:
                return href
        except NoSuchElementException:
            continue
    return ""

# ----------------------------
# Core scraper
# ----------------------------
def scrape_nam() -> pd.DataFrame:
    """
    Scrape NAM directory into a normalized DataFrame with a stable primary key (profile_url).
    Saves a timestamped snapshot under snapshots/1909/, and (optionally) the flat CSV to filepath+1909.csv.
    """
    driver = new_driver()
    driver.get(BASE_URL)
    time.sleep(7)  # allow heavy first render

    db: List[Dict[str, str]] = []

    try:
        while True:
            # Wait for the grid of cards to load
            cards = WebDriverWait(driver, WAIT_SEC).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "article.elementor-post"))
            )

            for card in cards:
                try:
                    # Year (extract a 4-digit year if present)
                    year = ""
                    try:
                        year_text = card.find_element(By.CSS_SELECTOR, "span.sd-post-date").text
                        m = re.search(r"\b(19|20)\d{2}\b", year_text or "")
                        if m:
                            year = m.group(0)
                    except Exception:
                        year = ""

                    # Name
                    try:
                        name_raw = card.find_elements(
                            By.CSS_SELECTOR, "div.elementor-heading-title.elementor-size-default"
                        )[0].text
                    except Exception:
                        name_raw = ""

                    name = clean_name(name_raw)

                    # Affiliation
                    try:
                        aff = card.find_element(
                            By.CSS_SELECTOR, "div.sd-member-institutions span.sd-member-institutions"
                        ).text
                    except Exception:
                        aff = ""

                    # Location (first category pill if present)
                    try:
                        locs = card.find_elements(
                            By.CSS_SELECTOR, "div.sd-post-categories--card-pills span.sd-post-category"
                        )
                        location = locs[0].text if locs else ""
                    except Exception:
                        location = ""

                    # Deceased flag based on class
                    deceased = "Y" if "health_status-deceased" in (card.get_attribute("class") or "") else ""

                    # Profile URL (primary key)
                    profile_url = first_href_in(card)

                    db.append({
                        "id":             AID,
                        "govid":          GOVID,
                        "govname":        GOVNAME,
                        "award":          AWARD,
                        "profile_url":    norm_text(profile_url),  # PRIMARY KEY
                        "year":           norm_text(year),
                        "name":           name,
                        "affiliation":    norm_text(aff),
                        "location":       norm_text(location),
                        "deceased":       norm_text(deceased),
                    })
                except Exception:
                    # Skip a bad card; continue scraping
                    continue

            # Pagination: click "Next" and wait for page sentinel to change
            try:
                prev_current = driver.find_element(
                    By.CSS_SELECTOR, "div.jet-filters-pagination__item.jet-filters-pagination__current"
                )
                next_btn = driver.find_element(
                    By.CSS_SELECTOR, "div.jet-filters-pagination__item.prev-next.next"
                )
                next_btn.click()

                # Wait for old sentinel to go stale, then new to appear
                WebDriverWait(driver, WAIT_SEC).until(EC.staleness_of(prev_current))
                WebDriverWait(driver, WAIT_SEC).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "div.jet-filters-pagination__item.jet-filters-pagination__current")
                    )
                )
                time.sleep(PAGE_PAUSE)

            except (NoSuchElementException, TimeoutException):
                # No next page → finished
                break
    finally:
        driver.quit()

    # Build DataFrame (all as string), normalize NaNs to ""
    df = pd.DataFrame(db, dtype=str).fillna("")
    # Deduplicate by profile_url just in case
    if not df.empty:
        df = df.sort_values(["profile_url", "name"]).drop_duplicates(subset=["profile_url"], keep="first")

    # ----------------------------
    # Persist: timestamped snapshot + optional flat CSV
    # ----------------------------
    snap_dir = Path("snapshots") / AID
    snap_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    snap_path = snap_dir / f"{stamp}.csv"
    df.to_csv(snap_path, index=False)

    # Also write your legacy flat CSV if `filepath` is provided by the caller’s runtime
    try:
        # `filepath` may exist in your global environment from other scripts
        # and should end with a trailing slash/backslash.
        df.to_csv(f"{filepath}{AID}.csv", index=False)  # type: ignore[name-defined]
    except NameError:
        # No `filepath` defined; ignore.
        pass

    if not df.empty:
        now = datetime.now().strftime("%H:%M:%S")
        print(f"AwardID {AID} — scraped ({len(df)} rows) and saved snapshot {snap_path.name} at {now}")
    else:
        print(f"AwardID {AID} — no rows scraped; snapshot still written: {snap_path.name}")

    return df

# Allow: python -m scrapers.nam
if __name__ == "__main__":
    scrape_nam()