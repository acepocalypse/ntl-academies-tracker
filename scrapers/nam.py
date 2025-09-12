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
from selenium.common.exceptions import NoSuchElementException, TimeoutException, StaleElementReferenceException

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
WAIT_SEC   = 5  # Reduced from 10
PAGE_PAUSE = 1.0  # Reduced from 2.0

# ----------------------------
# Helpers
# ----------------------------
def new_driver() -> webdriver.Chrome:
    """Create a reasonably stealthy Chrome driver."""
    opts = webdriver.ChromeOptions()
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-dev-shm-usage")
    # Suppress warnings and logs
    opts.add_argument("--disable-logging")
    opts.add_argument("--disable-web-security")
    opts.add_argument("--disable-features=TranslateUI")
    opts.add_argument("--disable-ipc-flooding-protection")
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument("--disable-backgrounding-occluded-windows")
    opts.add_argument("--disable-client-side-phishing-detection")
    opts.add_argument("--disable-sync")
    opts.add_argument("--disable-default-apps")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--log-level=3")  # Suppress INFO, WARNING, ERROR
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
    # Optimized: try most likely selector first with single call
    try:
        # Most common case first
        el = card.find_element(By.CSS_SELECTOR, "a.elementor-post__thumbnail__link")
        href = el.get_attribute("href") or ""
        if href.strip():
            return href.strip()
    except NoSuchElementException:
        pass
    
    # Fallback selectors
    selectors = [
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
    time.sleep(4)  # Reduced from 7, usually sufficient

    db: List[Dict[str, str]] = []

    try:
        while True:
            # Wait for the grid of cards to load
            cards = WebDriverWait(driver, WAIT_SEC).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "article.elementor-post"))
            )

            for card in cards:
                try:
                    # Pre-get card class for deceased check (single call)
                    card_class = card.get_attribute("class") or ""
                    deceased = "Y" if "health_status-deceased" in card_class else ""

                    # Year (extract a 4-digit year if present)
                    year = ""
                    try:
                        year_text = card.find_element(By.CSS_SELECTOR, "span.sd-post-date").text
                        if year_text:
                            m = re.search(r"\b(19|20)\d{2}\b", year_text)
                            if m:
                                year = m.group(0)
                    except NoSuchElementException:
                        pass

                    # Name - optimized selector
                    name_raw = ""
                    try:
                        name_el = card.find_element(By.CSS_SELECTOR, "div.elementor-heading-title.elementor-size-default")
                        name_raw = name_el.text or ""
                    except NoSuchElementException:
                        pass

                    name = clean_name(name_raw)

                    # Affiliation
                    aff = ""
                    try:
                        aff_el = card.find_element(By.CSS_SELECTOR, "div.sd-member-institutions span.sd-member-institutions")
                        aff = aff_el.text or ""
                    except NoSuchElementException:
                        pass

                    # Location (first category pill if present)
                    location = ""
                    try:
                        loc_el = card.find_element(By.CSS_SELECTOR, "div.sd-post-categories--card-pills span.sd-post-category")
                        location = loc_el.text or ""
                    except NoSuchElementException:
                        pass

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

            # Pagination: robust approach using URL-based detection
            try:
                # Retry loop to handle stale element
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        next_btn = WebDriverWait(driver, WAIT_SEC).until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, "div.jet-filters-pagination__item.prev-next.next"))
                        )
                        if "disabled" in (next_btn.get_attribute("class") or ""):
                            break  # No more pages
                        
                        # Store current URL/page state before clicking
                        current_url = driver.current_url
                        
                        # Count current articles as additional check
                        current_article_count = len(driver.find_elements(By.CSS_SELECTOR, "article.elementor-post"))
                        
                        next_btn.click()
                        break  # Success, exit retry loop
                    except StaleElementReferenceException:
                        if attempt == max_retries - 1:
                            raise  # Re-raise if all retries fail
                        time.sleep(0.5)  # Brief pause before retry
                
                # Wait for navigation to complete - use multiple indicators
                navigation_complete = False
                for attempt in range(20):  # Max 4 seconds
                    time.sleep(0.2)
                    try:
                        # Check if URL changed or new content loaded
                        new_url = driver.current_url
                        new_article_count = len(driver.find_elements(By.CSS_SELECTOR, "article.elementor-post"))
                        
                        # Navigation complete if URL changed OR articles reloaded
                        if (new_url != current_url or 
                            new_article_count != current_article_count or
                            new_article_count >= 12):  # Typical article count per page
                            
                            # Double-check articles are actually loaded
                            WebDriverWait(driver, 2).until(
                                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "article.elementor-post"))
                            )
                            navigation_complete = True
                            break
                            
                    except Exception:
                        # Continue waiting if any check fails
                        continue
                
                # Fallback: just wait for articles if navigation detection failed
                if not navigation_complete:
                    try:
                        WebDriverWait(driver, 3).until(
                            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "article.elementor-post"))
                        )
                    except TimeoutException:
                        # If we can't find articles, we might be done
                        break
                
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
        print(f"[{AID}] AwardID {AID} — scraped ({len(df)} rows) and saved snapshot {snap_path.name} at {now}")
    else:
        print(f"[{AID}] AwardID {AID} — no rows scraped; snapshot still written: {snap_path.name}")

    return df

# Allow: python -m scrapers.nam
if __name__ == "__main__":
    scrape_nam()