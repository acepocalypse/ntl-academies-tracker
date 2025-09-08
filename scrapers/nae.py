# scrapers/nae.py
from __future__ import annotations

import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import NoSuchElementException, TimeoutException
# If you use webdriver_manager, uncomment the next line and the call in new_driver()
# from webdriver_manager.chrome import ChromeDriverManager

AID       = "3008"
AWARD     = "NAE Membership"
GOVID     = "221"
GOVNAME   = "National Academy of Engineering"
BASE_URL  = "https://www.nae.edu/20412/MemberDirectory"
WAIT_SEC  = 10
PAGE_PAUSE= 0.8
PAGE_SIZE = "50"   # try to show max rows per page

def new_driver(headless: bool = False) -> webdriver.Chrome:
    opts = webdriver.ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--no-sandbox")
    # Suppress Chrome warnings and logs
    opts.add_argument("--disable-logging")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-background-timer-throttling")
    opts.add_argument("--disable-backgrounding-occluded-windows")
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument("--log-level=3")  # Suppress INFO, WARNING, ERROR messages
    opts.add_argument("--silent")
    # Disable Google API services to prevent registration errors
    opts.add_argument("--disable-sync")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--disable-default-apps")
    
    # Reduce bandwidth/paint: disable images (safe for our text scraping)
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.default_content_setting_values.notifications": 2
    }
    opts.add_experimental_option("prefs", prefs)
    # Suppress Chrome logging
    opts.add_experimental_option('excludeSwitches', ['enable-logging'])
    opts.add_experimental_option('useAutomationExtension', False)
    
    # return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    return webdriver.Chrome(options=opts)

def norm_text(s: Optional[str]) -> str:
    if not s:
        return ""
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    return s

def clean_name(name: str) -> str:
    name = norm_text(name)
    prefixes = ["Dr. ", "Dr ", "Mr. ", "Mr ", "Ms. ", "Ms ", "Mrs. ", "Mrs ", "Prof. ", "Professor "]
    suffixes = [" Jr.", " Jr", " Sr.", " Sr", " II", " III", " IV", ", PhD", ", MD", ", DSc"]
    for p in prefixes:
        if name.startswith(p):
            name = name[len(p):]
    for s in suffixes:
        if name.endswith(s):
            name = name[:-len(s)]
    return norm_text(name)

def safe_attr(driver, by, selector, attr="text") -> str:
    try:
        el = driver.find_element(by, selector)
        if attr == "text":
            return norm_text(el.text)
        return norm_text(el.get_attribute(attr))
    except NoSuchElementException:
        return ""

def set_page_size(driver: webdriver.Chrome, wait: WebDriverWait) -> None:
    """
    Try to switch the top pager to PAGE_SIZE rows per page (if control exists).
    """
    try:
        dropdown = wait.until(EC.presence_of_element_located((
            By.NAME,
            "ctl06$ctl05$ctl00$MembersList$members$ctl01$ctl22$filterTopPager$ddlPageSize"
        )))
        Select(dropdown).select_by_visible_text(PAGE_SIZE)
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "flexible-list-item")))
    except TimeoutException:
        pass
    except NoSuchElementException:
        pass

def discover_years(driver: webdriver.Chrome, wait: WebDriverWait) -> List[int]:
    """
    Attempt to read all available 'Election Year' values from the page's year filter.
    Falls back to a conservative range if the control can't be found.
    """
    driver.get(BASE_URL)
    time.sleep(2)
    years: List[int] = []

    # Common patterns: select elements with id/name containing 'Year'
    selectors = [
        (By.CSS_SELECTOR, "select[id*='Year']"),
        (By.CSS_SELECTOR, "select[name*='Year']"),
        (By.XPATH, "//select[contains(@id,'Year') or contains(@name,'Year')]"),
    ]
    for by, sel in selectors:
        try:
            el = WebDriverWait(driver, 4).until(EC.presence_of_element_located((by, sel)))
            options = el.find_elements(By.TAG_NAME, "option")
            for opt in options:
                txt = norm_text(opt.text)
                m = re.search(r"\b(19|20)\d{2}\b", txt)
                if m:
                    years.append(int(m.group(0)))
            if years:
                years = sorted(set(years))
                return years
        except TimeoutException:
            continue
        except NoSuchElementException:
            continue

    # Fallback (broad but finite): last 60 years
    from datetime import date
    this_year = date.today().year
    return list(range(this_year - 60, this_year + 1))

def _collect_links_from_current_listing(driver: webdriver.Chrome, wait: WebDriverWait) -> List[str]:
    """
    Collect all profile links from the current listing (no assumptions about filters),
    iterating pagination until exhausted.
    """
    links: List[str] = []
    seen: Set[str] = set()

    while True:
        # Wait for any result items
        wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, "flexible-list-item")))

        # Try to capture a sentinel (first card's href) to detect page change
        try:
            first_href_before = driver.find_element(By.CSS_SELECTOR, "span.name a").get_attribute("href")
        except NoSuchElementException:
            first_href_before = None

        # Collect links on this page
        for item in driver.find_elements(By.CLASS_NAME, "flexible-list-item"):
            try:
                href = item.find_element(By.CSS_SELECTOR, "span.name a").get_attribute("href")
                href = (href or "").strip()
                if href and href not in seen:
                    seen.add(href)
                    links.append(href)
            except NoSuchElementException:
                continue

        # Pagination: try to click "next"
        next_buttons = driver.find_elements(By.CSS_SELECTOR, "li.pager-pagenextb a.next_page")
        if not next_buttons or not next_buttons[0].is_displayed():
            break

        btn = next_buttons[0]
        driver.execute_script("arguments[0].scrollIntoView(true); window.scrollBy(0, -100);", btn)
        driver.execute_script("arguments[0].click();", btn)

        # Wait until first card href changes (or short pause fallback)
        try:
            wait.until(lambda d: d.find_element(By.CSS_SELECTOR, "span.name a").get_attribute("href") != first_href_before)
        except TimeoutException:
            time.sleep(1)

        # If nothing changed, bail to avoid infinite loop
        try:
            first_href_after = driver.find_element(By.CSS_SELECTOR, "span.name a").get_attribute("href")
            if first_href_before == first_href_after:
                break
        except NoSuchElementException:
            break

        time.sleep(PAGE_PAUSE)

    return links

def collect_all_links(driver: webdriver.Chrome, wait: WebDriverWait) -> List[str]:
    """
    Collect all profile links across the full directory in a single pass (no year filter).
    """
    driver.get(f"{BASE_URL}?qdec=both")
    set_page_size(driver, wait)
    return _collect_links_from_current_listing(driver, wait)

def collect_links_for_year(driver: webdriver.Chrome, wait: WebDriverWait, year: int) -> List[str]:
    """
    For a given election year, iterate all pages and return all profile links.
    """
    url = f"{BASE_URL}?qey={year}&qdec=both"
    driver.get(url)
    set_page_size(driver, wait)
    return _collect_links_from_current_listing(driver, wait)

def scrape_profile(driver: webdriver.Chrome, wait: WebDriverWait, url: str, fallback_year: Optional[int] = None) -> Dict[str, str]:
    driver.get(url)
    wait.until(EC.presence_of_element_located((By.CLASS_NAME, "name")))

    raw_name   = safe_attr(driver, By.CSS_SELECTOR, "div.name", attr="text")
    name       = clean_name(raw_name)
    title      = safe_attr(driver, By.CSS_SELECTOR, ".personInfo.hidden-xs .jobOrg .jobTitle", attr="text")
    affiliation= safe_attr(driver, By.CSS_SELECTOR, ".personInfo.hidden-xs .jobOrg .organization", attr="text")

    other_affs = ", ".join(
        norm_text(el.text)
        for el in driver.find_elements(
            By.XPATH, "//label[normalize-space()='Other Affiliations']/following-sibling::*//li"
        ) if norm_text(el.text)
    )

    location   = safe_attr(
        driver, By.XPATH,
        "//label[normalize-space()='Location']/following-sibling::div[contains(@class,'address')]"
    )

    # Election year (try to read; fallback to the listing year)
    try:
        wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "ul.ordList li")))
    except TimeoutException:
        pass
    election_year = safe_attr(
        driver, By.XPATH,
        "(//ul[contains(@class,'ordList')])[last()]/"
        "li[label[normalize-space()='Election Year']]/span",
        attr="text"
    ) or (str(fallback_year) if fallback_year is not None else "")

    # Deceased badge
    deceased = "Y"
    try:
        driver.find_element(By.CSS_SELECTOR, "span.badge.deceased")
    except NoSuchElementException:
        deceased = ""

    return {
        "id":                 AID,
        "govid":              GOVID,
        "govname":            GOVNAME,
        "award":              AWARD,
        "profile_url":        url,          # PRIMARY KEY
        "name":               name,
        "title":              norm_text(title),
        "affiliation":        norm_text(affiliation),
        "other_affiliations": norm_text(other_affs),
        "location":           norm_text(location),
        "year":               norm_text(election_year),
        "deceased":           deceased,
    }

def scrape_nae(all_years: Optional[List[int]] = None, headless: bool = True) -> pd.DataFrame:
    """
    Scrape NAE directory efficiently.
    - Default: collect all profile links once (no year filter) and scrape each profile once.
    - If `all_years` provided: collect links per year but de-duplicate across years, then scrape once per profile.
    - Saves timestamped snapshot to snapshots/3008/YYYYMMDD_HHMMSS.csv and optional legacy CSV to `filepath + "3008.csv"`.
    """
    driver = new_driver(headless=headless)
    wait = WebDriverWait(driver, WAIT_SEC)
    records: List[Dict[str, str]] = []
    errors_count = 0

    try:
        # Build a unique set of links to avoid scraping duplicates across years.
        unique_links: List[str] = []
        seen_links: Set[str] = set()
        fallback_year_for: Dict[str, int] = {}

        years = all_years or []
        if years:
            print(f"Years provided ({len(years)}): {years}")
        else:
            print(f"[{AID}] Collecting all profile links (single pass)...")
            unique_links = collect_all_links(driver, wait)
            seen_links = set(unique_links)
            print(f"[{AID}] Collected {len(unique_links)} unique links total")

        for yr in years:
            print(f"[{AID}] Collecting links for year {yr} …")
            links = collect_links_for_year(driver, wait, yr)
            print(f"[{AID}] Year {yr}: {len(links)} profile links")

            for href in links:
                if href not in seen_links:
                    seen_links.add(href)
                    unique_links.append(href)
                    fallback_year_for[href] = yr
        
        # After collecting links (either from years or single pass), scrape once per unique URL
        total_links = len(unique_links)
        print(f"[{AID}] Starting to scrape {total_links} unique profiles...")
        
        for i, href in enumerate(unique_links, 1):
            try:
                rec = scrape_profile(
                    driver, wait, href, fallback_year=fallback_year_for.get(href)
                )
                records.append(rec)
                time.sleep(PAGE_PAUSE)  # polite, consistent pacing
                
                # Progress reporting every 25 profiles
                if i % 25 == 0:
                    print(f"[{AID}] Scraped {i}/{total_links} profiles... ({len(records)} successful)")
            except Exception as e:
                errors_count += 1
                print(f"  - Error scraping profile {i}/{total_links} ({href}): {e}")
                continue

        # Final summary
        print(f"[{AID}] Scraping complete: {len(records)} successful, {errors_count} errors")

    finally:
        driver.quit()

    df = pd.DataFrame(records, dtype=str).fillna("")
    if not df.empty:
        # De-dupe on stable primary key
        df = df.sort_values(["profile_url", "name"]).drop_duplicates(subset=["profile_url"], keep="first")

    # Persist: timestamped snapshot
    snap_dir = Path("snapshots") / AID
    snap_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    snap_path = snap_dir / f"{stamp}.csv"
    df.to_csv(snap_path, index=False)

    # Optional legacy CSV if your runner expects it
    try:
        df.to_csv(f"{filepath}{AID}.csv", index=False)  # type: ignore[name-defined]
    except NameError:
        pass

    if not df.empty:
        print(f"AwardID {AID} — scraped {len(df)} rows; snapshot {snap_path.name}")
    else:
        print(f"AwardID {AID} — no rows scraped; snapshot {snap_path.name}")

    return df

# Allow: python -m scrapers.nae
if __name__ == "__main__":
    scrape_nae()
