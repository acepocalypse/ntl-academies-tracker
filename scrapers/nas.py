# scrapers/nas.py
from __future__ import annotations

import re
import time
import json
import hashlib
import concurrent.futures
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException

# Import backup utility
try:
    from monitor.backup_utils import save_backup_snapshot
except ImportError:
    # Fallback if import fails
    def save_backup_snapshot(*args, **kwargs):
        return None

# ----------------------------
# Constants / Config
# ----------------------------
AID        = "2023"
AWARD      = "NAS Member"
GOVID      = "222"
GOVNAME    = "National Academy of Sciences"
BASE_URL   = (
    "https://www.nasonline.org/membership/member-directory/"
    "?_member_directory_sort=last_name_asc&_per_page=100"
)
WAIT_SEC   = 15
PAGE_PAUSE = 2.0
MAX_WORKERS = 5  # Number of parallel scrapers
CACHE_DIR = Path("cache") / AID  # Directory for caching profile data

# ----------------------------
# Helpers
# ----------------------------
def new_driver(headless: bool = True) -> webdriver.Chrome:
    """Create a Chrome driver with sensible defaults."""
    opts = Options()
    if headless:
        # Use new headless for modern Chrome
        opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    )
    # Add options to disable background sync and timer throttling to avoid GCM errors
    opts.add_argument("--disable-background-sync")
    opts.add_argument("--disable-background-timer-throttling")
    opts.add_argument("--disable-backgrounding-occluded-windows")
    opts.add_argument("--disable-renderer-backgrounding")
    # Suppress browser logging to avoid page load metrics errors
    opts.add_argument("--log-level=3")
    opts.add_argument("--disable-logging")
    return webdriver.Chrome(options=opts)

def norm_text(s: Optional[str]) -> str:
    """Strip and collapse whitespace; None -> ''."""
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

def clean_key(text: str) -> Optional[str]:
    """Sanitize dynamic label into a safe key name."""
    if not text:
        return None
    text = re.sub(r"<[^>]+>", "", text).lower().strip()
    text = text.replace(" ", "_").replace("/", "_")
    text = re.sub(r"[^\w_]+$", "", text)
    return text or None

# Cache helpers
def get_cache_path() -> Path:
    """Create and return cache directory path."""
    cache_dir = CACHE_DIR
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir

def get_cache_key(url: str) -> str:
    """Generate a safe filename from URL."""
    return hashlib.md5(url.encode()).hexdigest() + ".json"

def get_from_cache(url: str) -> Optional[Dict[str, str]]:
    """Get profile data from cache if available and not older than 2 hours."""
    cache_path = get_cache_path() / get_cache_key(url)
    if cache_path.exists():
        # Check file age
        mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
        if datetime.now() - mtime > timedelta(hours=2):
            return None
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return None
    return None

def save_to_cache(url: str, data: Dict[str, str]) -> None:
    """Save profile data to cache."""
    cache_path = get_cache_path() / get_cache_key(url)
    try:
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except IOError:
        pass  # Continue even if cache write fails

# ----------------------------
# Core scraper
# ----------------------------
def extract_card_info(card) -> Dict[str, str]:
    """Extract basic information from a member card on the directory page."""
    try:
        # Extract profile link
        link_element = card.find_element(By.XPATH, ".//h5/a")
        profile_url = (link_element.get_attribute("href") or "").strip()
        
        # Extract name from link text
        name_text = norm_text(link_element.text)
        
        # Extract membership type and living/deceased status from CSS classes
        card_classes = card.get_attribute("class") or ""
        
        # Parse membership type from classes
        membership_type = ""
        if "membership-type-member" in card_classes:
            membership_type = "member"
        elif "membership-type-international-member" in card_classes:
            membership_type = "international-member"
        elif "membership-type-emeritus" in card_classes:
            membership_type = "emeritus"
        elif "membership-type-public-welfare-medalist" in card_classes:
            membership_type = "public-welfare-medalist"
        
        # Parse living/deceased status from classes
        deceased_status = ""
        if "living-deceased-deceased" in card_classes:
            deceased_status = "Y"
        elif "living-deceased-living" in card_classes:
            deceased_status = ""
        
        # Extract affiliation from card-meta section
        affiliation = ""
        try:
            card_meta = card.find_element(By.CSS_SELECTOR, ".card-meta")
            meta_paragraphs = card_meta.find_elements(By.TAG_NAME, "p")
            
            # Look for affiliation - skip membership type and section labels
            membership_labels = ["member", "international member", "emeritus", "public welfare medalist"]
            for p in meta_paragraphs:
                p_text = norm_text(p.text)
                p_lower = p_text.lower()
                if (p_text and 
                    p_lower not in membership_labels and
                    not p_text.startswith("Primary Section") and 
                    not p_text.startswith("Secondary Section") and
                    not p_text.startswith("Section ")):
                    affiliation = p_text
                    break
        except NoSuchElementException:
            affiliation = ""
        
        return {
            "profile_url": profile_url,
            "name": clean_name(name_text),
            "membership_type": membership_type,
            "deceased": deceased_status,
            "affiliation": affiliation,
        }
    except Exception as e:
        print(f"[{AID}] Error extracting card info: {e}")
        return {}

def scrape_profile_details(link: str, base_info: Dict[str, str]) -> Dict[str, str]:
    """Scrape detailed information from a member profile page."""
    # Check cache first
    cached_data = get_from_cache(link)
    if cached_data:
        print(f"[{AID}] Using cached data for: {link}")
        return cached_data

    # Create a new driver for each thread to avoid concurrency issues
    driver = new_driver(headless=True)
    try:
        driver.set_page_load_timeout(30)
        
        member: Dict[str, str] = {
            "id": AID,
            "govid": GOVID,
            "govname": GOVNAME,
            "award": AWARD,
            "year": "",
            "affiliation": "",
            "profile_url": link,  # PRIMARY KEY
            **base_info  # Include info from card
        }

        driver.get(link)

        # Get affiliation - this is the main missing piece from cards
        try:
            aff_div = driver.find_element(By.CSS_SELECTOR, "div[data-node='jd7ypfvaiw1h']")
            paragraphs = aff_div.find_elements(By.TAG_NAME, "p")
            aff_text = "\n".join([norm_text(p.text) for p in paragraphs if norm_text(p.text)])
            member["affiliation"] = aff_text
        except NoSuchElementException:
            member["affiliation"] = ""

        # Dynamic meta items - get election year and other details
        try:
            meta_items = driver.find_elements(By.CSS_SELECTOR, "div.meta-item")
            for item in meta_items:
                try:
                    p_elems = item.find_elements(By.CSS_SELECTOR, "div.fl-rich-text p")
                    if len(p_elems) >= 2:
                        label_html = (p_elems[0].get_attribute("innerHTML") or "").strip()
                        value = norm_text(p_elems[1].text)
                        key = clean_key(label_html)
                        if key and value is not None:
                            if key == "election_year":
                                member["year"] = value
                            elif key == "birth___deceased_date":
                                parts = value.split("-")
                                if len(parts) > 1 and norm_text(parts[1]):
                                    member["deceased"] = "Y"
                            else:
                                # Write dynamic fields if they don't collide
                                if key not in member:
                                    member[key] = value
                                else:
                                    member[f"dynamic_{key}"] = value
                except Exception:
                    continue
        except NoSuchElementException:
            pass

        # Normalize fields
        for k in list(member.keys()):
            member[k] = clean_name(member[k]) if k == "name" else norm_text(member[k])

        # Cache successful results
        save_to_cache(link, member)
        return member

    except TimeoutException:
        print(f"[{AID}] Timed out loading profile page: {link}")
        return {"profile_url": link, "error": "timeout", **base_info}
    except Exception as e:
        print(f"[{AID}] Error processing profile {link}: {e}")
        return {"profile_url": link, "error": str(e), **base_info}
    finally:
        driver.quit()

def scrape_nas() -> pd.DataFrame:
    """
    Scrape NAS directory by iterating through election years to capture year information.
    """
    driver = new_driver(headless=True)
    driver.implicitly_wait(5)
    
    all_cards_info: List[Dict[str, str]] = []
    processed_urls: Set[str] = set()
    
    # Define year ranges to iterate through - NAS started in 1863
    current_year = datetime.now().year
    start_year = 1863
    
    print(f"[{AID}] Starting card collection by iterating through years {start_year}-{current_year}")
    
    # Iterate through individual years to get election year data
    for year in range(start_year, current_year + 1):
        # Use the correct parameter name: _election_year
        year_url = f"{BASE_URL}&_election_year={year}"
        cards_from_year = scrape_year_cards(driver, year, year_url, processed_urls)
        all_cards_info.extend(cards_from_year)
        
        # Add small delay between years to be respectful
        time.sleep(0.2)
    
    print(f"\n[{AID}] Year-based scraping collected {len(all_cards_info)} profiles")
    
    # If year filtering didn't work well, try without year filter as fallback
    if len(all_cards_info) < 1000:
        print(f"[{AID}] Year-based scraping yielded only {len(all_cards_info)} results, trying full directory scan...")
        processed_urls.clear()  # Reset for full scan
        fallback_cards = scrape_all_pages(driver, processed_urls)
        
        # If fallback got more results, use those instead
        if len(fallback_cards) > len(all_cards_info):
            print(f"[{AID}] Using fallback results: {len(fallback_cards)} profiles")
            all_cards_info = fallback_cards
        else:
            print(f"[{AID}] Keeping year-based results: {len(all_cards_info)} profiles")

    driver.quit()
    
    print(f"\n[{AID}] Total unique profiles collected: {len(all_cards_info)}")
    
    if len(all_cards_info) < 5000:
        print(f"[{AID}] WARNING: Only collected {len(all_cards_info)} profiles, expected around 7000.")

    # Add standard metadata fields to all cards
    for card_info in all_cards_info:
        card_info.setdefault("id", AID)
        card_info.setdefault("govid", GOVID)
        card_info.setdefault("govname", GOVNAME)
        card_info.setdefault("award", AWARD)

    # ----------------------------
    # Persist: timestamped snapshot + optional flat CSV
    # ----------------------------
    snap_dir = Path("snapshots") / AID
    snap_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    snap_path = snap_dir / f"{stamp}.csv"
    df = pd.DataFrame(all_cards_info, dtype=str).fillna("")
    df.to_csv(snap_path, index=False)

    # Save to secondary backup location (if configured)
    save_backup_snapshot(snap_path, AID)

    # Also write your legacy flat CSV if `filepath` exists in runtime
    try:
        df.to_csv(f"{filepath}{AID}.csv", index=False)  # type: ignore[name-defined]
    except NameError:
        pass

    if not df.empty:
        now = datetime.now().strftime("%H:%M:%S")
        print(f"[{AID}] AwardID {AID} — scraped ({len(df)} rows) and saved snapshot {snap_path.name} at {now}")
    else:
        print(f"[{AID}] AwardID {AID} — no rows scraped; snapshot still written: {snap_path.name}")

    return df

def scrape_year_cards(driver: webdriver.Chrome, year: int, url: str, processed_urls: Set[str]) -> List[Dict[str, str]]:
    """Scrape all cards for a specific election year."""
    cards_info: List[Dict[str, str]] = []
    
    try:
        driver.get(url)
        # Wait for cards to load
        WebDriverWait(driver, WAIT_SEC).until(
            EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'fl-post-grid-post')]"))
        )
        
        # Check if there are any results for this year
        member_cards = driver.find_elements(By.XPATH, "//div[contains(@class, 'fl-post-grid-post')]")
        
        if not member_cards:
            return cards_info
            
        cards_found = 0
        # Process all pages for this year
        page_num = 1
        
        while True:
            # Process current page
            for card in member_cards:
                try:
                    card_info = extract_card_info(card)
                    if card_info and card_info.get("profile_url"):
                        url_key = card_info["profile_url"]
                        if url_key not in processed_urls:
                            card_info["year"] = str(year)  # Add the election year from URL parameter
                            cards_info.append(card_info)
                            processed_urls.add(url_key)
                            cards_found += 1
                except Exception:
                    continue
            
            # Check for next page within this year's results
            try:
                next_button = driver.find_element(By.XPATH, "//a[@class='next page-numbers']")
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
                time.sleep(0.5)
                driver.execute_script("arguments[0].click();", next_button)
                time.sleep(PAGE_PAUSE)
                
                # Wait for new page to load
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'fl-post-grid-post')]"))
                )
                member_cards = driver.find_elements(By.XPATH, "//div[contains(@class, 'fl-post-grid-post')]")
                page_num += 1
                
            except (NoSuchElementException, TimeoutException):
                # No more pages for this year
                break
                
        if cards_found > 0:
            print(f"[{AID}] Year {year}: found {cards_found} new members across {page_num} pages")
            
    except TimeoutException:
        # No results for this year, which is normal for many years
        pass
    except Exception as e:
        print(f"[{AID}] Error scraping year {year}: {e}")
    
    return cards_info


def scrape_all_pages(driver: webdriver.Chrome, processed_urls: Set[str]) -> List[Dict[str, str]]:
    """Fallback: scrape all pages without year filtering."""
    try:
        driver.get(BASE_URL)
        WebDriverWait(driver, WAIT_SEC).until(
            EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'fl-post-grid-post')]"))
        )
        print(f"[{AID}] Accessed: {BASE_URL}")
    except Exception as e:
        print(f"[{AID}] Error loading initial directory: {e}")
        return []

    cards_info: List[Dict[str, str]] = []
    current_page = 1
    consecutive_empty_pages = 0
    max_consecutive_empty = 3
    print(f"[{AID}] Starting full directory scan...")

    while consecutive_empty_pages < max_consecutive_empty:
        print(f"[{AID}] Processing page {current_page}...")
        page_start_time = time.time()
        
        try:
            member_cards = WebDriverWait(driver, WAIT_SEC).until(
                EC.presence_of_all_elements_located((By.XPATH, "//div[contains(@class, 'fl-post-grid-post')]"))
            )
            num_cards = len(member_cards)
            print(f"[{AID}] Page {current_page}: found {num_cards} cards on page")
            
            cards_found_on_page = 0
            skipped_on_page = 0
            
            for i, card in enumerate(member_cards):
                try:
                    card_info = extract_card_info(card)
                    if card_info and card_info.get("profile_url"):
                        url = card_info["profile_url"]
                        if url not in processed_urls:
                            cards_info.append(card_info)
                            processed_urls.add(url)
                            cards_found_on_page += 1
                        else:
                            skipped_on_page += 1
                except Exception:
                    continue
            
            page_time = time.time() - page_start_time
            print(f"[{AID}] Page {current_page}: processed {num_cards} cards in {page_time:.1f}s")
            print(f"[{AID}]   New cards: {cards_found_on_page}, duplicates: {skipped_on_page}")
            print(f"[{AID}]   Total unique profiles so far: {len(cards_info)}")

            if cards_found_on_page == 0:
                consecutive_empty_pages += 1
                print(f"[{AID}] Warning: No new cards found on page {current_page}")
            else:
                consecutive_empty_pages = 0

            # Pagination logic (same as before)
            next_button = None
            try:
                next_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, "//a[@class='next page-numbers']"))
                )
            except (NoSuchElementException, TimeoutException):
                try:
                    next_button = driver.find_element(By.XPATH, "//a[contains(@class, 'next')]")
                except NoSuchElementException:
                    break

            if next_button:
                try:
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
                    time.sleep(1)
                    driver.execute_script("arguments[0].click();", next_button)
                    time.sleep(PAGE_PAUSE)
                    
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'fl-post-grid-post')]"))
                    )
                    current_page += 1
                    print(f"[{AID}]   Navigated to page {current_page}")
                except Exception as e:
                    print(f"[{AID}] Error clicking next page: {e}")
                    break
        except Exception as e:
            print(f"[{AID}] Error processing page {current_page}: {e}")
            break

    print(f"[{AID}] Completed full directory scan.")
    print(f"[{AID}] Total profiles found: {len(cards_info)}")
    
    return cards_info

# Allow running as a module: python -m scrapers.nas
if __name__ == "__main__":
    df = scrape_nas()
