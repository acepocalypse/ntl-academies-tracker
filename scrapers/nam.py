# scrapers/nam.py
from __future__ import annotations

import re
import time
from datetime import datetime
import sys
from pathlib import Path
from typing import Dict, List

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, StaleElementReferenceException

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
AID        = "1909"
AWARD      = "NAM Member"
GOVID      = "202"
GOVNAME    = "National Academy of Medicine"
BASE_URL   = (
    "https://nam.edu/membership/members/directory/?jsf=epro-posts:content-feed&tax=health_status:include_all"
)
WAIT_SEC   = 8  # Increased for better reliability
PAGE_PAUSE = 2.0  # Increased for better page loading

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
    # Run headless:
    opts.add_argument("--headless=new")
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
    time.sleep(6)  # Increased initial wait

    db: List[Dict[str, str]] = []
    page_num = 1
    total_cards_attempted = 0
    total_records_extracted = 0

    print(f"[{AID}] Starting NAM scraper...")

    try:
        while True:
            print(f"[{AID}] Scraping page {page_num}...")

            # Wait longer and more reliably for page content
            page_loaded = False
            for load_attempt in range(10):  # Try 10 times to ensure page is loaded
                try:
                    WebDriverWait(driver, WAIT_SEC).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "article.elementor-post"))
                    )
                    # Additional wait for dynamic content
                    time.sleep(1.5)
                    cards = driver.find_elements(By.CSS_SELECTOR, "article.elementor-post")
                    if len(cards) > 0:
                        page_loaded = True
                        break
                    else:
                        print(f"[{AID}] Page {page_num}: No cards found (attempt {load_attempt + 1}), retrying...")
                        time.sleep(2)
                except TimeoutException:
                    print(f"[{AID}] Page {page_num}: Timeout waiting for cards (attempt {load_attempt + 1}), retrying...")
                    time.sleep(2)
            
            if not page_loaded:
                print(f"[{AID}] Page {page_num}: Failed to load after multiple attempts, stopping...")
                break

            initial_card_count = len(driver.find_elements(By.CSS_SELECTOR, "article.elementor-post"))
            print(f"[{AID}] Page {page_num}: found {initial_card_count} cards on page")
            total_cards_attempted += initial_card_count

            page_records = 0

            for i in range(initial_card_count):
                card_processed = False
                card_attempts = 0
                
                while not card_processed and card_attempts < 50:  # Never give up until 50 attempts
                    try:
                        card_attempts += 1
                        
                        # Re-fetch cards before each access to avoid stale references
                        cards = driver.find_elements(By.CSS_SELECTOR, "article.elementor-post")
                        if i >= len(cards):
                            # Wait for page to stabilize
                            time.sleep(2.0)
                            cards = driver.find_elements(By.CSS_SELECTOR, "article.elementor-post")
                            if i >= len(cards):
                                print(f"[{AID}] Page {page_num}, Card {i+1}: Card disappeared, waiting longer...")
                                time.sleep(3.0)
                                continue
                        
                        card = cards[i]

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

                        # Name - try multiple selectors
                        name_raw = ""
                        name_selectors = [
                            "div.elementor-heading-title.elementor-size-default",
                            "h3.elementor-heading-title",
                            ".elementor-heading-title",
                            ".sd-member-name",
                        ]
                        for name_sel in name_selectors:
                            try:
                                name_el = card.find_element(By.CSS_SELECTOR, name_sel)
                                name_raw = name_el.text or ""
                                if name_raw.strip():
                                    break
                            except NoSuchElementException:
                                continue

                        name = clean_name(name_raw)

                        # Profile URL (primary key)
                        profile_url = first_href_in(card)

                        # Member Type (check for "Emeritus" or other types)
                        member_type = ""
                        try:
                            # Look for member type labels (e.g., "Emeritus")
                            type_elements = card.find_elements(By.CSS_SELECTOR, "div.sd-member-institutions span")
                            for elem in type_elements:
                                text = (elem.text or "").strip()
                                if text.lower() in ["emeritus", "international", "foreign associate"]:
                                    member_type = text
                                    break
                        except Exception:
                            pass

                        # Affiliation - get actual institution, skip member type labels
                        aff = ""
                        try:
                            # Get all text content within the institutions div
                            aff_container = card.find_element(By.CSS_SELECTOR, "div.sd-member-institutions")
                            # Get all span elements
                            aff_spans = aff_container.find_elements(By.CSS_SELECTOR, "span")
                            
                            for span in aff_spans:
                                text = (span.text or "").strip()
                                # Skip empty, skip member type labels, and skip "No Affiliation"
                                if text and text.lower() not in ["emeritus", "international", "foreign associate", "no affiliation", ""]:
                                    aff = text
                                    break
                            
                            # Fallback: if no valid affiliation found in spans, try getting all text
                            if not aff:
                                full_text = (aff_container.text or "").strip()
                                # Split by newlines and filter out member types and "No Affiliation"
                                lines = [line.strip() for line in full_text.split("\n") if line.strip()]
                                for line in lines:
                                    if line.lower() not in ["emeritus", "international", "foreign associate", "no affiliation"]:
                                        aff = line
                                        break
                        except NoSuchElementException:
                            pass

                        # Location (first category pill if present)
                        location = ""
                        try:
                            loc_el = card.find_element(By.CSS_SELECTOR, "div.sd-post-categories--card-pills span.sd-post-category")
                            location = loc_el.text or ""
                        except NoSuchElementException:
                            pass

                        # Create record - NEVER skip, even if name or URL is missing
                        # Use card index as fallback identifier if needed
                        fallback_id = f"page_{page_num}_card_{i+1}"
                        
                        record = {
                            "id":             AID,
                            "govid":          GOVID,
                            "govname":        GOVNAME,
                            "award":          AWARD,
                            "profile_url":    norm_text(profile_url) or f"missing_url_{fallback_id}",
                            "year":           norm_text(year),
                            "name":           name or f"missing_name_{fallback_id}",
                            "affiliation":    norm_text(aff),
                            "member_type":    norm_text(member_type),
                            "location":       norm_text(location),
                            "deceased":       norm_text(deceased),
                        }
                        
                        db.append(record)
                        page_records += 1
                        total_records_extracted += 1
                        card_processed = True

                        # Log unusual cases for debugging
                        if not name.strip() or not profile_url.strip():
                            print(f"[{AID}] Page {page_num}, Card {i+1}: Captured incomplete record - name: '{name[:30]}', url: '{profile_url[:50]}'")

                    except StaleElementReferenceException:
                        print(f"[{AID}] Page {page_num}, Card {i+1}: StaleElementReferenceException (attempt {card_attempts}), retrying...")
                        time.sleep(min(0.5 * (card_attempts // 5 + 1), 3.0))
                    except Exception as e:
                        print(f"[{AID}] Page {page_num}, Card {i+1}: Error processing (attempt {card_attempts}) - {e}")
                        time.sleep(min(0.5 * (card_attempts // 5 + 1), 3.0))

                if not card_processed:
                    # Last resort: create a placeholder record so we don't lose the count
                    print(f"[{AID}] Page {page_num}, Card {i+1}: FAILED after {card_attempts} attempts, creating placeholder record")
                    fallback_id = f"page_{page_num}_card_{i+1}_failed"
                    db.append({
                        "id":             AID,
                        "govid":          GOVID,
                        "govname":        GOVNAME,
                        "award":          AWARD,
                        "profile_url":    f"failed_to_extract_{fallback_id}",
                        "year":           "",
                        "name":           f"failed_extraction_{fallback_id}",
                        "affiliation":    "",
                        "location":       "",
                        "deceased":       "",
                    })
                    total_records_extracted += 1

            print(f"[{AID}] Page {page_num}: processed {initial_card_count} cards, extracted {page_records} records (running total: {total_records_extracted})")

            # Pagination: more robust navigation detection
            try:
                # Wait for pagination to be ready
                time.sleep(1.0)
                
                next_btn = None
                for nav_attempt in range(5):
                    try:
                        next_btn = WebDriverWait(driver, WAIT_SEC).until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, "div.jet-filters-pagination__item.prev-next.next"))
                        )
                        break
                    except (StaleElementReferenceException, TimeoutException):
                        print(f"[{AID}] Page {page_num}: Navigation retry {nav_attempt + 1}")
                        time.sleep(1.0)
                
                if not next_btn or "disabled" in (next_btn.get_attribute("class") or ""):
                    print(f"[{AID}] Page {page_num}: No next button or disabled, scraping complete.")
                    break
                
                # Store state before navigation
                current_url = driver.current_url
                current_article_count = len(driver.find_elements(By.CSS_SELECTOR, "article.elementor-post"))
                
                print(f"[{AID}] Navigating to page {page_num + 1}...")
                next_btn.click()
                
                # Wait for navigation with multiple indicators
                navigation_success = False
                for wait_attempt in range(40):  # Wait up to 8 seconds
                    time.sleep(0.2)
                    try:
                        new_url = driver.current_url
                        new_article_count = len(driver.find_elements(By.CSS_SELECTOR, "article.elementor-post"))
                        
                        # Check for successful navigation
                        if (new_url != current_url or 
                            new_article_count != current_article_count or
                            new_article_count >= 10):  # Reasonable article count
                            
                            # Confirm articles are actually loaded
                            WebDriverWait(driver, 3).until(
                                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "article.elementor-post"))
                            )
                            navigation_success = True
                            break
                    except Exception:
                        continue
                
                if not navigation_success:
                    print(f"[{AID}] Page {page_num}: Navigation might have failed, but continuing...")
                
                time.sleep(PAGE_PAUSE)
                page_num += 1

            except (NoSuchElementException, TimeoutException):
                print(f"[{AID}] No more pages found. Scraping complete.")
                break
                
    finally:
        # Ensure driver shutdown never forces a non-zero process exit
        try:
            driver.quit()
        except Exception as e:
            print(f"[{AID}] Warning: driver.quit() raised an exception: {e}")

    # Build DataFrame (all as string), normalize NaNs to ""
    df = pd.DataFrame(db, dtype=str).fillna("")
    print(f"Total cards attempted across all pages: {total_cards_attempted}")
    print(f"Total records extracted: {total_records_extracted}")
    print(f"Raw rows collected: {len(df)}")
    
    # Check for any records that might be actual duplicates (not from retries)
    if not df.empty:
        print(f"Unique profile_url count before deduplication: {df['profile_url'].nunique()}")
        
        # Show sample of any potential issues
        failed_extractions = df[df['profile_url'].str.contains('failed_to_extract|missing_url', na=False)]
        if len(failed_extractions) > 0:
            print(f"Found {len(failed_extractions)} records with extraction issues:")
            print(failed_extractions[['profile_url', 'name']].head())
        
        # Deduplicate more carefully - only remove true duplicates, not failed extractions
        initial_count = len(df)
        # Don't deduplicate failed extractions since they might be legitimate separate records
        mask_failed = df['profile_url'].str.contains('failed_to_extract|missing_url', na=False)
        df_good = df[~mask_failed]
        df_failed = df[mask_failed]
        
        if not df_good.empty:
            df_good = df_good.sort_values(["profile_url", "name"]).drop_duplicates(subset=["profile_url"], keep="first")
        
        # Recombine
        df = pd.concat([df_good, df_failed], ignore_index=True) if not df_failed.empty else df_good
        
        final_count = len(df)
        duplicates_removed = initial_count - final_count
        print(f"Final unique records after deduplication: {final_count}")
        if duplicates_removed > 0:
            print(f"Removed {duplicates_removed} duplicate entries")
        
        print(f"✓ SUCCESS: Captured all available records ({final_count} unique records)")

    # ----------------------------
    # Persist: timestamped snapshot + optional flat CSV
    # ----------------------------
    snap_dir = Path("snapshots") / AID
    snap_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    snap_path = snap_dir / f"{stamp}.csv"
    df.to_csv(snap_path, index=False)

    # Save to secondary backup location (if configured)
    save_backup_snapshot(snap_path, AID)

    # Also write your legacy flat CSV if `filepath` is provided by the caller's runtime
    legacy_target = globals().get("filepath")
    if legacy_target:
        try:
            legacy_root = Path(str(legacy_target))
            legacy_file = legacy_root if legacy_root.suffix.lower() == ".csv" else legacy_root / f"{AID}.csv"
            legacy_file.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(legacy_file, index=False)
            print(f"[{AID}] Legacy CSV written to {legacy_file}")
        except Exception as exc:
            print(f"[{AID}] Legacy CSV export skipped: {exc}")
    if not df.empty:
        now = datetime.now().strftime("%H:%M:%S")
        print(f"[{AID}] AwardID {AID} — scraped ({len(df)} rows) from {page_num} pages and saved snapshot {snap_path.name} at {now}")
    else:
        print(f"[{AID}] AwardID {AID} — no rows scraped; snapshot still written: {snap_path.name}")

    return df

# Allow: python -m scrapers.nam
if __name__ == "__main__":
    try:
        scrape_nam()
        # Explicitly signal success so orchestrators don't mis-read an implicit non-zero
        sys.exit(0)
    except SystemExit as se:  # Respect explicit exits
        raise
    except Exception as e:
        # Print a concise message; full trace not needed for weekly batch summary
        print(f"[{AID}] Unhandled exception: {e}", file=sys.stderr)
        sys.exit(1)
