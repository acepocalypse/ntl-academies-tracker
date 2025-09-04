# scrapers/nas.py
from __future__ import annotations

import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException

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

# ----------------------------
# Core scraper
# ----------------------------
def scrape_nas() -> pd.DataFrame:
    """
    Scrape NAS directory into a normalized DataFrame with a stable primary key (profile_url).
    Saves a timestamped snapshot under snapshots/2023/, and (optionally) the flat CSV to filepath+2023.csv.
    """
    driver = new_driver(headless=True)
    driver.implicitly_wait(5)

    # --- Get initial page ---
    try:
        driver.get(BASE_URL)
        WebDriverWait(driver, WAIT_SEC).until(
            EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'fl-post-grid-post')]"))
        )
        print(f"Accessed: {BASE_URL}")
    except Exception as e:
        print(f"Error loading initial directory: {e}")
        driver.quit()
        return pd.DataFrame()

    # --- Link collection ---
    links: List[str] = []
    current_page = 1
    print("Starting link collection...")

    while True:
        print(f"Processing directory page {current_page}...")
        try:
            member_cards = WebDriverWait(driver, WAIT_SEC).until(
                EC.presence_of_all_elements_located((By.XPATH, "//div[contains(@class, 'fl-post-grid-post')]"))
            )
            links_found_on_page = 0
            for card in member_cards:
                try:
                    link_element = card.find_element(By.XPATH, ".//h5/a")
                    href = (link_element.get_attribute("href") or "").strip()
                    if href and href not in links:
                        links.append(href)
                        links_found_on_page += 1
                except NoSuchElementException:
                    continue
            print(f"  Found {links_found_on_page} new links on page {current_page}.")

            # Pagination
            try:
                next_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, "//a[@class='next page-numbers']"))
                )
                # Click via JS for robustness
                driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                time.sleep(0.5)
                driver.execute_script("arguments[0].click();", next_button)
                # Simple pause; if flaky, upgrade to EC.staleness_of on a sentinel
                time.sleep(PAGE_PAUSE)
                current_page += 1
            except (NoSuchElementException, TimeoutException):
                print("No 'Next' button — reached last page.")
                break
        except TimeoutException:
            print(f"Timed out waiting for member cards on page {current_page}. Stopping link collection.")
            break
        except Exception as e:
            print(f"Unexpected error on page {current_page}: {e}")
            break

    print(f"\nCompleted link extraction. Total unique links: {len(links)}")

    # --- Detail extraction ---
    db: List[Dict[str, str]] = []
    print("Starting detail extraction...")
    for i, link in enumerate(links, 1):
        print(f"Member {i}/{len(links)}: {link}")
        try:
            driver.get(link)

            member: Dict[str, str] = {
                "id": AID,
                "govid": GOVID,
                "govname": GOVNAME,
                "award": AWARD,
                "year": "",
                "name": "",
                "affiliation": "",
                "deceased": "",
                "profile_url": link,  # PRIMARY KEY
            }

            # Name
            try:
                name_el = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, "//span[@class='fl-heading-text']"))
                )
                member["name"] = clean_name(name_el.text)
            except (NoSuchElementException, TimeoutException):
                member["name"] = ""

            # Affiliation (NAS page uses fixed node id for the block; keep fallback)
            try:
                aff_div = driver.find_element(By.XPATH, "//div[@data-node='jd7ypfvaiw1h']")
                paragraphs = aff_div.find_elements(By.TAG_NAME, "p")
                aff_text = "\n".join([norm_text(p.text) for p in paragraphs if norm_text(p.text)])
                member["affiliation"] = aff_text
            except NoSuchElementException:
                member["affiliation"] = ""

            # Dynamic meta items
            try:
                meta_items = driver.find_elements(By.XPATH, "//div[contains(@class, 'meta-item')]")
                for item in meta_items:
                    try:
                        p_elems = item.find_elements(By.XPATH, ".//div[contains(@class, 'fl-rich-text')]/p")
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

            if member.get("deceased") != "Y":
                member["deceased"] = ""

            # Normalize fields
            for k in list(member.keys()):
                member[k] = clean_name(member[k]) if k == "name" else norm_text(member[k])

            db.append(member)
            # Small polite pause if desired
            # time.sleep(0.1)

        except TimeoutException:
            print(f"Timed out loading profile page: {link}. Skipping.")
            continue
        except Exception as e:
            print(f"Error processing profile {link}: {e}. Skipping.")
            continue

    driver.quit()
    print("\nBrowser closed.")

    # Build DataFrame
    if not db:
        print("No data scraped.")
        return pd.DataFrame()

    df = pd.DataFrame(db, dtype=str).fillna("")
    # Example filter retained from your snippet (keep if meaningful for NAS)
    if "public_welfare_medal" in df.columns:
        df = df[~((df["public_welfare_medal"].notna()) & (df["year"] == ""))]

    # De-dupe on primary key
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

    # Also write your legacy flat CSV if `filepath` exists in runtime
    try:
        df.to_csv(f"{filepath}{AID}.csv", index=False)  # type: ignore[name-defined]
    except NameError:
        pass

    if not df.empty:
        now = datetime.now().strftime("%H:%M:%S")
        print(f"AwardID {AID} — scraped ({len(df)} rows) and saved snapshot {snap_path.name} at {now}")
    else:
        print(f"AwardID {AID} — no rows scraped; snapshot still written: {snap_path.name}")

    return df

# Allow running as a module: python -m scrapers.nas
if __name__ == "__main__":
    scrape_nas()
