#!/usr/bin/env python3
"""
DNAFL Scraper v6.0
Aggregates Florida animal abuser registries using specific user-provided
endpoints.

Changelog v6.0:
- FIX: `standardize_data` wrapped the date parser in `pd.to_datetime()` to
  fix the `.dt accessor` crash from Hillsborough.
- FIX: `scrape_volusia` regex was rewritten to be more robust.
- FIX: `scrape_osceola` regex was improved (correctly finds 0 records).
- NOTE: All Selenium 'Errno 2' crashes are an ENVIRONMENT issue.
  Run: sudo apt-get install -y libnss3 libatk-bridge2.0-0 libgbm1 xvfb
  Then run with: xvfb-run python3 scraper.py --dry-run
"""

import os
import sys
import logging
import json
import time
import re
import io
import threading  # Added for lock
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

# Third-party imports
import pandas as pd
import gspread
import requests
from google.oauth2.service_account import Credentials
from bs4 import BeautifulSoup
import pdfplumber
from dateutil.parser import parse as date_parse  # For flexible date parsing
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException,
)
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager

# Try importing tenacity for retries
try:
    from tenacity import (
        retry,
        retry_if_exception_type,
        stop_after_attempt,
        wait_exponential,
    )

    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

    def retry(*args, **kwargs):
        return lambda f: f

    stop_after_attempt = wait_exponential = retry_if_exception_type = None


# --- CONFIGURATION ---
SHEET_ID = os.getenv("SHEET_ID", "1V0ERkUXzc2G_SvSVUaVac50KyNOpw4N7bL6yAiZospY")
MASTER_TAB_NAME = "Master_Registry"  # This will be the combined tab
CREDENTIALS_FILE = "credentials.json"
GOOGLE_CREDENTIALS_ENV = os.getenv("GOOGLE_CREDENTIALS")
WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL")

SELENIUM_TIMEOUT = 30
MAX_WORKERS = 12
DRY_RUN = "--dry-run" in sys.argv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s",
)
logger = logging.getLogger("DNAFL_Scraper")

if not TENACITY_AVAILABLE:
    logger.warning("Tenacity library not available. Retries are disabled.")

# --- CORE UTILITIES ---

# Thread-safe lock and global path for WebDriver Manager
driver_manager_lock = threading.Lock()
GLOBAL_DRIVER_PATH = None


class SeleniumDriver:
    def __enter__(self):
        global GLOBAL_DRIVER_PATH  # Use the global path
        opts = Options()
        opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--log-level=3")
        opts.add_argument("--window-size=1920,1080")
        opts.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 "
            "Safari/537.36"
        )

        try:
            with driver_manager_lock:
                logger.debug("Acquired lock for driver manager.")
                if GLOBAL_DRIVER_PATH is None:
                    logger.info("First thread: Installing/caching chromedriver...")
                    
                    driver_path = ChromeDriverManager().install()
                    
                    logger.info("Verifying driver executable...")
                    verified = False
                    for i in range(5):
                        if os.path.exists(driver_path):
                            if not os.access(driver_path, os.X_OK):
                                try:
                                    os.chmod(driver_path, 0o755)
                                    logger.info("Set execute permissions on chromedriver.")
                                except Exception as chmod_e:
                                    logger.warning(f"Failed to set permissions: {chmod_e}")
                            
                            if os.access(driver_path, os.X_OK):
                                logger.info(f"Driver verified and executable at: {driver_path}")
                                GLOBAL_DRIVER_PATH = driver_path
                                verified = True
                                break
                        logger.warning(
                            f"Driver not ready, waiting... (Attempt {i+1}/5)"
                        )
                        time.sleep(0.5)

                    if not verified:
                        raise Exception(
                            f"Failed to install or verify chromedriver at {driver_path}"
                        )
                else:
                    logger.debug(f"Using cached driver path: {GLOBAL_DRIVER_PATH}")

            service = ChromeService(executable_path=GLOBAL_DRIVER_PATH)
            self.driver = webdriver.Chrome(service=service, options=opts)

        except Exception as e:
            logger.critical(
                f"Failed to initialize SeleniumDriver: {e}"
            )
            logger.critical(
                "THIS IS LIKELY A LINUX ENVIRONMENT ERROR. "
                "Run: sudo apt-get install -y libnss3 libatk-bridge2.0-0 libgbm1 xvfb"
            )
            raise

        self.driver.set_page_load_timeout(60)
        return self.driver

    def __exit__(self, *_):
        if hasattr(self, "driver"):
            self.driver.quit()


def alert_failure(message):
    logger.error(message)
    if WEBHOOK_URL and not DRY_RUN:
        try:
            requests.post(
                WEBHOOK_URL,
                json={"text": f"🚨 **DNAFL Scraper Alert** 🚨\n{message}"},
                timeout=5,
            )
        except Exception as e:
            logger.error(f"Webhook post failed: {e}")


def get_gspread_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    try:
        if GOOGLE_CREDENTIALS_ENV:
            creds_json = json.loads(GOOGLE_CREDENTIALS_ENV)
            creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
        elif os.path.exists(CREDENTIALS_FILE):
            creds = Credentials.from_service_account_file(
                CREDENTIALS_FILE, scopes=scopes
            )
        else:
            logger.error("No Google credentials found.")
            return None
        return gspread.authorize(creds)
    except Exception as e:
        logger.error(f"Failed to create gspread client: {e}")
        return None


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(requests.exceptions.RequestException)
    if TENACITY_AVAILABLE
    else None,
)
def fetch_url(url, stream=False, verify=True):
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
        }
        resp = requests.get(
            url, timeout=45, stream=stream, verify=verify, headers=headers
        )
        resp.raise_for_status()
        return resp
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error fetching {url}: {e}")
        raise
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Connection error fetching {url}: {e}")
        raise
    except requests.exceptions.Timeout as e:
        logger.error(f"Timeout fetching {url}: {e}")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"Request exception fetching {url}: {e}")
        raise


def extract_text_from_pdf(url):
    """Helper to robustly extract all text from a PDF URL."""
    text_content = []
    try:
        resp = fetch_url(url, stream=False, verify=False)
        pdf_file = io.BytesIO(resp.content)

        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text(x_tolerance=1, y_tolerance=1)
                if page_text:
                    text_content.append(page_text) # Append full page text
    except requests.exceptions.RequestException as e:
        logger.warning(f"Failed to fetch PDF from {url}: {e}")
    except pdfplumber.PDFSyntaxError as e:
        logger.warning(f"Invalid PDF syntax for {url}: {e}")
    except Exception as e:
        logger.warning(f"PDF extraction error for {url}: {e}")
    # Return list of page texts
    return text_content


# Define the final, standardized schema for all tabs
FINAL_COLUMNS = [
    "Name",
    "Date",  # Date of conviction/enjoinment
    "County",
    "Type",  # Convicted / Enjoined
    "Source",  # e.g., "Lee Registry"
    "DOB",  # Date of Birth
    "Address",
    "CaseNumber",
    "Charges",  # Or Offense, Restrictions
    "RegistrationEnd",  # Expiration date
    "Link",  # Mugshot, case link
    "Details",  # Catch-all for extra info
]


def standardize_data(df):
    """
    Standardizes a DataFrame to match the FINAL_COLUMNS schema.
    """
    if df.empty:
        return df
    try:
        for col in FINAL_COLUMNS:
            if col not in df.columns:
                df[col] = "N/A"

        df = df.reindex(columns=FINAL_COLUMNS)

        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = (
                    df[col]
                    .fillna("N/A")
                    .astype(str)
                    .str.strip()
                    .str.replace(r"\s+", " ", regex=True)
                )

        if "Name" in df.columns:
            logger.debug("Normalizing 'Name' column...")
            df["Name"] = df["Name"].str.upper().str.replace(r"[.,]", "", regex=True)
            df["Name"] = df["Name"].str.replace(
                r"^\s*([A-Z\'-]+)\s*,\s*([A-Z\s\'-]+)\s*$",
                r"\2 \1",
                regex=True,
            )
            df["Name"] = df["Name"].str.replace(r"\s+", " ", regex=True).str.strip()

        if "Date" in df.columns:
            def flexible_date_parse(date_str):
                if date_str in ["N/A", "Unknown", ""]:
                    return pd.NaT
                try:
                    # v5.9 FIX: Set fuzzy=True to parse "28-JAN-2019" etc.
                    return date_parse(date_str, fuzzy=True)
                except:
                    return pd.NaT

            # v6.0 FIX: Wrap in pd.to_datetime to fix .dt accessor crash
            df["Date_Parsed"] = pd.to_datetime(
                df["Date"].apply(flexible_date_parse), errors='coerce'
            )
            df["Date"] = df["Date_Parsed"].dt.strftime("%Y-%m-%d").fillna("Unknown")
            # --- END v6.0 FIX ---

        return (
            df.drop(columns=["Date_Parsed"], errors="ignore")
            .sort_values("Date", ascending=False)
            .drop_duplicates(subset=["Name", "County", "Date"])
        )
    except Exception as e:
        logger.error(f"Error standardizing data: {e}")
        return pd.DataFrame()


def upload_to_sheet(gc, sheet_id, tab_name, df):
    """
    Helper function to upload a DataFrame to a specific tab.
    """
    if df.empty:
        logger.warning(f"No data to upload for tab '{tab_name}'. Skipping.")
        return

    if DRY_RUN:
        try:
            filename = f"dry_run_{tab_name.replace(' ', '_')}.csv"
            df.to_csv(filename, index=False)
            logger.info(f"DRY RUN: Saved {len(df)} records to {filename}")
        except Exception as e:
            logger.error(f"DRY RUN: Failed to save CSV for {tab_name}: {e}")
        return

    try:
        sh = gc.open_by_key(sheet_id)
        try:
            wks = sh.worksheet(tab_name)
        except gspread.WorksheetNotFound:
            logger.info(f"Creating new tab: '{tab_name}'")
            wks = sh.add_worksheet(tab_name, 1, 1)

        logger.info(f"Uploading {len(df)} records to tab '{tab_name}'...")
        wks.clear()
        wks.freeze(rows=1)
        wks.update(
            [df.columns.tolist()] + df.astype(str).values.tolist(),
            value_input_option="USER_ENTERED",
        )
        logger.info(f"Upload to '{tab_name}' complete.")

    except gspread.exceptions.APIError as e:
        alert_failure(f"Google Sheets API error during upload to {tab_name}: {e}")
    except Exception as e:
        alert_failure(f"Upload Failed for {tab_name}: {e}")


# --- SCRAPERS ---

def scrape_lee():
    data = []
    # 1. Enjoined List (Static, keep using BS4 for speed)
    try:
        resp = fetch_url(
            "https://www.sheriffleefl.org/animal-abuser-registry-enjoined/"
        )
        soup = BeautifulSoup(resp.content, "html.parser")
        table = soup.find("table")
        if table:
            for row in table.find_all("tr")[1:]:
                cols = [c.get_text(strip=True) for c in row.find_all("td")]
                if len(cols) >= 3:
                    data.append(
                        {
                            "Name": cols[0],
                            "Date": cols[2],
                            "County": "Lee",
                            "Source": "Lee Enjoined",
                            "Type": "Enjoined",
                            "CaseNumber": cols[1],
                        }
                    )
    except Exception as e:
        alert_failure(f"Lee Enjoined failed: {str(e)[:200]}")

    # 2. Dynamic Registry Search (with pagination)
    try:
        with SeleniumDriver() as driver:
            driver.get("https://www.sheriffleefl.org/animal-abuser-search/")
            wait = WebDriverWait(driver, SELENIUM_TIMEOUT)
            try:
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "tbody")))
            except TimeoutException:
                logger.warning("Lee Registry: No initial table found.")
            page_num = 1
            while True:
                try:
                    rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
                    page_count = 0
                    for row in rows:
                        try:
                            cols = row.find_elements(By.TAG_NAME, "td")
                            if len(cols) >= 4:
                                name, dob, address, charges = (
                                    cols[0].text.strip(),
                                    cols[1].text.strip(),
                                    cols[2].text.strip(),
                                    cols[3].text.strip(),
                                )
                                img_url = "N/A"
                                try:
                                    img_elem = row.find_element(By.TAG_NAME, "img")
                                    src = img_elem.get_attribute("src")
                                    if src:
                                        img_url = urljoin(driver.current_url, src)
                                except NoSuchElementException:
                                    pass
                                if name:
                                    data.append(
                                        {
                                            "Name": name,
                                            "Date": "Unknown",
                                            "County": "Lee",
                                            "Source": "Lee Registry",
                                            "Type": "Convicted",
                                            "DOB": dob,
                                            "Address": address,
                                            "Charges": charges,
                                            "Link": img_url,
                                        }
                                    )
                                    page_count += 1
                        except StaleElementReferenceException:
                            logger.warning(
                                f"Stale element in Lee row extraction on page {page_num}"
                            )
                            continue
                    logger.info(
                        f"Lee Registry Page {page_num}: Extracted {page_count} records."
                    )
                    try:
                        next_btn = driver.find_element(
                            By.XPATH,
                            "//a[contains(text(),'Next') or contains(text(),'>')]",
                        )
                        if "disabled" in next_btn.get_attribute(
                            "class"
                        ) or not next_btn.is_enabled():
                            break
                        driver.execute_script(
                            "arguments[0].scrollIntoView(true);", next_btn
                        )
                        next_btn.click()
                        page_num += 1
                        wait.until(EC.staleness_of(rows[0]))
                        wait.until(
                            EC.presence_of_element_located((By.TAG_NAME, "tbody"))
                        )
                    except (NoSuchElementException, TimeoutException):
                        logger.info("Lee Registry: Reached last page.")
                        break
                    except StaleElementReferenceException:
                        logger.warning(
                            f"Stale next button on Lee page {page_num}"
                        )
                        break
                except Exception as e:
                    logger.warning(f"Error on Lee page {page_num}: {e}")
                    break
    except Exception as e:
        alert_failure(f"Lee Registry Selenium failed: {str(e)[:200]}")
    return pd.DataFrame(data)


def scrape_marion():
    data = []
    # 1. Static Abuser Registry (Convicted) - Using Selenium to bypass 403
    registry_url = "https://animalservices.marionfl.org/animal-control/animal-control-and-pet-laws/animal-abuser-registry"
    try:
        with SeleniumDriver() as driver:
            driver.get(registry_url)
            wait = WebDriverWait(driver, SELENIUM_TIMEOUT)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            soup = BeautifulSoup(driver.page_source, "html.parser")
            entries = soup.find_all(["p", "li"], string=re.compile(r"Name:", re.I))
            for entry in entries:
                text = entry.get_text(separator=" | ").strip()
                name_match = re.search(r"Name:\s*([^|]+)", text, re.I)
                date_match = re.search(r"(Conviction) Date:\s*([^|]+)", text, re.I)
                if name_match:
                    name = name_match.group(1).strip()
                    date = date_match.group(2).strip() if date_match else "Unknown"
                    img_url = "N/A"
                    try:
                        img_tag = entry.find("img")
                        if not img_tag:
                            img_tag = soup.find(
                                "img",
                                alt=re.compile(
                                    re.escape(name.split()[0]) + r".*mugshot", re.I
                                ),
                            )
                        if img_tag and img_tag.get("src"):
                            img_url = urljoin(registry_url, img_tag["src"])
                    except Exception:
                        pass
                    data.append(
                        {
                            "Name": name,
                            "Date": date,
                            "County": "Marion",
                            "Source": "Marion Registry",
                            "Type": "Convicted",
                            "Link": img_url,
                            "Details": text,  # Store the raw text as details
                        }
                    )
    except Exception as e:
        alert_failure(f"Marion Registry (Selenium) failed: {str(e)[:200]}")

    # 2. Dynamic Enjoinment List (Requires Selenium)
    enjoined_url = "https://animalservices.marionfl.org/animal-control/animal-control-and-pet-laws/civil-enjoinment-list"
    try:
        with SeleniumDriver() as driver:
            driver.get(enjoined_url)
            wait = WebDriverWait(driver, SELENIUM_TIMEOUT)
            try:
                query_btn_xpath = (
                    "//input[@value='Query'] | //button[contains(text(),'Query')]"
                )
                query_button = wait.until(
                    EC.element_to_be_clickable((By.XPATH, query_btn_xpath))
                )
                driver.execute_script("arguments[0].scrollIntoView();", query_button)
                query_button.click()
            except (NoSuchElementException, TimeoutException) as e:
                logger.warning(
                    f"Marion Enjoined: Query button not found or clickable: {e}"
                )
            try:
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            except TimeoutException:
                logger.warning("Marion Enjoined: No table found after query.")
                return pd.DataFrame(data)
            rows = driver.find_elements(By.CSS_SELECTOR, "table tr")[1:]
            for row in rows:
                try:
                    cols = [c.text for c in row.find_elements(By.TAG_NAME, "td")]
                    if len(cols) >= 4:
                        data.append(
                            {
                                "Name": cols[0],
                                "Date": cols[2] if cols[2] else "Unknown",
                                "County": "Marion",
                                "Source": "Marion Enjoined",
                                "Type": "Enjoined",
                                "Address": cols[1],
                                "CaseNumber": cols[3],
                            }
                        )
                except StaleElementReferenceException:
                    logger.warning("Stale element in Marion Enjoined row extraction")
                    continue
    except Exception as e:
        alert_failure(f"Marion Enjoined (Selenium) failed: {str(e)[:200]}")
    return pd.DataFrame(data)


def scrape_hillsborough():
    """
    UPDATED: Enjoined list is now a PDF; extract from PDF with improved parsing.
    """
    data = []

    # --- 1. Enjoined List (PDF) ---
    enjoined_pdf_url = "https://assets.contentstack.io/v3/assets/blteea73b27b731f985/bltc47cc1e37ac0e54a/Enjoinment%20List.pdf"
    try:
        resp = fetch_url(enjoined_pdf_url, stream=False)
        pdf_file = io.BytesIO(resp.content)
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                try:
                    # --- v5.7 FIX: Removed 'keep_blank_chars' ---
                    table_settings = {
                        "vertical_strategy": "lines",
                        "horizontal_strategy": "lines",
                        "text_tolerance": 1,
                        "intersection_tolerance": 2,
                    }
                    tables = page.extract_tables(table_settings)
                    for table in tables:
                        if not table:
                            continue
                        for row in table[1:]:
                            cleaned_row = [cell.strip() if cell else "" for cell in row]
                            if (
                                len([c for c in cleaned_row if c]) < 4
                                or "Name" in cleaned_row[0]
                            ):
                                continue
                            name, start_date, end_date, restrictions = (
                                cleaned_row[0],
                                cleaned_row[1] if len(cleaned_row) > 1 else "Unknown",
                                cleaned_row[2]
                                if len(cleaned_row) > 2
                                else "Permanent",
                                cleaned_row[3] if len(cleaned_row) > 3 else "N/A",
                            )
                            if name:
                                data.append(
                                    {
                                        "Name": name, # Name is already "Last, First"
                                        "Date": start_date,
                                        "County": "Hillsborough",
                                        "Source": "Hillsborough Enjoined",
                                        "Type": "Enjoined",
                                        "RegistrationEnd": end_date,
                                        "Charges": restrictions,
                                        "Details": "Extracted from PDF",
                                    }
                                )
                except Exception as e:
                    logger.warning(
                        f"Error extracting table from Hillsborough Enjoined PDF page: {e}"
                    )
        logger.info(
            f"Hillsborough Enjoined: Extracted {len(data)} records from PDF."
        )
    except Exception as e:
        alert_failure(f"Hillsborough Enjoined PDF failed: {str(e)[:200]}")

    # --- 2. General Registry (HTML) ---
    registry_url = "https://hcfl.gov/residents/animals-and-pets/animal-abuser-registry/search-the-registry"
    try:
        with SeleniumDriver() as driver:
            driver.get(registry_url)
            wait = WebDriverWait(driver, SELENIUM_TIMEOUT)
            try:
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            except TimeoutException:
                logger.warning("Hillsborough Registry: No table found; appears empty.")
                return pd.DataFrame(data)

            page_num = 1
            while True:
                try:
                    rows = driver.find_elements(By.CSS_SELECTOR, "table tr")[1:]
                    if not rows:
                        logger.warning(
                            f"Hillsborough Registry: No rows found on page {page_num}."
                        )
                        break
                    for row in rows:
                        try:
                            cols = [
                                c.text for c in row.find_elements(By.TAG_NAME, "td")
                            ]
                            if len(cols) >= 4:
                                name, dob, address, charges = (
                                    cols[0],
                                    cols[1],
                                    cols[2],
                                    cols[3],
                                )
                                img_url = "N/A"
                                try:
                                    img_elem = row.find_element(By.TAG_NAME, "img")
                                    src = img_elem.get_attribute("src")
                                    if src:
                                        img_url = urljoin(driver.current_url, src)
                                except NoSuchElementException:
                                    pass
                                data.append(
                                    {
                                        "Name": name,
                                        "Date": "Unknown",
                                        "County": "Hillsborough",
                                        "Source": "Hillsborough Registry",
                                        "Type": "Convicted",
                                        "DOB": dob,
                                        "Address": address,
                                        "Charges": charges,
                                        "Link": img_url,
                                    }
                                )
                        except StaleElementReferenceException:
                            logger.warning(
                                f"Stale element in Hillsborough Registry row extraction on page {page_num}"
                            )
                            continue
                    logger.info(
                        f"Hillsborough Registry: Scraped page {page_num}."
                    )
                    try:
                        next_btn = wait.until(
                            EC.element_to_be_clickable(
                                (
                                    By.XPATH,
                                    "//a[contains(text(),'Next') or contains(text(),'>')]",
                                )
                            )
                        )
                        if "disabled" in next_btn.get_attribute(
                            "class"
                        ) or not next_btn.is_enabled():
                            logger.info("Hillsborough Registry: Next btn disabled.")
                            break
                        driver.execute_script(
                            "arguments[0].scrollIntoView(true);", next_btn
                        )
                        next_btn.click()
                        page_num += 1
                        wait.until(EC.staleness_of(rows[0]))
                        wait.until(
                            EC.presence_of_element_located(
                                (By.CSS_SELECTOR, "table tr")
                            )
                        )
                    except (TimeoutException, NoSuchElementException):
                        logger.info("Hillsborough Registry: No next button found.")
                        break
                    except StaleElementReferenceException:
                        logger.warning(
                            f"Stale next button on Hillsborough Registry page {page_num}"
                        )
                        break
                except Exception as e:
                    logger.warning(
                        f"Error on Hillsborough Registry page {page_num}: {e}"
                    )
                    break
    except Exception as e:
        alert_failure(f"Hillsborough Registry (Selenium) failed: {str(e)[:200]}")

    return pd.DataFrame(data)


def scrape_volusia():
    """
    v6.0 FIX: This PDF is not a table. Switched to regex text parsing.
    """
    data = []
    pdf_url = (
        "https://vcservices.vcgov.org/AnimalControlAttachments/VolusiaAnimalAbuse.pdf"
    )
    try:
        # Get text from all pages and join into one block
        all_text = "\n".join(extract_text_from_pdf(pdf_url))
        
        # Regex to find a complete record block.
        # It looks for "Name:", then captures everything until the next "Name:"
        # (?s) = dotall, . matches newline
        # `re.split` is better here, splitting by the delimiter `Name:`
        entries = re.split(r"Name:", all_text, flags=re.IGNORECASE)
        
        if len(entries) <= 1:
            logger.warning("Volusia: PDF split on 'Name:' resulted in 1 entry. Check parser.")

        for entry in entries:
            entry = entry.strip().replace("’", "'") # Fix encoding
            if not entry or "Animal Abuse Registry" in entry:
                continue

            # `entry` now starts with the name (e.g., "DOE, JOHN \n DOB: ...")
            lines = entry.split('\n')
            name = lines[0].strip()
            
            record_text = "\n".join(lines[1:])
            record = {"Name": name}

            # Use re.search to find key-value pairs in the remaining block
            dob_match = re.search(r"DOB:\s*(.*)", record_text, re.IGNORECASE)
            case_match = re.search(r"Case Number:\s*(.*)", record_text, re.IGNORECASE)
            date_match = re.search(r"Conviction Date:\s*(.*)", record_text, re.IGNORECASE)
            offense_match = re.search(r"Offense:\s*([\s\S]*)", record_text, re.IGNORECASE)

            if dob_match:
                record["DOB"] = dob_match.group(1).strip()
            if case_match:
                record["CaseNumber"] = case_match.group(1).strip()
            if date_match:
                record["Date"] = date_match.group(1).strip()
            if offense_match:
                # Capture multi-line offense, stop at the end of the entry
                record["Charges"] = offense_match.group(1).strip().replace('\n', ' ')

            data.append({
                "Name": record.get("Name", "N/A"),
                "Date": record.get("Date", "Unknown"),
                "County": "Volusia",
                "Source": "Volusia PDF",
                "Type": "Convicted",
                "DOB": record.get("DOB", "N/A"),
                "CaseNumber": record.get("CaseNumber", "N/A"),
                "Charges": record.get("Charges", "N/A"),
            })
            
    except Exception as e:
        alert_failure(f"Volusia PDF scraper failed: {str(e)[:200]}")
        
    return pd.DataFrame(data)


def scrape_seminole():
    data = []
    COUNTY_NAME, SOURCE_NAME, RECORD_TYPE = "Seminole", "Seminole PDF", "Convicted"
    landing_page_url = "https://www.seminolecountyfl.gov/departments-services/prepare-seminole/animal-services/animal-abuse-registry"
    try:
        resp = fetch_url(landing_page_url)
        soup = BeautifulSoup(resp.content, "html.parser")
        pdf_link = soup.find(
            "a", string=re.compile(r"(view|download|open|access).*registry|report", re.I)
        )
        if not pdf_link:
            pdf_link = soup.find("a", href=re.compile(r"AnimalCruelty", re.I))
        if not pdf_link or not pdf_link.get("href"):
            logger.warning(
                "Seminole: Could not find dynamic PDF link, trying old static link..."
            )
            pdf_url = "https://scwebapp2.seminolecountyfl.gov:6443/AnimalCruelty/AnimalCrueltyReporty.pdf"
        else:
            pdf_url = urljoin(landing_page_url, pdf_link["href"])
            logger.info(f"Seminole: Found dynamic PDF link: {pdf_url}")

        all_text = "\n".join(extract_text_from_pdf(pdf_url))
        entries = re.split(r"(?=\nName:)", "\n" + all_text, flags=re.IGNORECASE)
        for entry in entries:
            if not entry.strip() or "Name:" not in entry:
                continue
            record, current_key = {}, None
            for line in entry.split("\n"):
                line = line.strip()
                if not line:
                    continue
                match = re.match(r"^([^:]{1,30}):\s*(.*)", line)
                if match:
                    current_key, value = match.groups()
                    current_key = current_key.strip()
                    record[current_key] = value.strip()
                elif current_key:
                    record[current_key] += " " + line
            if "Name" in record:
                date_val = record.get("Adjudication Date") or next(
                    (v for k, v in record.items() if "Date" in k and v.strip()),
                    "Unknown",
                )
                details = " | ".join(
                    [
                        f"{k}: {v}"
                        for k, v in record.items()
                        if k.lower()
                        not in [
                            "name",
                            "date of birth",
                            "case number",
                            "offense",
                            "adjudication date",
                        ]
                    ]
                )
                data.append(
                    {
                        "Name": record.get("Name", "N/A"),
                        "Date": date_val,
                        "County": COUNTY_NAME,
                        "Source": SOURCE_NAME,
                        "Type": RECORD_TYPE,
                        "DOB": record.get("Date of Birth", "N/A"),
                        "CaseNumber": record.get("Case Number", "N/A"),
                        "Charges": record.get("Offense", "N/A"),
                        "Details": details,
                    }
                )
    except Exception as e:
        alert_failure(f"Seminole PDF scraper failed: {str(e)[:200]}")
    return pd.DataFrame(data)


def scrape_pasco():
    data = []
    try:
        with SeleniumDriver() as driver:
            driver.get("https://app.pascoclerk.com/animalabusersearch/")
            try:
                btn_css = "button[type='submit'], .btn-search"
                btn = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, btn_css))
                )
                btn.click()
            except (NoSuchElementException, TimeoutException) as e:
                logger.warning(f"Pasco: Search button not found or clickable: {e}")
            try:
                WebDriverWait(driver, SELENIUM_TIMEOUT).until(
                    EC.presence_of_element_located((By.TAG_NAME, "tr"))
                )
            except TimeoutException:
                logger.warning("Pasco: No table found after search.")
                return pd.DataFrame(data)
            rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
            for row in rows:
                try:
                    cols = [c.text for c in row.find_elements(By.TAG_NAME, "td")]
                    if len(cols) >= 3:
                        data.append(
                            {
                                "Name": cols[0],
                                "Date": cols[2],
                                "County": "Pasco",
                                "Source": "Pasco Clerk App",
                                "Type": "Convicted",
                                "CaseNumber": cols[1],
                            }
                        )
                except StaleElementReferenceException:
                    logger.warning("Stale element in Pasco row extraction")
                    continue
    except Exception as e:
        alert_failure(f"Pasco App failed: {str(e)[:200]}")
    return pd.DataFrame(data)


def scrape_collier():
    data = []
    try:
        resp = fetch_url(
            "https://www2.colliersheriff.org/animalabusesearch", verify=False
        )
        soup = BeautifulSoup(resp.content, "html.parser")
        for row in soup.select("table tr")[1:]:
            cols = [c.get_text(strip=True) for c in row.find_all("td")]
            if len(cols) >= 6:
                date = (
                    cols[5]
                    if cols[5] not in ["N/A", ""]
                    else datetime.now().strftime("%Y-%m-%d")
                )
                data.append(
                    {
                        "Name": cols[1],
                        "Date": date,
                        "County": "Collier",
                        "Source": "Collier Sheriff",
                        "Type": cols[0],
                        "DOB": cols[2],
                        "CaseNumber": cols[4],
                    }
                )
    except Exception as e:
        alert_failure(f"Collier failed: {str(e)[:200]}")
    return pd.DataFrame(data)


def scrape_osceola():
    """
    v6.0 FIX: Switched to regex text parsing to find lines
    containing a case number.
    """
    data = []
    pdf_url = "https://courts.osceolaclerk.com/reports/AnimalCrueltyReportWeb.pdf"
    
    # Regex to find a case number format like 2024-MM-001234
    case_regex = re.compile(r"(\d{4}-\w{2}-\d{6})")
    
    try:
        # Get one text block per page
        page_texts = extract_text_from_pdf(pdf_url)
        
        if not page_texts or "no records found" in page_texts[0].lower():
            logger.warning("Osceola: PDF appears to be empty or says 'no records found'.")
            return pd.DataFrame(data)

        for page_text in page_texts:
            for line in page_text.split('\n'):
                line = line.strip().replace("’", "'") # Fix encoding
                match = case_regex.search(line)
                
                if match:
                    case_num = match.group(1)
                    
                    # Try to extract the name, assuming it's the first part of the line
                    name = line.split(case_num)[0].strip()
                    
                    # Filter out headers
                    if "name" in name.lower() or "case" in name.lower() or not name:
                        continue
                        
                    data.append(
                        {
                            "Name": name,
                            "Date": "Unknown", # No date field in this simple format
                            "County": "Osceola",
                            "Source": "Osceola Clerk PDF",
                            "Type": "Convicted",
                            "CaseNumber": case_num,
                            "Details": line,  # Data is unstructured, save full line
                        }
                    )
    except Exception as e:
        alert_failure(f"Osceola PDF scraper failed: {str(e)[:200]}")
    return pd.DataFrame(data)


def scrape_broward():
    data = []
    logger.info("Broward County no longer has a public animal abuse registry as of 2025.")
    return pd.DataFrame(data)


def scrape_leon():
    data = []
    COUNTY_NAME, SOURCE_NAME, RECORD_TYPE = (
        "Leon",
        "Leon/Tallahassee Registry",
        "Convicted",
    )
    url = "https://www.talgov.com/animals/asc-abuse.aspx"
    try:
        with SeleniumDriver() as driver:
            driver.get(url)
            wait = WebDriverWait(driver, SELENIUM_TIMEOUT)
            page_num = 1
            while True:
                try:
                    table_id = "p_lt_zoneContent_pageplaceholder_p_lt_zoneLeft_TAL_AnimalAbuseRegistry_gvRegistryList"
                    table = wait.until(
                        EC.presence_of_element_located((By.ID, table_id))
                    )
                except TimeoutException:
                    logger.warning(
                        f"{COUNTY_NAME}: No table found on page {page_num}."
                    )
                    break
                rows = table.find_elements(By.TAG_NAME, "tr")[1:]
                if not rows and page_num == 1:
                    logger.warning(f"{COUNTY_NAME}: Table found but no data rows.")
                    break
                rows = [
                    row
                    for row in rows
                    if "gridPager" not in row.get_attribute("class")
                ]
                logger.info(f"{COUNTY_NAME}: Scraping page {page_num}...")
                for row in rows:
                    try:
                        cols = [
                            c.text.strip()
                            for c in row.find_elements(By.TAG_NAME, "td")
                        ]
                        # [Name, Address, Offense Date, Conviction Date, Exp. Date, Offense]
                        if len(cols) >= 6:
                            data.append(
                                {
                                    "Name": cols[0],
                                    "Date": cols[3],
                                    "County": COUNTY_NAME,
                                    "Source": SOURCE_NAME,
                                    "Type": RECORD_TYPE,
                                    "Address": cols[1],
                                    "Charges": cols[5],
                                    "RegistrationEnd": cols[4],
                                    "Details": f"Offense Date: {cols[2]}",
                                }
                            )
                    except StaleElementReferenceException:
                        logger.warning(
                            f"Stale element in {COUNTY_NAME} row extraction on page {page_num}"
                        )
                        continue
                try:
                    if not rows:
                        break
                    first_row = rows[0]
                    next_btn = driver.find_element(By.LINK_TEXT, ">")
                    driver.execute_script(
                        "arguments[0].scrollIntoView(true);", next_btn
                    )
                    next_btn.click()
                    page_num += 1
                    wait.until(EC.staleness_of(first_row))
                except NoSuchElementException:
                    logger.info(f"{COUNTY_NAME}: Reached last page.")
                    break
                except Exception as e:
                    logger.warning(f"{COUNTY_NAME}: Pagination error: {e}")
                    break
    except Exception as e:
        alert_failure(f"{COUNTY_NAME} scraper failed: {str(e)[:200]}")
    return pd.DataFrame(data)


def scrape_polk():
    """
    Restored: This scraper uses Selenium because the div.registrant elements
    are loaded by JavaScript.
    """
    data = []
    COUNTY_NAME, SOURCE_NAME, RECORD_TYPE = "Polk", "Polk Registry", "Convicted"
    url = "https://www.polksheriff.org/animal-abuse-registry"

    try:
        with SeleniumDriver() as driver:
            driver.get(url)
            wait = WebDriverWait(driver, SELENIUM_TIMEOUT)

            try:
                wait.until(
                    EC.presence_of_element_located((By.CLASS_NAME, "registrant"))
                )
            except TimeoutException:
                logger.warning("Polk: No 'div.registrant' elements found on page.")
                return pd.DataFrame(data)

            soup = BeautifulSoup(driver.page_source, "html.parser")
            registrants = soup.find_all("div", class_="registrant")
            logger.info(f"Polk: Found {len(registrants)} registrant divs.")

            for reg in registrants:
                try:
                    name = reg.find("h3").get_text(strip=True)
                    info = {
                        p.find("strong").get_text(strip=True).replace(":", ""): p.find(
                            "span"
                        ).get_text(strip=True)
                        for p in reg.find_all("p")
                        if p.find("strong") and p.find("span")
                    }
                    data.append(
                        {
                            "Name": name,
                            "Date": info.get("Date of Conviction", "Unknown"),
                            "County": COUNTY_NAME,
                            "Source": SOURCE_NAME,
                            "Type": RECORD_TYPE,
                            "Address": info.get("Address", "N/A"),
                            "DOB": info.get("Date of Birth", "N/A"),
                            "Charges": info.get("FL Statute", "N/A"),
                            "RegistrationEnd": info.get(
                                "Registration Expiration", "N/A"
                            ),
                        }
                    )
                except Exception as e:
                    logger.warning(f"Polk: Failed to parse a registrant div: {e}")
    except Exception as e:
        alert_failure(f"{COUNTY_NAME} scraper failed: {str(e)[:200]}")
    return pd.DataFrame(data)


def scrape_orange():
    data = []
    logger.info("Orange County no public animal abuse registry as of 2025.")
    return pd.DataFrame(data)


def scrape_palmbeach():
    """
    Restored: This site URL changed. This is the new, working URL.
    It's a simple HTML table.
    """
    data = []
    COUNTY_NAME, SOURCE_NAME, RECORD_TYPE = (
        "Palm Beach",
        "Palm Beach Registry",
        "Convicted",
    )
    url = "https://secure.mypbc.org/publicsafety/animalcare/Animal-Abuse-Registry"

    try:
        resp = fetch_url(url)
        soup = BeautifulSoup(resp.content, "html.parser")
        
        table = soup.find(
            "table", summary=re.compile(r"Animal Abuse Registry", re.I)
        )
        if not table:
            table = soup.find("table", id=re.compile(r"Registry", re.I))
        
        if not table:
            logger.warning("Palm Beach: No table found on page.")
            return pd.DataFrame(data)
            
        for row in table.find_all("tr")[1:]:
            cols = [c.get_text(strip=True) for c in row.find_all("td")]
            if len(cols) >= 6:
                data.append(
                    {
                        "Name": cols[0],
                        "Date": cols[3],
                        "County": COUNTY_NAME,
                        "Source": SOURCE_NAME,
                        "Type": RECORD_TYPE,
                        "Address": cols[1],
                        "DOB": cols[2],
                        "RegistrationEnd": cols[5],
                        "Details": f"Authority: {cols[4]}",
                    }
                )
    except Exception as e:
        # This will likely fail if the Kali VM has DNS issues
        alert_failure(f"{COUNTY_NAME} scraper failed: {str(e)[:200]}")
    return pd.DataFrame(data)


def scrape_miamidade():
    data = []
    COUNTY_NAME, SOURCE_NAME, RECORD_TYPE = (
        "Miami-Dade",
        "Miami-Dade Registry",
        "Convicted",
    )
    url = "https://www.miamidade.gov/Apps/ASD/crueltyweb/"
    try:
        with SeleniumDriver() as driver:
            driver.get(url)
            wait = WebDriverWait(driver, SELENIUM_TIMEOUT)
            try:
                query_btn_xpath = "//input[@value='Search'] | //button[contains(text(),'Search') or contains(text(),'Query')]"
                query_button = wait.until(
                    EC.element_to_be_clickable((By.XPATH, query_btn_xpath))
                )
                driver.execute_script("arguments[0].scrollIntoView();", query_button)
                query_button.click()
            except (NoSuchElementException, TimeoutException) as e:
                logger.warning(
                    f"{COUNTY_NAME}: Search button not found or clickable: {e}"
                )
                logger.info(
                    f"{COUNTY_NAME}: No search button found, assuming data loads automatically."
                )
            try:
                table = wait.until(
                    EC.presence_of_element_located((By.TAG_NAME, "table"))
                )
            except TimeoutException:
                logger.warning(f"{COUNTY_NAME}: No table found.")
                return pd.DataFrame(data)
            rows = driver.find_elements(By.CSS_SELECTOR, "table tr")[1:]
            for row in rows:
                try:
                    cols = [
                        c.text.strip() for c in row.find_elements(By.TAG_NAME, "td")
                    ]
                    if len(cols) >= 3:
                        details = " | ".join(cols[3:]) if len(cols) > 3 else "N/A"
                        data.append(
                            {
                                "Name": cols[0],
                                "Date": cols[2] if cols[2] else "Unknown",
                                "County": COUNTY_NAME,
                                "Source": SOURCE_NAME,
                                "Type": RECORD_TYPE,
                                "DOB": cols[1],
                                "Details": details,
                            }
                        )
                except StaleElementReferenceException:
                    logger.warning(
                        f"Stale element in {COUNTY_NAME} row extraction"
                    )
                    continue
    except Exception as e:
        alert_failure(f"{COUNTY_NAME} scraper failed: {str(e)[:200]}")
    return pd.DataFrame(data)


def scrape_brevard():
    data = []
    COUNTY_NAME, SOURCE_NAME, RECORD_TYPE = "Brevard", "Brevard Registry", "Convicted"
    url = "https://www.brevardfl.gov/AnimalAbuseDatabaseSearch"
    try:
        with SeleniumDriver() as driver:
            driver.get(url)
            wait = WebDriverWait(driver, SELENIUM_TIMEOUT)
            try:
                name_input = driver.find_element(By.NAME, "defendantName")
                name_input.clear()
            except NoSuchElementException:
                logger.warning(f"{COUNTY_NAME}: No name input found.")
            try:
                query_btn_xpath = (
                    "//input[@type='submit'] | //button[contains(text(),'Search')]"
                )
                query_button = wait.until(
                    EC.element_to_be_clickable((By.XPATH, query_btn_xpath))
                )
                driver.execute_script("arguments[0].scrollIntoView();", query_button)
                query_button.click()
            except (NoSuchElementException, TimeoutException) as e:
                logger.warning(
                    f"{COUNTY_NAME}: Search button not found or clickable: {e}"
                )
                logger.info(f"{COUNTY_NAME}: No search button, assuming auto-load.")
            try:
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "tr")))
            except TimeoutException:
                logger.warning(f"{COUNTY_NAME}: No results table found.")
                return pd.DataFrame(data)
            rows = driver.find_elements(By.CSS_SELECTOR, "table tr")[1:]
            for row in rows:
                try:
                    cols = [
                        c.text.strip() for c in row.find_elements(By.TAG_NAME, "td")
                    ]
                    if len(cols) >= 3:
                        details = " | ".join(cols[3:]) if len(cols) > 3 else "N/A"
                        data.append(
                            {
                                "Name": cols[0],
                                "Date": cols[2] if cols[2] else "Unknown",
                                "County": COUNTY_NAME,
                                "Source": SOURCE_NAME,
                                "Type": RECORD_TYPE,
                                "CaseNumber": cols[1],
                                "Details": details,
                            }
                        )
                except StaleElementReferenceException:
                    logger.warning(
                        f"Stale element in {COUNTY_NAME} row extraction"
                    )
                    continue
    except Exception as e:
        alert_failure(f"{COUNTY_NAME} scraper failed: {str(e)[:200]}")
    return pd.DataFrame(data)


def scrape_manatee():
    """
    v5.7 FIX: Correctly map columns.
    Charges -> Case Type
    Details -> Filing Date | Disposition
    """
    data = []
    COUNTY_NAME, SOURCE_NAME = "Manatee", "Manatee Clerk Animal Cases"
    url = (
        "https://records.manateeclerk.com/Content/animal-cases/Animal-Cases-Last-10.html"
    )
    try:
        resp = fetch_url(url, verify=False)
        soup = BeautifulSoup(resp.content, "html.parser")
        table = soup.find("table")
        if not table:
            logger.warning(f"{COUNTY_NAME}: No table found on page.")
            return pd.DataFrame(data)
            
        for row in table.find_all("tr")[1:]:
            cols = [c.get_text(strip=True) for c in row.find_all("td")]
            if len(cols) >= 5:
                case_number, name, case_type, filing_date = (
                    cols[0],
                    cols[1],
                    cols[2],
                    cols[3],
                )
                disposition_date = cols[4] if cols[4] else "Unknown"
                disposition = cols[5] if len(cols) > 5 else "N/A"
                
                date = disposition_date if disposition_date != "Unknown" else filing_date
                type_ = "Convicted" if "CONVICTED" in disposition.upper() else "Case"
                
                data.append(
                    {
                        "Name": name,
                        "Date": date,
                        "County": COUNTY_NAME,
                        "Source": SOURCE_NAME,
                        "Type": type_,
                        "CaseNumber": case_number,
                        "Charges": case_type, # This is the actual charge
                        "Details": f"Filing Date: {filing_date} | Disposition: {disposition}",
                    }
                )
    except Exception as e:
        alert_failure(f"{COUNTY_NAME} scraper failed: {str(e)[:200]}")
    return pd.DataFrame(data)


def scrape_sarasota():
    data = []
    COUNTY_NAME = "Sarasota"
    url = "https://www.sarasotasheriff.org/programs_and_amp_services/animal_services/vicious_dangerous_dogs.php"
    try:
        fetch_url(url)
        logger.info(
            f"{COUNTY_NAME}: No public abuser list found on the page, only Vicious Dog info."
        )
    except Exception as e:
        alert_failure(f"{COUNTY_NAME} scraper failed: {str(e)[:200]}")
    return pd.DataFrame(data)


def scrape_charlotte():
    data = []
    COUNTY_NAME = "Charlotte"
    url = (
        "https://www.charlottecountyfl.gov/departments/public-safety/animal-control/"
    )
    try:
        fetch_url(url)
        logger.info(f"{COUNTY_NAME}: No public abuser registry found on the page.")
    except Exception as e:
        alert_failure(f"{COUNTY_NAME} scraper failed: {str(e)[:200]}")
    return pd.DataFrame(data)


# --- ORCHESTRATOR ---

def main():
    start_ts = time.time()
    logger.info("Starting DNAFL Scraper Job v6.0 (Volusia/Hillsborough Fix)...")
    gc = get_gspread_client()
    if not gc and not DRY_RUN:
        logger.critical("Credentials missing. Aborting.")
        sys.exit(1)

    # Define tasks as a dict {Tab Name: function}
    tasks = {
        "Lee": scrape_lee,
        "Marion": scrape_marion,
        "Hillsborough": scrape_hillsborough,
        "Volusia": scrape_volusia,
        "Seminole": scrape_seminole,
        "Pasco": scrape_pasco,
        "Collier": scrape_collier,
        "Osceola": scrape_osceola,
        "Broward": scrape_broward,  # Kept, but function logs no data
        "Leon": scrape_leon,
        "Polk": scrape_polk,  # Restored
        "Orange": scrape_orange,  # Kept, but function logs no data
        "Palm Beach": scrape_palmbeach,  # Restored
        "Miami-Dade": scrape_miamidade,
        "Brevard": scrape_brevard,
        "Manatee": scrape_manatee,
        "Sarasota": scrape_sarasota,
        "Charlotte": scrape_charlotte,
    }

    all_data_frames = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {
            executor.submit(func): tab_name for tab_name, func in tasks.items()
        }

        for future in as_completed(future_map):
            tab_name = future_map[future]
            try:
                df = future.result()
                if not df.empty:
                    logger.info(f"[{tab_name}] Success: {len(df)} records.")
                    all_data_frames[tab_name] = df
                else:
                    logger.warning(f"[{tab_name}] yielded 0 records.")
            except Exception as e:
                alert_failure(f"CRITICAL: Scraper for {tab_name} crashed: {e}")

    if all_data_frames:
        standardized_dfs = []

        for tab_name, df in all_data_frames.items():
            if df.empty:
                continue

            logger.info(f"Standardizing data for {tab_name}...")
            standardized_df = standardize_data(df)

            if not standardized_df.empty:
                upload_to_sheet(gc, SHEET_ID, tab_name, standardized_df)
                standardized_dfs.append(standardized_df)
            else:
                logger.warning(
                    f"No data remaining for {tab_name} after standardization."
                )

        if standardized_dfs:
            logger.info("Concatenating all dataframes for Master Registry...")
            master_df = pd.concat(standardized_dfs, ignore_index=True)
            master_df = standardize_data(master_df)

            logger.info(f"TOTAL UNIQUE RECORDS: {len(master_df)}")
            upload_to_sheet(gc, SHEET_ID, MASTER_TAB_NAME, master_df)
        else:
            logger.warning(
                "No data collected from any scraper. Master list not updated."
            )

    else:
        alert_failure("Global Failure: No data scraped from any source.")
        if not DRY_RUN:
            sys.exit(1)

    logger.info(f"Job finished in {time.time() - start_ts:.1f}s")
    logger.info("Note: Statewide registry under Dexter's Law to be implemented by Jan 2026.")


if __name__ == "__main__":
    main()