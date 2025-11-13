#!/usr/bin/env python3
"""
DNAFL Scraper v3.5 (Targeted Sources & Lint-Fixed)
Aggregates Florida animal abuser registries using specific user-provided
endpoints.
"""

import os
import sys
import logging
import json
import time
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Third-party imports
import pandas as pd
import gspread
import requests
from google.oauth2.service_account import Credentials
from bs4 import BeautifulSoup
import pdfplumber
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Try importing tenacity for retries
try:
    from tenacity import (
        retry, stop_after_attempt, wait_exponential,
        retry_if_exception_type
    )
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False
    def retry(*args, **kwargs):
        return lambda f: f
    stop_after_attempt = wait_exponential = retry_if_exception_type = None

# --- CONFIGURATION ---
SHEET_ID = os.getenv('SHEET_ID', '1V0ERkUXzc2G_SvSVUaVac50KyNOpw4N7bL6yAiZospY')
MASTER_TAB_NAME = "Master_Registry"
CREDENTIALS_FILE = 'credentials.json'
GOOGLE_CREDENTIALS_ENV = os.getenv('GOOGLE_CREDENTIALS')
WEBHOOK_URL = os.getenv('ALERT_WEBHOOK_URL')

SELENIUM_TIMEOUT = 30
MAX_WORKERS = 4 # Increased slightly for new task
DRY_RUN = '--dry-run' in sys.argv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(threadName)s: %(message)s'
)
logger = logging.getLogger('DNAFL_Scraper')


# --- CORE UTILITIES ---

class SeleniumDriver:
    def __enter__(self):
        opts = Options()
        opts.add_argument('--headless=new')
        opts.add_argument('--no-sandbox')
        opts.add_argument('--disable-gpu')
        opts.add_argument('--disable-dev-shm-usage')
        opts.add_argument('--log-level=3')
        opts.add_argument("--window-size=1920,1080")
        opts.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 "
            "Safari/537.36"
        )
        self.driver = webdriver.Chrome(options=opts)
        # Set strict page load timeout to fail fast on hung pages
        self.driver.set_page_load_timeout(60)
        return self.driver

    def __exit__(self, *_):
        if hasattr(self, 'driver'):
            self.driver.quit()


def alert_failure(message):
    logger.error(message)
    if WEBHOOK_URL and not DRY_RUN:
        try:
            requests.post(
                WEBHOOK_URL,
                json={'text': f"🚨 **DNAFL Scraper Alert** 🚨\n{message}"},
                timeout=5
            )
        except Exception:
            pass


def get_gspread_client():
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    if GOOGLE_CREDENTIALS_ENV:
        creds_json = json.loads(GOOGLE_CREDENTIALS_ENV)
        creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
    elif os.path.exists(CREDENTIALS_FILE):
        creds = Credentials.from_service_account_file(
            CREDENTIALS_FILE, scopes=scopes
        )
    else:
        return None
    return gspread.authorize(creds)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_url(url, stream=False, verify=True):
    resp = requests.get(url, timeout=45, stream=stream, verify=verify)
    resp.raise_for_status()
    return resp


def extract_text_from_pdf(url):
    """Helper to robustly extract all text from a PDF URL."""
    text_content = []
    try:
        resp = fetch_url(url, stream=True, verify=False)
        with pdfplumber.open(resp.raw) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text_content.extend(page_text.split('\n'))
    except Exception as e:
        logger.warning(f"PDF extraction warning for {url}: {e}")
    return text_content


def standardize_data(df):
    if df.empty:
        return df
    for col in ['Name', 'Date', 'County', 'Source', 'Details', 'Type']:
        if col not in df.columns:
            df[col] = 'N/A'

    # Clean strings
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].fillna('N/A').astype(str).str.strip().str.replace(
                r'\s+', ' ', regex=True
            )

    # Robust Date Parsing
    df['Date_Parsed'] = pd.to_datetime(
        df['Date'], format='%m/%d/%Y', errors='coerce'
    )
    df['Date_Parsed'] = df['Date_Parsed'].fillna(
        pd.to_datetime(df['Date'], format='%Y-%m-%d', errors='coerce')
    )
    df['Date_Parsed'] = df['Date_Parsed'].fillna(
        pd.to_datetime(df['Date'], errors='coerce')
    )
    df['Date'] = df['Date_Parsed'].dt.strftime('%Y-%m-%d').fillna('Unknown')

    return df.drop(columns=['Date_Parsed']).sort_values(
        'Date', ascending=False
    ).drop_duplicates(subset=['Name', 'County', 'Date'])


# --- SCRAPERS ---

def scrape_lee():
    data = []

    # 1. Enjoined List (Static, keep using BS4 for speed)
    try:
        resp = fetch_url(
            "https://www.sheriffleefl.org/animal-abuser-registry-enjoined/"
        )
        soup = BeautifulSoup(resp.content, 'html.parser')
        table = soup.find('table')
        if table:
            for row in table.find_all('tr')[1:]:
                cols = [c.get_text(strip=True) for c in row.find_all('td')]
                if len(cols) >= 3:
                    data.append({
                        'Name': cols[0],
                        'Date': cols[2],
                        'County': 'Lee',
                        'Source': 'Lee Enjoined',
                        'Type': 'Enjoined',
                        'Details': f"Case: {cols[1]}"
                    })
    except Exception as e:
        alert_failure(f"Lee Enjoined failed: {str(e)[:200]}")

    # 2. Dynamic Registry Search (with pagination)
    try:
        with SeleniumDriver() as driver:
            driver.get("https://www.sheriffleefl.org/animal-abuser-search/")
            wait = WebDriverWait(driver, SELENIUM_TIMEOUT)

            # Wait for initial table
            try:
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "tbody")))
            except TimeoutException:
                logger.warning("Lee Registry: No initial table found.")

            page_num = 1
            while True:
                try:
                    # Extract rows from current page
                    rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
                    page_count = 0
                    for row in rows:
                        cols = row.find_elements(By.TAG_NAME, "td")
                        if len(cols) >= 4:
                            name = cols[0].text.strip()
                            dob = cols[1].text.strip()
                            address = cols[2].text.strip()
                            charges = cols[3].text.strip()

                            # Try to find image URL
                            img_url = ''
                            try:
                                img_elem = row.find_element(By.TAG_NAME, "img")
                                src = img_elem.get_attribute('src')
                                if src:
                                    if src.startswith('http'):
                                        img_url = f" | Image: {src}"
                                    else:
                                        img_url = f" | Image: https://www.sheriffleefl.org{src}"  # noqa: E501
                            except NoSuchElementException:
                                pass

                            if name:
                                details = (
                                    f"DOB: {dob} | Charges: {charges} | "
                                    f"Address: {address}{img_url}"
                                )
                                data.append({
                                    'Name': name,
                                    'Date': 'Unknown',
                                    'County': 'Lee',
                                    'Source': 'Lee Registry',
                                    'Type': 'Convicted',
                                    'Details': details
                                })
                                page_count += 1

                    logger.info(
                        f"Lee Registry Page {page_num}: Extracted {page_count} records."
                    )

                    # Pagination Logic
                    try:
                        next_btn = driver.find_element(
                            By.XPATH,
                            "//a[contains(text(),'Next') or contains(text(),'>')]"
                        )
                        if 'disabled' in next_btn.get_attribute('class') or \
                           not next_btn.is_enabled():
                            break

                        driver.execute_script(
                            "arguments[0].scrollIntoView(true);", next_btn
                        )
                        time.sleep(1)
                        next_btn.click()
                        page_num += 1

                        # Wait for table to stale/reload
                        wait.until(EC.staleness_of(rows[0]))
                        wait.until(
                            EC.presence_of_element_located((By.TAG_NAME, "tbody"))
                        )

                    except (NoSuchElementException, TimeoutException):
                        logger.info("Lee Registry: Reached last page.")
                        break

                except Exception as e:
                    logger.warning(f"Error on Lee page {page_num}: {e}")
                    break

    except Exception as e:
        alert_failure(f"Lee Registry Selenium failed: {str(e)[:200]}")

    return pd.DataFrame(data)


def scrape_marion():
    data = []

    # --- 1. Static Abuser Registry (Convicted) ---
    registry_url = (
        "https://animalservices.marionfl.org/animal-control/"
        "animal-control-and-pet-laws/animal-abuser-registry"
    )
    try:
        resp = fetch_url(registry_url)
        soup = BeautifulSoup(resp.content, 'html.parser')

        # Find all relevant entries (paragraphs or list items)
        entries = soup.find_all(['p', 'li'], string=re.compile(r'Name:', re.I))

        for entry in entries:
            text = entry.get_text(separator=' | ').strip()
            name_match = re.search(r'Name:\s*([^|]+)', text, re.I)
            date_match = re.search(r'(Conviction) Date:\s*([^|]+)', text, re.I)

            if name_match:
                name = name_match.group(1).strip()
                date = date_match.group(2).strip() if date_match else 'Unknown'

                # --- Try to find an associated image ---
                img_url = ''
                try:
                    img_tag = entry.find('img')

                    if not img_tag:
                        # Find by alt text regex
                        img_tag = soup.find(
                            'img',
                            alt=re.compile(
                                re.escape(name.split()[0]) + r'.*mugshot', re.I
                            )
                        )

                    if img_tag and img_tag.get('src'):
                        src = img_tag['src']
                        if src.startswith('/'):
                            img_url = 'https://animalservices.marionfl.org' + src
                        elif src.startswith('http'):
                            img_url = src
                except Exception:
                    pass  # Ignore image search failures

                details_text = text
                if img_url:
                    details_text += f" | Image: {img_url}"

                data.append({
                    'Name': name,
                    'Date': date,
                    'County': 'Marion',
                    'Source': 'Marion Registry',
                    'Type': 'Convicted',
                    'Details': details_text
                })
    except Exception as e:
        alert_failure(f"Marion Registry (Static) failed: {str(e)[:200]}")

    # --- 2. Dynamic Enjoinment List (Requires Selenium) ---
    enjoined_url = (
        "https://animalservices.marionfl.org/animal-control/"
        "animal-control-and-pet-laws/civil-enjoinment-list"
    )
    try:
        with SeleniumDriver() as driver:
            driver.get(enjoined_url)
            wait = WebDriverWait(driver, SELENIUM_TIMEOUT)

            # Click the "Query" button to load the table
            query_btn_xpath = "//input[@value='Query'] | //button[contains(text(),'Query')]"  # noqa: E501
            query_button = wait.until(
                EC.element_to_be_clickable((By.XPATH, query_btn_xpath))
            )
            driver.execute_script("arguments[0].scrollIntoView();", query_button)
            query_button.click()

            # Wait for results table
            table = wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))

            rows = driver.find_elements(By.CSS_SELECTOR, "table tr")[1:]
            for row in rows:
                cols = [c.text for c in row.find_elements(By.TAG_NAME, "td")]

                # Map columns: 0=Name, 1=Address, 2=Enjoinment_Date, 3=Case
                if len(cols) >= 4:
                    data.append({
                        'Name': cols[0],
                        'Date': cols[2] if cols[2] else 'Unknown',
                        'County': 'Marion',
                        'Source': 'Marion Enjoined',
                        'Type': 'Enjoined',
                        'Details': f"Address: {cols[1]} | Case: {cols[3]}"
                    })
                elif len(cols) >= 2:  # Fallback
                    data.append({
                        'Name': cols[0],
                        'Date': 'Unknown',
                        'County': 'Marion',
                        'Source': 'Marion Enjoined',
                        'Type': 'Enjoined',
                        'Details': f"Address: {cols[1]}"
                    })
    except Exception as e:
        alert_failure(f"Marion Enjoined (Selenium) failed: {str(e)[:200]}")

    return pd.DataFrame(data)


def scrape_hillsborough():
    data = []

    # 1. Enjoined PDF (Retaining robust table extraction)
    pdf_url = (
        "https://assets.contentstack.io/v3/assets/"
        "blteea73b27b731f985/bltc47cc1e37ac0e54a/Enjoinment%20List.pdf"
    )
    try:
        # Use existing fetch_url helper
        resp = fetch_url(pdf_url, stream=True, verify=False)

        with pdfplumber.open(resp.raw) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    if not table or len(table) < 2:
                        continue

                    headers = [str(h).strip() for h in table[0]]
                    for row in table[1:]:
                        # Map row to headers safely
                        row_list = [str(cell).strip() if cell else '' for cell in row]
                        row_data = dict(zip(headers, row_list + [''] * (len(headers) - len(row))))  # noqa: E501

                        if 'Last Name' in row_data and 'First Name' in row_data:
                            last = row_data.get('Last Name', '').strip()
                            first = row_data.get('First Name', '').strip()
                            name = f"{last} {first}".strip()

                            # Skip empty/header rows
                            if not name or 'Last Name' in name:
                                continue

                            details_parts = []
                            if row_data.get('Case Number'):
                                details_parts.append(
                                    f"Case: {row_data['Case Number']}"
                                )
                            if row_data.get('End Date'):
                                details_parts.append(
                                    f"End: {row_data['End Date']}"
                                )
                            if row_data.get('Special Restrictions'):
                                details_parts.append(
                                    f"Restrictions: {row_data['Special Restrictions']}"  # noqa: E501
                                )

                            data.append({
                                'Name': name,
                                'Date': row_data.get('Start Date', 'Unknown'),
                                'County': 'Hillsborough',
                                'Source': 'Hillsborough Enjoined PDF',
                                'Type': 'Enjoined',
                                'Details': ' | '.join(details_parts)
                            })

    except Exception as e:
        alert_failure(f"Hillsborough PDF scraper failed: {str(e)[:200]}")

    # 2. General Registry (Selenium with new pagination logic)
    try:
        with SeleniumDriver() as driver:
            driver.get(
                "https://hcfl.gov/residents/animals-and-pets/"
                "animal-abuser-registry/search-the-registry"
            )
            wait = WebDriverWait(driver, SELENIUM_TIMEOUT)

            # Wait for initial table load
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))

            page_num = 1
            while True:
                # Extract rows from current page
                rows = driver.find_elements(By.CSS_SELECTOR, "table tr")[1:]
                if not rows:
                    logger.warning(
                        f"Hillsborough Search: No rows found on page {page_num}."
                    )
                    break

                for row in rows:
                    cols = [c.text for c in row.find_elements(By.TAG_NAME, "td")]

                    # Expects 4 cols: Name, DOB, Address, Charges
                    if len(cols) >= 4:
                        name = cols[0]
                        dob = cols[1]
                        address = cols[2]
                        charges = cols[3]

                        # Get image URL, don't download
                        img_url_str = ''
                        try:
                            img_elem = row.find_element(By.TAG_NAME, "img")
                            src = img_elem.get_attribute('src')
                            if src:
                                if src.startswith('/'):
                                    src = 'https://hcfl.gov' + src
                                img_url_str = f" | Image: {src}"
                        except Exception:
                            pass  # No image

                        details = (
                            f"DOB: {dob} | Address: {address} | "
                            f"Charges: {charges}{img_url_str}"
                        )
                        data.append({
                            'Name': name,
                            'Date': 'Unknown',
                            'County': 'Hillsborough',
                            'Source': 'Hillsborough Registry',
                            'Type': 'Convicted',
                            'Details': details
                        })

                logger.info(f"Hillsborough Search: Scraped page {page_num}.")

                # Pagination logic
                try:
                    next_btn = wait.until(
                        EC.element_to_be_clickable((
                            By.XPATH,
                            "//a[contains(text(),'Next') or contains(text(),'>')]"
                        ))
                    )
                    if 'disabled' in next_btn.get_attribute('class') or \
                       not next_btn.is_enabled():
                        logger.info("Hillsborough Search: Next btn disabled.")
                        break  # Last page

                    driver.execute_script(
                        "arguments[0].scrollIntoView(true);", next_btn
                    )
                    time.sleep(1)
                    next_btn.click()
                    page_num += 1

                    # Wait for the page to reload
                    wait.until(EC.staleness_of(rows[0]))
                    wait.until(
                        EC.presence_of_element_located(
                            (By.CSS_SELECTOR, "table tr")
                        )
                    )

                except (TimeoutException, NoSuchElementException):
                    logger.info("Hillsborough Search: No next button found.")
                    break  # No "Next" button

    except Exception as e:
        alert_failure(f"Hillsborough Search (Selenium) failed: {str(e)[:200]}")

    return pd.DataFrame(data)


def scrape_volusia():
    data = []
    pdf_url = "https://vcservices.vcgov.org/AnimalControlAttachments/VolusiaAnimalAbuse.pdf"  # noqa: E501
    try:
        # Use existing fetch_url for consistent timeout/retry handling
        resp = fetch_url(pdf_url, stream=True, verify=False)

        with pdfplumber.open(resp.raw) as pdf:
            for page in pdf.pages:
                # Custom settings optimized for Volusia's grid-based PDF
                table_settings = {
                    "vertical_strategy": "lines_strict",
                    "horizontal_strategy": "lines_strict",
                    "snap_tolerance": 3,
                    "join_tolerance": 3,
                    "edge_min_length": 3,
                    "min_words_vertical": 3,
                    "min_words_horizontal": 1,
                }
                tables = page.extract_tables(table_settings=table_settings)

                for table in tables:
                    if not table:
                        continue
                    for row in table:
                        # Clean and normalize row
                        cleaned_row = [
                            re.sub(r'\s+', ' ', str(cell).strip()) if cell else ''
                            for cell in row
                        ]

                        # Expected: [Name, DOB, Case#, Offense Date, Description]
                        if len(cleaned_row) >= 3 and cleaned_row[0] and \
                           'Name' not in cleaned_row[0]:
                            # Pad row if it's short
                            if len(cleaned_row) < 5:
                                cleaned_row += [''] * (5 - len(cleaned_row))

                            details = (
                                f"DOB: {cleaned_row[1]} | "
                                f"Case: {cleaned_row[2]} | "
                                f"Offense: {cleaned_row[4]}"
                            )
                            data.append({
                                'Name': cleaned_row[0],
                                'Date': cleaned_row[3] if cleaned_row[3] else 'Unknown',  # noqa: E501
                                'County': 'Volusia',
                                'Source': 'Volusia PDF',
                                'Type': 'Convicted',
                                'Details': details
                            })

    except Exception as e:
        alert_failure(f"Volusia PDF improved scraper failed: {str(e)[:200]}")

    return pd.DataFrame(data)


def scrape_seminole():
    data = []
    pdf_url = "https://scwebapp2.seminolecountyfl.gov:6443/AnimalCruelty/AnimalCrueltyReporty.pdf"  # noqa: E501
    try:
        # Use existing fetch_url with stream=True for pdfplumber
        resp = fetch_url(pdf_url, stream=True, verify=False)

        with pdfplumber.open(resp.raw) as pdf:
            # Extract text from all pages
            all_text = '\n'.join(page.extract_text() or '' for page in pdf.pages)

        # Split text into records starting with "Name:"
        # Prepend newline to ensure first record is caught by regex lookahead
        entries = re.split(r'(?=\nName:)', "\n" + all_text, flags=re.IGNORECASE)

        for entry in entries:
            if not entry.strip() or 'Name:' not in entry:
                continue

            record = {}
            current_key = None
            for line in entry.split('\n'):
                line = line.strip()
                if not line:
                    continue

                # Look for "Key: Value" pattern
                match = re.match(r'^([^:]{1,30}):\s*(.*)', line)
                if match:
                    current_key, value = match.groups()
                    current_key = current_key.strip()
                    record[current_key] = value.strip()
                elif current_key:
                    # Continuation of previous key's value
                    record[current_key] += ' ' + line

            if 'Name' in record:
                # Determine best date field
                date_val = record.get('Adjudication Date') or \
                    next(
                        (v for k, v in record.items() if 'Date' in k and v.strip()),  # noqa: E501
                        'Unknown'
                    )

                # Compile all other fields into Details
                details = ' | '.join(
                    [f"{k}: {v}" for k, v in record.items() if k != 'Name']
                )

                data.append({
                    'Name': record['Name'],
                    'Date': date_val,
                    'County': 'Seminole',
                    'Source': 'Seminole PDF',
                    'Type': 'Convicted',
                    'Details': details
                })

    except Exception as e:
        alert_failure(f"Seminole PDF improved scraper failed: {str(e)[:200]}")

    return pd.DataFrame(data)


def scrape_pasco():
    data = []
    try:
        with SeleniumDriver() as driver:
            driver.get("https://app.pascoclerk.com/animalabusersearch/")
            # Pasco's new app might need a "Search" button click
            try:
                btn_css = "button[type='submit'], .btn-search"
                btn = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, btn_css))
                )
                btn.click()
            except Exception:
                pass  # Ignore if no button
            WebDriverWait(driver, SELENIUM_TIMEOUT).until(
                EC.presence_of_element_located((By.TAG_NAME, "tr"))
            )
            for row in driver.find_elements(By.CSS_SELECTOR, "table tbody tr"):
                cols = [c.text for c in row.find_elements(By.TAG_NAME, "td")]
                if len(cols) >= 3:
                    data.append({
                        'Name': cols[0],
                        'Date': cols[2],
                        'County': 'Pasco',
                        'Source': 'Pasco Clerk App',
                        'Type': 'Convicted',
                        'Details': f"Case: {cols[1]}"
                    })
    except Exception as e:
        alert_failure(f"Pasco App failed: {str(e)[:200]}")
    return pd.DataFrame(data)


def scrape_collier():
    data = []
    try:
        resp = fetch_url(
            "https://www2.colliersheriff.org/animalabusesearch", verify=False
        )
        soup = BeautifulSoup(resp.content, 'html.parser')
        for row in soup.select('table tr')[1:]:
            cols = [c.get_text(strip=True) for c in row.find_all('td')]
            if len(cols) >= 6:
                date = cols[5] if cols[5] not in ['N/A', ''] else \
                    datetime.now().strftime('%Y-%m-%d')
                data.append({
                    'Name': cols[1],
                    'Date': date,
                    'County': 'Collier',
                    'Source': 'Collier Sheriff',
                    'Type': cols[0],
                    'Details': f"DOB: {cols[2]} | Case: {cols[4]}"
                })
    except Exception as e:
        alert_failure(f"Collier failed: {str(e)[:200]}")
    return pd.DataFrame(data)


def scrape_osceola():
    data = []
    try:
        lines = extract_text_from_pdf(
            "https://courts.osceolaclerk.com/reports/AnimalCrueltyReportWeb.pdf"
        )
        for line in lines:
            # Look for case number patterns common in Osceola
            if re.search(r'\d{4}-\w{2}-\d+', line):
                parts = [p for p in line.split('  ') if p]
                if len(parts) >= 2:
                    data.append({
                        'Name': parts[0],
                        'Date': 'Unknown',
                        'County': 'Osceola',
                        'Source': 'Osceola Clerk PDF',
                        'Type': 'Convicted',
                        'Details': line
                    })
    except Exception as e:
        alert_failure(f"Osceola PDF failed: {str(e)[:200]}")
    return pd.DataFrame(data)


def scrape_broward():
    """
    Scrapes the Broward County registry.
    This page uses ASP.NET postbacks for pagination.
    """
    data = []
    COUNTY_NAME = "Broward"
    SOURCE_NAME = "Broward Registry"
    RECORD_TYPE = "Convicted"
    url = "https://www.broward.org/AnimalCare/AbusePublicList/AbusePublicList.aspx"

    try:
        with SeleniumDriver() as driver:
            driver.get(url)
            wait = WebDriverWait(driver, SELENIUM_TIMEOUT)

            page_num = 1
            while True:
                # Wait for the table to exist
                try:
                    table = wait.until(
                        EC.presence_of_element_located((By.ID, "gvAbuseList"))
                    )
                except TimeoutException:
                    logger.warning(f"{COUNTY_NAME}: No table found on page {page_num}.") # noqa: E501
                    break

                # Get rows, skip header
                rows = table.find_elements(By.TAG_NAME, "tr")[1:]
                if not rows and page_num == 1:
                    logger.warning(f"{COUNTY_NAME}: Table found but no data rows.")
                    break
                
                logger.info(f"{COUNTY_NAME}: Scraping page {page_num}...")
                
                for row in rows:
                    cols = [c.text.strip() for c in row.find_elements(By.TAG_NAME, "td")]
                    # [Name, DOB, Address, Case, Conviction Date, Reg. End]
                    if len(cols) >= 6:
                        details = (
                            f"DOB: {cols[1]} | Address: {cols[2]} | "
                            f"Case: {cols[3]} | Registration End: {cols[5]}"
                        )
                        data.append({
                            'Name': cols[0], # Already 'Last, First'
                            'Date': cols[4], # Conviction Date
                            'County': COUNTY_NAME,
                            'Source': SOURCE_NAME,
                            'Type': RECORD_TYPE,
                            'Details': details
                        })

                # Pagination Logic: Find the ">" link
                try:
                    # Store first row to check for staleness
                    first_row_id = rows[0].id 
                    
                    next_btn = driver.find_element(By.LINK_TEXT, ">")
                    driver.execute_script(
                        "arguments[0].scrollIntoView(true);", next_btn
                    )
                    time.sleep(1) # Brief pause
                    next_btn.click()
                    page_num += 1

                    # Wait for the page to reload by checking staleness
                    wait.until(
                        EC.staleness_of(driver.find_element(By.ID, first_row_id))
                    )
                except NoSuchElementException:
                    # No ">" link, this is the last page
                    logger.info(f"{COUNTY_NAME}: Reached last page.")
                    break
                except Exception as e:
                    logger.warning(f"{COUNTY_NAME}: Pagination error: {e}")
                    break

    except Exception as e:
        alert_failure(f"{COUNTY_NAME} scraper failed: {str(e)[:200]}")

    return pd.DataFrame(data)


# --- NEW SCRAPER TEMPLATE ---
def scrape_new_county_template():
    """
    TEMPLATE for scraping a new county.
    1. Fill in the COUNTY_NAME, SOURCE_NAME, and URL.
    2. Uncomment one of the methods (BS4, Selenium, or PDF).
    3. Adjust the selectors (e.g., 'table tr') to match the website.
    4. Map the scraped data to the dictionary fields.
    5. Add this function to the `tasks` list in main().
    """
    data = []
    # --- CONFIGURATION ---
    COUNTY_NAME = "New County"
    SOURCE_NAME = "New County Registry"
    # Set the type, e.g., "Convicted" or "Enjoined"
    RECORD_TYPE = "Convicted"

    try:
        # --- METHOD 1: Simple HTML Page (use requests + BeautifulSoup) ---
        # url = "https://www.newcounty.gov/registry"
        # resp = fetch_url(url)
        # soup = BeautifulSoup(resp.content, 'html.parser')
        #
        # # Adjust this selector to find the rows
        # for row in soup.find_all('tr')[1:]:
        #     cols = [c.get_text(strip=True) for c in row.find_all('td')]
        #     if len(cols) >= 3:
        #         data.append({
        #             'Name': cols[0],
        #             'Date': cols[2], # e.g., Date of conviction
        #             'County': COUNTY_NAME,
        #             'Source': SOURCE_NAME,
        #             'Type': RECORD_TYPE,
        #             'Details': f"Case: {cols[1]}" # Add other info
        #         })

        # --- METHOD 2: Dynamic JavaScript Page (use Selenium) ---
        # url = "https://www.newcounty.gov/search-app"
        # with SeleniumDriver() as driver:
        #     driver.get(url)
        #     wait = WebDriverWait(driver, SELENIUM_TIMEOUT)
        #
        #     # Wait for the table/rows to exist
        #     wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr"))) # noqa: E501
        #
        #     # Add logic for clicking 'Search' or 'Next Page' if needed
        #
        #     for row in driver.find_elements(By.CSS_SELECTOR, "table tbody tr"):
        #         cols = [c.text for c in row.find_elements(By.TAG_NAME, "td")]
        #         if len(cols) >= 3:
        #             data.append({
        #                 'Name': cols[0],
        #                 'Date': cols[2],
        #                 'County': COUNTY_NAME,
        #                 'Source': SOURCE_NAME,
        #                 'Type': RECORD_TYPE,
        #                 'Details': f"DOB: {cols[1]}"
        #             })

        # --- METHOD 3: PDF Document (use pdfplumber) ---
        # pdf_url = "https://www.newcounty.gov/registry.pdf"
        # lines = extract_text_from_pdf(pdf_url)
        #
        # for line in lines:
        #     # Add custom regex logic to parse lines
        #     # This example looks for a line with a name and a case number
        #     match = re.search(r'^(.*?)\s+(\d{4}-\w{2}-\d+)', line)
        #     if match:
        #         name = match.group(1).strip()
        #         case_num = match.group(2).strip()
        #         data.append({
        #             'Name': name,
        #             'Date': 'Unknown',
        #             'County': COUNTY_NAME,
        #             'Source': SOURCE_NAME,
        #             'Type': RECORD_TYPE,
        #             'Details': f"Case: {case_num} | Full Line: {line}"
        #         })
        pass # Remove this 'pass' when you uncomment a method

    except Exception as e:
        alert_failure(f"{COUNTY_NAME} scraper failed: {str(e)[:200]}")

    return pd.DataFrame(data)


# --- ORCHESTRATOR ---

def main():
    start_ts = time.time()
    logger.info("Starting DNAFL Scraper Job v3.1 (Lint-Fixed)...")
    gc = get_gspread_client()
    if not gc and not DRY_RUN:
        logger.critical("Credentials missing. Aborting.")
        sys.exit(1)

    tasks = [
        scrape_lee, scrape_marion, scrape_hillsborough, scrape_volusia,
        scrape_seminole, scrape_pasco, scrape_collier, scrape_osceola,
        scrape_broward, # <-- NEWLY ADDED
        # --- To add your new scraper, uncomment the line below ---
        # scrape_new_county_template,
    ]

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {executor.submit(task): task.__name__ for task in tasks}
        for future in as_completed(future_map):
            try:
                df = future.result()
                if not df.empty:
                    logger.info(
                        f"[{future_map[future]}] Success: {len(df)} records."
                    )
                    results.append(df)
                else:
                    logger.warning(
                        f"[{future_map[future]}] yielded 0 records."
                    )
            except Exception as e:
                alert_failure(f"CRITICAL: {future_map[future]} crashed: {e}")

    if results:
        master_df = standardize_data(pd.concat(results, ignore_index=True))
        logger.info(f"TOTAL UNIQUE RECORDS: {len(master_df)}")
        if not DRY_RUN and gc:
            try:
                sh = gc.open_by_key(SHEET_ID)
                try:
                    wks = sh.worksheet(MASTER_TAB_NAME)
                except gspread.WorksheetNotFound:
                    wks = sh.add_worksheet(MASTER_TAB_NAME, 1, 1)
                wks.clear()
                wks.update(
                    [master_df.columns.tolist()] +
                    master_df.astype(str).values.tolist()
                )
                logger.info("Upload to Google Sheets complete.")
            except Exception as e:
                alert_failure(f"Upload Failed: {e}")
        else:
            master_df.to_csv("dry_run_master.csv", index=False)
            logger.info("Dry run complete, saved to CSV.")
    else:
        alert_failure("Global Failure: No data scraped.")
        if not DRY_RUN:
            sys.exit(1)

    logger.info(f"Job finished in {time.time() - start_ts:.1f}s")


if __name__ == '__main__':
    main()