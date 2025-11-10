#!/usr/bin/env python3
"""
DNAFL Scraper: Fetches Florida animal abuser registries from 15+ sources, dedupes, and uploads to Google Sheets.
Runs daily via GitHub Actions at 2 AM UTC.
Tech: Python 3.10+, gspread, pandas, Selenium/BS4/pdfplumber, concurrent.futures.
License: MIT.
"""

import os
import sys
import logging
from datetime import datetime
import time
import re
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import requests
from bs4 import BeautifulSoup
import pdfplumber
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys

# Attempt to import tenacity for robust retries, fallback if not installed
try:
    from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False
    # Dummy decorator if tenacity is missing
    def retry(*args, **kwargs):
        def decorator(func):
            return func
        return decorator
    stop_after_attempt = wait_fixed = retry_if_exception_type = None


# --- Configuration ---
# Use environment variables for flexibility, fallback to hardcoded defaults for local dev
SHEET_ID = os.getenv('SHEET_ID', '1V0ERkUXzc2G_SvSVUaVac50KyNOpw4N7bL6yAiZospY')
# Handle credentials from file or environment variable (common in CI/CD)
CREDENTIALS_FILE = 'credentials.json'
GOOGLE_CREDENTIALS_JSON = os.getenv('GOOGLE_CREDENTIALS') 

SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/googleapis.com/auth/drive']
SELENIUM_TIMEOUT = 15
MAX_WORKERS = 3  # Adjust based on available system resources (RAM/CPU)
DRY_RUN = '--dry-run' in sys.argv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(threadName)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


# --- Helper Classes & Functions ---

class SeleniumDriver:
    """Context manager for Selenium WebDriver to ensure clean setup and teardown."""
    def __enter__(self):
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        # Add a user-agent to look less like a bot
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        # Suppress some selenium logs
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        self.driver = webdriver.Chrome(options=options)
        return self.driver

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.driver:
            self.driver.quit()

def get_gspread_client():
    """Authenticate and return gspread client using file or env var."""
    if GOOGLE_CREDENTIALS_JSON:
        creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPE)
    elif os.path.exists(CREDENTIALS_FILE):
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPE)
    else:
        if not DRY_RUN:
             raise ValueError(f"Credentials not found. Set GOOGLE_CREDENTIALS env var or place '{CREDENTIALS_FILE}' in working directory.")
        return None # Return None for dry run if no creds are available

    return gspread.authorize(creds)

def robust_request(url, timeout=20):
    """Wrapper for requests.get with optional tenacity retries if available."""
    if TENACITY_AVAILABLE:
        @retry(stop=stop_after_attempt(3), wait=wait_fixed(5), retry_if_exception_type=(requests.RequestException))
        def _make_request():
             response = requests.get(url, timeout=timeout)
             response.raise_for_status()
             return response
        return _make_request()
    else:
        # Simple fallback without retries
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        return response

def clean_df(df):
    """Standardizes, cleans, and dedupes the DataFrame."""
    if df.empty:
        return df
    
    # Ensure standard columns exist
    required_cols = ['Name', 'Date', 'County', 'Source', 'Details', 'Type']
    for col in required_cols:
        if col not in df.columns:
            df[col] = 'N/A'

    # Robust date parsing: handle various formats and invalid dates
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.strftime('%Y-%m-%d')
    df['Date'] = df['Date'].fillna('Unknown')

    # Clean text fields: remove extra spaces, handle NaNs
    for col in ['Name', 'County', 'Details', 'Type', 'Source']:
        df[col] = df[col].astype(str).str.strip().replace('nan', 'N/A')

    # Deduplicate based on key fields
    df = df.drop_duplicates(subset=['Name', 'Date', 'County'])
    
    # Final sort
    return df.sort_values('Date', ascending=False)

def append_to_sheet(df, sheet_name, client):
    """Uploads DataFrame to a specific Google Sheet tab."""
    if DRY_RUN or client is None:
        logger.info(f"[DRY RUN] Would upload {len(df)} rows to tab '{sheet_name}'")
        return

    try:
        spreadsheet = client.open_by_key(SHEET_ID)
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=len(df)+100, cols=len(df.columns))
            logger.info(f"Created new worksheet: {sheet_name}")
        
        worksheet.clear()
        if not df.empty:
            # gspread requires all data to be JSON serializable, so we cast to string just in case
            worksheet.update([df.columns.values.tolist()] + df.astype(str).values.tolist())
        logger.info(f"Uploaded {len(df)} rows to '{sheet_name}'")
    except Exception as e:
        logger.error(f"Failed to upload to '{sheet_name}': {e}")


# --- Individual Scrapers ---

def scrape_brevard():
    logger.info("Scraping Brevard...")
    data = []
    try:
        with SeleniumDriver() as driver:
            driver.get("https://www.brevardfl.gov/AnimalAbuseDatabaseSearch")
            search_box = WebDriverWait(driver, SELENIUM_TIMEOUT).until(EC.presence_of_element_located((By.NAME, "defendantName")))
            search_box.clear()
            search_box.send_keys(Keys.RETURN)
            
            # Wait for table to load after search
            WebDriverWait(driver, SELENIUM_TIMEOUT).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table tr")))
            # Slight delay to ensure full render
            time.sleep(2) 
            
            rows = driver.find_elements(By.TAG_NAME, "tr")[1:]
            for row in rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) >= 2:
                    data.append({
                        'Name': cols[0].text,
                        'Date': cols[1].text,
                        'County': 'Brevard',
                        'Source': 'Animal Abuse Database',
                        'Type': 'Convicted',
                        'Details': ' | '.join([c.text for c in cols[2:]])
                    })
    except Exception as e:
        logger.error(f"Brevard scrape failed: {e}")
    return pd.DataFrame(data)

def scrape_lee():
    logger.info("Scraping Lee (Enjoined & Registry)...")
    all_data = []
    
    # 1. Static Enjoined List
    try:
        response = robust_request("https://www.sheriffleefl.org/animal-abuser-registry-enjoined/")
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table')
        if table:
            for row in table.find_all('tr')[1:]:
                cols = [c.get_text(strip=True) for c in row.find_all('td')]
                if len(cols) >= 3:
                    all_data.append({
                        'Name': cols[0],
                        'Date': cols[2],
                        'County': 'Lee',
                        'Source': 'Enjoined List',
                        'Type': 'Enjoined',
                        'Details': f"Case: {cols[1]} | Condition: {cols[3] if len(cols) > 3 else ''}"
                    })
    except Exception as e:
         logger.error(f"Lee Enjoined scrape failed: {e}")

    # 2. Dynamic Registry
    try:
        with SeleniumDriver() as driver:
            driver.get("https://www.sheriffleefl.org/animal-abuser-search/")
            WebDriverWait(driver, SELENIUM_TIMEOUT).until(EC.presence_of_element_located((By.TAG_NAME, 'table')))
            rows = driver.find_elements(By.TAG_NAME, 'tr')[1:]
            for row in rows:
                cols = [c.text for c in row.find_elements(By.TAG_NAME, 'td')]
                if len(cols) >= 4:
                    all_data.append({
                        'Name': cols[0],
                        'Date': datetime.now().strftime('%Y-%m-%d'), # Date not explicitly in table, using scrape date
                        'County': 'Lee',
                        'Source': 'Convicted Registry',
                        'Type': 'Convicted',
                        'Details': f"DOB: {cols[1]} | Address: {cols[2]} | Charges: {cols[3]}"
                    })
    except Exception as e:
        logger.error(f"Lee Registry scrape failed: {e}")

    return pd.DataFrame(all_data)

def scrape_collier():
    logger.info("Scraping Collier...")
    data = []
    try:
        response = robust_request("https://www2.colliersheriff.org/animalabusesearch")
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table')
        if table:
            for row in table.find_all('tr')[1:]:
                cols = [c.get_text(strip=True) for c in row.find_all('td')]
                if len(cols) >= 6:
                    data.append({
                        'Name': cols[1],
                        # Use expiration date as a proxy for 'Date' if it's a date, else today
                        'Date': cols[5] if cols[5] != 'N/A' else datetime.now().strftime('%Y-%m-%d'),
                        'County': 'Collier',
                        'Source': 'Animal Abuse Search',
                        'Type': cols[0],
                        'Details': f"DOB: {cols[2]} | Address: {cols[3]} | Years: {cols[4]} | Charge: {cols[6] if len(cols) > 6 else ''}"
                    })
    except Exception as e:
        logger.error(f"Collier scrape failed: {e}")
    return pd.DataFrame(data)

def scrape_hillsborough():
    logger.info("Scraping Hillsborough...")
    data = []
    try:
        with SeleniumDriver() as driver:
            driver.get("https://hcfl.gov/residents/animals-and-pets/animal-abuser-registry")
            WebDriverWait(driver, SELENIUM_TIMEOUT).until(EC.presence_of_element_located((By.TAG_NAME, 'table')))
            for row in driver.find_elements(By.TAG_NAME, 'tr')[1:]:
                cols = [c.text for c in row.find_elements(By.TAG_NAME, "td")]
                if len(cols) >= 2:
                     data.append({
                        'Name': cols[0],
                        'Date': cols[1],
                        'County': 'Hillsborough',
                        'Source': 'Animal Abuser Registry',
                        'Type': 'Convicted',
                        'Details': ' | '.join(cols[2:])
                    })
    except Exception as e:
        logger.error(f"Hillsborough scrape failed: {e}")
    return pd.DataFrame(data)

def scrape_miami_dade():
    logger.info("Scraping Miami-Dade...")
    data = []
    try:
        with SeleniumDriver() as driver:
            driver.get("https://www.miamidade.gov/Apps/ASD/crueltyweb/")
            # Wait for at least one registry entry to appear
            WebDriverWait(driver, SELENIUM_TIMEOUT).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".registry-entry")))
            
            # Using a more generic approach if specific class names aren't perfectly reliable
            entries = driver.find_elements(By.CSS_SELECTOR, ".registry-entry")
            for entry in entries:
                text = entry.text
                # Simple parsing based on expected text structure; might need refinement based on actual HTML
                lines = text.split('\n')
                if len(lines) >= 1:
                    # Assuming first line is name, second might be date or other info
                    data.append({
                        'Name': lines[0],
                         # Try to find a date-like string in the text, else use today
                        'Date': next((line for line in lines if re.search(r'\d{1,2}/\d{1,2}/\d{2,4}', line)), datetime.now().strftime('%Y-%m-%d')),
                        'County': 'Miami-Dade',
                        'Source': 'Animal Abuser Registry',
                        'Type': 'Convicted',
                        'Details': ' | '.join(lines[1:])
                    })
    except Exception as e:
        logger.error(f"Miami-Dade scrape failed: {e}")
    return pd.DataFrame(data)

def scrape_marion():
    logger.info("Scraping Marion...")
    data = []
    try:
        response = robust_request("https://animalservices.marionfl.org/animal-control/animal-control-and-pet-laws/animal-abuser-registry")
        soup = BeautifulSoup(response.content, 'html.parser')
        # Assuming 'registry-entry' class exists based on original code, might need adjustment
        for entry in soup.find_all('div', class_='registry-entry'):
            text = entry.get_text(separator='\n')
            name_match = re.search(r'Name:\s*(.+)', text)
            date_match = re.search(r'Conviction Date:\s*(.+)', text)
            
            if name_match:
                 data.append({
                    'Name': name_match.group(1).strip(),
                    'Date': date_match.group(1).strip() if date_match else datetime.now().strftime('%Y-%m-%d'),
                    'County': 'Marion',
                    'Source': 'Animal Abuser Registry',
                    'Type': 'Convicted',
                    'Details': text.replace('\n', ' | ').strip()
                })
    except Exception as e:
        logger.error(f"Marion scrape failed: {e}")
    return pd.DataFrame(data)

def scrape_pasco():
    logger.info("Scraping Pasco...")
    data = []
    try:
        with SeleniumDriver() as driver:
            driver.get("https://www.pascoclerk.com/153/Animal-Abuser-Search")
            continue_link = WebDriverWait(driver, SELENIUM_TIMEOUT).until(EC.element_to_be_clickable((By.LINK_TEXT, "Continue to Search")))
            continue_link.click()
            
            WebDriverWait(driver, SELENIUM_TIMEOUT).until(EC.presence_of_element_located((By.TAG_NAME, 'table')))
            # Slight delay for table population
            time.sleep(2)
            
            for row in driver.find_elements(By.CSS_SELECTOR, 'table tr')[1:]:
                cols = [c.text for c in row.find_elements(By.TAG_NAME, 'td')]
                if len(cols) >= 2:
                    data.append({
                        'Name': cols[0],
                        'Date': cols[1],
                        'County': 'Pasco',
                        'Source': 'Animal Abuser Search',
                        'Type': 'Convicted',
                        'Details': ' | '.join(cols[2:])
                    })
    except Exception as e:
         logger.error(f"Pasco scrape failed: {e}")
    return pd.DataFrame(data)

def scrape_seminole():
    logger.info("Scraping Seminole...")
    data = []
    try:
        with SeleniumDriver() as driver:
            driver.get("https://www.seminolecountyfl.gov/departments-services/prepare-seminole/animal-services/animal-abuse-registry")
            WebDriverWait(driver, SELENIUM_TIMEOUT).until(EC.presence_of_element_located((By.TAG_NAME, 'table')))
            for row in driver.find_elements(By.CSS_SELECTOR, 'table tr')[1:]:
                cols = [c.text for c in row.find_elements(By.TAG_NAME, 'td')]
                if len(cols) >= 2:
                     data.append({
                        'Name': cols[0],
                        'Date': cols[1],
                        'County': 'Seminole',
                        'Source': 'Animal Abuse Registry',
                        'Type': 'Convicted',
                        'Details': ' | '.join(cols[2:])
                    })
    except Exception as e:
        logger.error(f"Seminole scrape failed: {e}")
    return pd.DataFrame(data)

def scrape_volusia():
    logger.info("Scraping Volusia...")
    data = []
    pdf_url = "https://vcservices.vcgov.org/AnimalControlAttachments/VolusiaAnimalAbuse.pdf"
    try:
        # stream=True is better for potentially large files
        response = requests.get(pdf_url, stream=True, timeout=30)
        response.raise_for_status()
        
        # Saving to a temporary file might be more reliable for pdfplumber than raw stream sometimes
        # but sticking to in-memory for now as it usually works for smaller PDFs
        with pdfplumber.open(response.raw) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text: continue
                for line in text.split('\n'):
                    # Basic heuristic: assume lines starting with a name-like pattern are entries
                    if re.match(r'^[A-Z][a-z]+,?\s+[A-Z]', line):
                        parts = [p.strip() for p in re.split(r'\s{2,}|,', line) if p.strip()]
                        if len(parts) >= 2:
                             data.append({
                                'Name': parts[0],
                                # Try to find a date in the remaining parts
                                'Date': next((p for p in parts[1:] if re.search(r'\d{1,2}/\d{1,2}/\d{2,4}', p)), 'Unknown'),
                                'County': 'Volusia',
                                'Source': 'PDF Database',
                                'Type': 'Convicted',
                                'Details': ' | '.join(parts[1:])
                            })
    except Exception as e:
        logger.error(f"Volusia scrape failed: {e}")
    return pd.DataFrame(data)

# --- Stubs for counties without public automated registries ---
def scrape_stub(county_name, message):
    logger.info(f"Skipping {county_name}: {message}")
    return pd.DataFrame()


# --- Main Orchestration ---

def main():
    start_time = time.time()
    logger.info(f"Starting DNAFL scraper job at {datetime.utcnow().isoformat()} UTC")
    if DRY_RUN:
        logger.warning("--- DRY RUN MODE: No data will be uploaded to Google Sheets ---")

    # Authenticate once at the start
    try:
        gspread_client = get_gspread_client()
    except Exception as e:
        logger.critical(f"Authentication failed: {e}")
        sys.exit(1)

    # List of all scraper functions to run
    scrapers = [
        scrape_brevard,
        scrape_lee,
        scrape_collier,
        scrape_hillsborough,
        scrape_miami_dade,
        scrape_marion,
        scrape_pasco,
        scrape_seminole,
        scrape_volusia,
        # Stubs
        lambda: scrape_stub('Pinellas', 'No public registry'),
        lambda: scrape_stub('Broward', 'Internal list only'),
        lambda: scrape_stub('Orange', 'Manual checks required'),
        # ... add other stubs as needed
    ]

    results = []
    # Run scrapers concurrently to save time
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Map future to scraper name for logging
        future_to_name = {executor.submit(s): s.__name__ if hasattr(s, '__name__') else 'Anonymous Lambda' for s in scrapers}
        
        for future in as_completed(future_to_name):
            name = future_to_name[future]
            try:
                df = future.result()
                if not df.empty:
                    results.append(df)
                    logger.info(f"[{name}] Found {len(df)} records.")
                else:
                     logger.info(f"[{name}] No records found.")
            except Exception as e:
                logger.error(f"[{name}] crashed: {e}", exc_info=True)

    if results:
        # Combine all individual county dataframes into one master list
        master_df = pd.concat(results, ignore_index=True)
        master_df = clean_df(master_df)
        
        logger.info(f"Total unique records after cleanup: {len(master_df)}")
        
        # Upload Master List to Google Sheet
        append_to_sheet(master_df, 'DNAFL_Master', gspread_client)
        
        # Optional: Save a local CSV backup (useful for debugging or CI artifacts)
        csv_filename = f'dnafl_backup_{datetime.now().strftime("%Y%m%d")}.csv'
        master_df.to_csv(csv_filename, index=False)
        logger.info(f"Local backup saved to {csv_filename}")

    else:
        logger.error("All scrapers failed to return data. Nothing to upload.")
        if not DRY_RUN:
             sys.exit(1) # Exit with error in CI if totally failed

    elapsed = time.time() - start_time
    logger.info(f"Job finished successfully in {elapsed:.2f} seconds.")

if __name__ == "__main__":
    main()