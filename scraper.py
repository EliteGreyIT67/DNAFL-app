#!/usr/bin/env python3
"""
DNAFL Scraper v2.1 (Complete Automation)
Fetches, cleans, and aggregates Florida animal abuser registries.
Features: Concurrency, Retries, standardized cleaning, and alert hooks.
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
from selenium.webdriver.common.keys import Keys

# Try importing tenacity for retries, gracefully degrade if missing
try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False
    def retry(*args, **kwargs): return lambda f: f
    stop_after_attempt = wait_exponential = retry_if_exception_type = None

# --- CONFIGURATION ---
SHEET_ID = os.getenv('SHEET_ID', '1V0ERkUXzc2G_SvSVUaVac50KyNOpw4N7bL6yAiZospY')
# The tab name in your Google Sheet where all data will be aggregated automatically
MASTER_TAB_NAME = "DNA List" 
CREDENTIALS_FILE = 'credentials.json'
GOOGLE_CREDENTIALS_ENV = os.getenv('GOOGLE_CREDENTIALS')
WEBHOOK_URL = os.getenv('ALERT_WEBHOOK_URL') 

SELENIUM_TIMEOUT = 25
MAX_WORKERS = 3 # Lowered slightly to prevent resource choking with many selenium instances
DRY_RUN = '--dry-run' in sys.argv

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(threadName)s: %(message)s')
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
        opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36")
        self.driver = webdriver.Chrome(options=opts)
        self.driver.set_page_load_timeout(90)
        return self.driver
    def __exit__(self, *_):
        if hasattr(self, 'driver'): self.driver.quit()

def alert_failure(message):
    logger.error(message)
    if WEBHOOK_URL and not DRY_RUN:
        try: requests.post(WEBHOOK_URL, json={'text': f"🚨 **DNAFL Scraper Alert** 🚨\n{message}"}, timeout=5)
        except Exception: pass

def get_gspread_client():
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    if GOOGLE_CREDENTIALS_ENV: creds = Credentials.from_service_account_info(json.loads(GOOGLE_CREDENTIALS_ENV), scopes=scopes)
    elif os.path.exists(CREDENTIALS_FILE): creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
    else: return None
    return gspread.authorize(creds)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_url(url, stream=False, verify=True):
    # Some county sites have bad SSL certs, might need verify=False occasionally
    resp = requests.get(url, timeout=30, stream=stream, verify=verify)
    resp.raise_for_status()
    return resp

def standardize_data(df):
    if df.empty: return df
    for col in ['Name', 'Date', 'County', 'Source', 'Details', 'Type']:
        if col not in df.columns: df[col] = 'N/A'
    
    # Clean strings
    for col in df.columns:
        if df[col].dtype == 'object': 
            df[col] = df[col].fillna('N/A').astype(str).str.strip().str.replace(r'\s+', ' ', regex=True)

    # Robust Date Parsing
    # Try multiple formats before falling back to 'Unknown'
    df['Date_Parsed'] = pd.to_datetime(df['Date'], format='%m/%d/%Y', errors='coerce')
    df['Date_Parsed'] = df['Date_Parsed'].fillna(pd.to_datetime(df['Date'], format='%Y-%m-%d', errors='coerce'))
    df['Date_Parsed'] = df['Date_Parsed'].fillna(pd.to_datetime(df['Date'], errors='coerce'))
    
    df['Date'] = df['Date_Parsed'].dt.strftime('%Y-%m-%d').fillna('Unknown')
    df = df.drop(columns=['Date_Parsed'])

    # Deduplicate favoring newest entries if names match
    return df.sort_values('Date', ascending=False).drop_duplicates(subset=['Name', 'County', 'Date'])

# --- INDIVIDUAL SCRAPERS ---

def scrape_brevard():
    data = []
    try:
        with SeleniumDriver() as driver:
            driver.get("https://www.brevardfl.gov/AnimalAbuseDatabaseSearch")
            WebDriverWait(driver, SELENIUM_TIMEOUT).until(EC.element_to_be_clickable((By.NAME, "defendantName"))).send_keys(Keys.RETURN)
            WebDriverWait(driver, SELENIUM_TIMEOUT).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr")))
            for row in driver.find_elements(By.CSS_SELECTOR, "table tbody tr"):
                cols = [td.text for td in row.find_elements(By.TAG_NAME, "td")]
                if len(cols) >= 5:
                    data.append({'Name': f"{cols[1]} {cols[0]}", 'Date': cols[2], 'County': 'Brevard', 'Source': 'Brevard County', 'Type': 'Convicted', 'Details': f"DOB: {cols[3]} | Case: {cols[4]}"})
    except Exception as e: alert_failure(f"Brevard failed: {str(e)[:200]}")
    return pd.DataFrame(data)

def scrape_lee():
    data = []
    # 1. Static Enjoined
    try:
        resp = fetch_url("https://www.sheriffleefl.org/animal-abuser-registry-enjoined/")
        soup = BeautifulSoup(resp.content, 'html.parser')
        table = soup.find('table')
        if table:
            for row in table.find_all('tr')[1:]:
                cols = [c.get_text(strip=True) for c in row.find_all('td')]
                if len(cols) >= 3:
                    data.append({'Name': cols[0], 'Date': cols[2], 'County': 'Lee', 'Source': 'Lee Sheriff Enjoined', 'Type': 'Enjoined', 'Details': f"Case: {cols[1]} | {cols[3] if len(cols)>3 else ''}"})
    except Exception as e: alert_failure(f"Lee Enjoined failed: {str(e)[:200]}")

    # 2. Dynamic Registry
    try:
        with SeleniumDriver() as driver:
            driver.get("https://www.sheriffleefl.org/animal-abuser-search/")
            WebDriverWait(driver, SELENIUM_TIMEOUT).until(EC.presence_of_element_located((By.TAG_NAME, 'table')))
            for row in driver.find_elements(By.TAG_NAME, 'tr')[1:]:
                cols = [c.text for c in row.find_elements(By.TAG_NAME, 'td')]
                if len(cols) >= 4:
                    # Lee doesn't list conviction date in this table, use today's date as "Date Added" proxy if needed, or mark Unknown
                    data.append({'Name': cols[0], 'Date': datetime.now().strftime('%Y-%m-%d'), 'County': 'Lee', 'Source': 'Lee Sheriff Registry', 'Type': 'Convicted', 'Details': f"DOB: {cols[1]} | Addr: {cols[2]} | Charge: {cols[3]}"})
    except Exception as e: alert_failure(f"Lee Registry failed: {str(e)[:200]}")
    return pd.DataFrame(data)

def scrape_collier():
    data = []
    try:
        resp = fetch_url("https://www2.colliersheriff.org/animalabusesearch", verify=False) # Collier sometimes has SSL issues
        soup = BeautifulSoup(resp.content, 'html.parser')
        table = soup.find('table')
        if table:
             for row in table.find_all('tr')[1:]:
                cols = [c.get_text(strip=True) for c in row.find_all('td')]
                if len(cols) >= 7:
                    # Using Expiration Date (col 5) as proxy for date if registration date isn't clear
                    date_val = cols[5] if cols[5] not in ['N/A', ''] else datetime.now().strftime('%Y-%m-%d')
                    data.append({'Name': cols[1], 'Date': date_val, 'County': 'Collier', 'Source': 'Collier Sheriff', 'Type': cols[0], 'Details': f"DOB: {cols[2]} | Case: {cols[4]} | Charge: {cols[6]}"})
    except Exception as e: alert_failure(f"Collier failed: {str(e)[:200]}")
    return pd.DataFrame(data)

def scrape_hillsborough():
    data = []
    try:
        with SeleniumDriver() as driver:
            driver.get("https://hcfl.gov/residents/animals-and-pets/animal-abuser-registry")
            WebDriverWait(driver, SELENIUM_TIMEOUT).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            for row in driver.find_elements(By.CSS_SELECTOR, "table tr")[1:]:
                cols = [td.text for td in row.find_elements(By.TAG_NAME, "td")]
                if len(cols) >= 2:
                    data.append({'Name': cols[0], 'Date': cols[1], 'County': 'Hillsborough', 'Source': 'Hillsborough County', 'Type': 'Convicted', 'Details': ' | '.join(cols[2:])})
    except Exception as e: alert_failure(f"Hillsborough failed: {str(e)[:200]}")
    return pd.DataFrame(data)

def scrape_miami_dade():
    data = []
    try:
        with SeleniumDriver() as driver:
            driver.get("https://www.miamidade.gov/Apps/ASD/crueltyweb/")
            # Wait for the dynamic list to load. Using a generic wait for body content changing might be safer if classes change.
            WebDriverWait(driver, SELENIUM_TIMEOUT).until(EC.presence_of_element_located((By.TAG_NAME, "tbody"))) 
            # Assuming standard table layout for Miami Dade based on typical gov sites, adapt selector if needed
            rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
            for row in rows:
                 cols = [td.text for td in row.find_elements(By.TAG_NAME, "td")]
                 if len(cols) >= 3:
                     data.append({'Name': cols[0], 'Date': cols[2] if len(cols) > 2 else 'Unknown', 'County': 'Miami-Dade', 'Source': 'Miami-Dade ASD', 'Type': 'Convicted', 'Details': cols[1] if len(cols) > 1 else 'N/A'})
    except Exception as e: alert_failure(f"Miami-Dade failed: {str(e)[:200]}")
    return pd.DataFrame(data)

def scrape_marion():
    data = []
    try:
        resp = fetch_url("https://animalservices.marionfl.org/animal-control/animal-control-and-pet-laws/animal-abuser-registry")
        soup = BeautifulSoup(resp.content, 'html.parser')
        # Marion often uses unstructured text blocks. Looking for patterns.
        for entry in soup.find_all(['p', 'div'], string=re.compile(r'Name:', re.I)):
            text = entry.get_text(separator=' | ').strip()
            name_match = re.search(r'Name:\s*([^|]+)', text, re.I)
            date_match = re.search(r'Conviction Date:\s*([^|]+)', text, re.I)
            if name_match:
                data.append({'Name': name_match.group(1).strip(), 'Date': date_match.group(1).strip() if date_match else 'Unknown', 'County': 'Marion', 'Source': 'Marion Animal Services', 'Type': 'Convicted', 'Details': text})
    except Exception as e: alert_failure(f"Marion failed: {str(e)[:200]}")
    return pd.DataFrame(data)

def scrape_pasco():
    data = []
    try:
        with SeleniumDriver() as driver:
            driver.get("https://www.pascoclerk.com/153/Animal-Abuser-Search")
            # Click the disclaimer if it exists
            try:
                WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, "Continue"))).click()
            except: pass # Maybe no disclaimer today
            
            WebDriverWait(driver, SELENIUM_TIMEOUT).until(EC.presence_of_element_located((By.TAG_NAME, 'table')))
            for row in driver.find_elements(By.CSS_SELECTOR, 'table tr')[1:]:
                cols = [c.text for c in row.find_elements(By.TAG_NAME, 'td')]
                if len(cols) >= 3:
                    data.append({'Name': cols[0], 'Date': cols[2], 'County': 'Pasco', 'Source': 'Pasco Clerk', 'Type': 'Convicted', 'Details': f"Case: {cols[1]}"})
    except Exception as e: alert_failure(f"Pasco failed: {str(e)[:200]}")
    return pd.DataFrame(data)

def scrape_seminole():
    data = []
    try:
        with SeleniumDriver() as driver:
            driver.get("https://www.seminolecountyfl.gov/departments-services/prepare-seminole/animal-services/animal-abuse-registry")
            WebDriverWait(driver, SELENIUM_TIMEOUT).until(EC.presence_of_element_located((By.TAG_NAME, 'table')))
            for row in driver.find_elements(By.CSS_SELECTOR, 'table tr')[1:]:
                cols = [c.text for c in row.find_elements(By.TAG_NAME, 'td')]
                if len(cols) >= 2:
                     data.append({'Name': cols[0], 'Date': cols[1], 'County': 'Seminole', 'Source': 'Seminole County', 'Type': 'Convicted', 'Details': ' | '.join(cols[2:])})
    except Exception as e: alert_failure(f"Seminole failed: {str(e)[:200]}")
    return pd.DataFrame(data)

def scrape_volusia_pdf():
    data = []
    try:
        resp = fetch_url("https://vcservices.vcgov.org/AnimalControlAttachments/VolusiaAnimalAbuse.pdf", stream=True)
        with pdfplumber.open(resp.raw) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text: continue
                for line in text.split('\n'):
                    # Heuristic: Names often start with uppercase at start of line, followed by date-like string later
                    if re.match(r'^[A-Z][a-zA-Z]+,\s*[A-Z]', line):
                        date_match = re.search(r'\d{1,2}/\d{1,2}/\d{2,4}', line)
                        data.append({'Name': line.split(',')[0] + ", " + line.split(',')[1].split(' ')[0], # Rough name grab
                                     'Date': date_match.group(0) if date_match else 'Unknown', 
                                     'County': 'Volusia', 'Source': 'Volusia PDF', 'Type': 'Convicted', 'Details': line})
    except Exception as e: alert_failure(f"Volusia PDF failed: {str(e)[:200]}")
    return pd.DataFrame(data)

# --- MAIN ORCHESTRATOR ---

def main():
    start_ts = time.time()
    logger.info("Starting DNAFL Scraper Job...")
    
    # 1. Auth
    gc = get_gspread_client()
    if not gc and not DRY_RUN: logger.critical("No Credentials. Aborting."); sys.exit(1)

    # 2. Define Tasks
    tasks = [
        scrape_brevard, scrape_lee, scrape_collier, scrape_hillsborough,
        scrape_miami_dade, scrape_marion, scrape_pasco, scrape_seminole,
        scrape_volusia_pdf
    ]

    # 3. Execute
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {executor.submit(task): task.__name__ for task in tasks}
        for future in as_completed(future_map):
            task_name = future_map[future]
            try:
                df = future.result()
                if not df.empty:
                    logger.info(f"[{task_name}] Success: {len(df)} records.")
                    results.append(df)
                else:
                    logger.warning(f"[{task_name}] yielded 0 records.")
            except Exception as e:
                alert_failure(f"CRITICAL: [{task_name}] crashed: {e}")

    # 4. Aggregate & Automate Upload to Main Sheet
    if results:
        master_df = standardize_data(pd.concat(results, ignore_index=True))
        logger.info(f"TOTAL UNIQUE RECORDS TO AUTOMATE: {len(master_df)}")

        if not DRY_RUN and gc:
            try:
                # Open the main spreadsheet
                sh = gc.open_by_key(SHEET_ID)
                # Select or create the Master Tab for automated data
                try: wks = sh.worksheet(MASTER_TAB_NAME)
                except gspread.WorksheetNotFound: 
                    logger.info(f"Tab '{MASTER_TAB_NAME}' not found, creating it.")
                    wks = sh.add_worksheet(MASTER_TAB_NAME, 1, 1)
                
                # Clear and FULLY replace data in the main tab
                wks.clear()
                wks.update([master_df.columns.tolist()] + master_df.astype(str).values.tolist())
                logger.info(f"SUCCESS: Automated extraction to '{MASTER_TAB_NAME}' complete.")
            except Exception as e:
                alert_failure(f"Automated Google Sheet Upload Failed: {e}")
        else:
            logger.info("[DRY RUN] Skipping upload to main sheet.")
            master_df.to_csv("automated_extract_dry_run.csv", index=False)
    else:
        alert_failure("Global Failure: No data scraped from any source to automate.")
        if not DRY_RUN: sys.exit(1)

    logger.info(f"Job finished in {time.time() - start_ts:.1f}s")

if __name__ == '__main__':
    main()


