#!/usr/bin/env python3
"""
DNAFL Scraper v3.0 (Targeted Sources)
Aggregates Florida animal abuser registries using specific user-provided endpoints.
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

# Try importing tenacity for retries
try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False
    def retry(*args, **kwargs): return lambda f: f
    stop_after_attempt = wait_exponential = retry_if_exception_type = None

# --- CONFIGURATION ---
SHEET_ID = os.getenv('SHEET_ID', '1V0ERkUXzc2G_SvSVUaVac50KyNOpw4N7bL6yAiZospY')
MASTER_TAB_NAME = "Master_Registry"
CREDENTIALS_FILE = 'credentials.json'
GOOGLE_CREDENTIALS_ENV = os.getenv('GOOGLE_CREDENTIALS')
WEBHOOK_URL = os.getenv('ALERT_WEBHOOK_URL') 

SELENIUM_TIMEOUT = 30
MAX_WORKERS = 3
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
        # Set strict page load timeout to fail fast on hung pages
        self.driver.set_page_load_timeout(60)
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
    if df.empty: return df
    for col in ['Name', 'Date', 'County', 'Source', 'Details', 'Type']:
        if col not in df.columns: df[col] = 'N/A'
    
    # Clean strings
    for col in df.columns:
        if df[col].dtype == 'object': 
            df[col] = df[col].fillna('N/A').astype(str).str.strip().str.replace(r'\s+', ' ', regex=True)

    # Robust Date Parsing
    df['Date_Parsed'] = pd.to_datetime(df['Date'], format='%m/%d/%Y', errors='coerce')
    df['Date_Parsed'] = df['Date_Parsed'].fillna(pd.to_datetime(df['Date'], format='%Y-%m-%d', errors='coerce'))
    df['Date_Parsed'] = df['Date_Parsed'].fillna(pd.to_datetime(df['Date'], errors='coerce'))
    df['Date'] = df['Date_Parsed'].dt.strftime('%Y-%m-%d').fillna('Unknown')
    
    return df.drop(columns=['Date_Parsed']).sort_values('Date', ascending=False).drop_duplicates(subset=['Name', 'County', 'Date'])

# --- SCRAPERS ---

def scrape_lee():
    data = []
    # 1. Enjoined List
    try:
        resp = fetch_url("https://www.sheriffleefl.org/animal-abuser-registry-enjoined/")
        soup = BeautifulSoup(resp.content, 'html.parser')
        for row in soup.select('table tr')[1:]:
            cols = [c.get_text(strip=True) for c in row.find_all('td')]
            if len(cols) >= 3:
                data.append({'Name': cols[0], 'Date': cols[2], 'County': 'Lee', 'Source': 'Lee Enjoined', 'Type': 'Enjoined', 'Details': f"Case: {cols[1]}"})
    except Exception as e: alert_failure(f"Lee Enjoined failed: {str(e)[:200]}")

    # 2. Other Registry (ccsheriff.org might be Charlotte, but treating as listed under Lee for now)
    try:
        # This looks like a dynamic search page, might need Selenium if direct request fails
        with SeleniumDriver() as driver:
             driver.get("https://animalabuserregistry.ccsheriff.org/")
             # Wait for generic table data
             WebDriverWait(driver, SELENIUM_TIMEOUT).until(EC.presence_of_element_located((By.TAG_NAME, "td")))
             for row in driver.find_elements(By.TAG_NAME, "tr"):
                 cols = [td.text for td in row.find_elements(By.TAG_NAME, "td")]
                 if len(cols) >= 3:
                      # Heuristic mapping based on typical registry table layout
                      data.append({'Name': cols[0], 'Date': 'Unknown', 'County': 'Lee/Charlotte', 'Source': 'CCSO Registry', 'Type': 'Convicted', 'Details': ' | '.join(cols[1:])})
    except Exception as e: alert_failure(f"CC Sheriff Registry failed: {str(e)[:200]}")
    return pd.DataFrame(data)

def scrape_marion():
    data = []
    urls = [
        ("https://animalservices.marionfl.org/animal-control/animal-control-and-pet-laws/animal-abuser-registry", "Convicted"),
        ("https://animalservices.marionfl.org/animal-control/animal-control-and-pet-laws/civil-enjoinment-list", "Enjoined")
    ]
    for url, rtype in urls:
        try:
            resp = fetch_url(url)
            soup = BeautifulSoup(resp.content, 'html.parser')
            # Marion uses unstructured text mostly
            for entry in soup.find_all(['p', 'li'], string=re.compile(r'Name:', re.I)):
                text = entry.get_text(separator=' | ').strip()
                name = re.search(r'Name:\s*([^|]+)', text, re.I)
                date = re.search(r'(Conviction|Enjoinment) Date:\s*([^|]+)', text, re.I)
                if name:
                     data.append({'Name': name.group(1).strip(), 'Date': date.group(2).strip() if date else 'Unknown', 'County': 'Marion', 'Source': f"Marion {rtype}", 'Type': rtype, 'Details': text})
        except Exception as e: alert_failure(f"Marion {rtype} failed: {str(e)[:200]}")
    return pd.DataFrame(data)

def scrape_hillsborough():
    data = []
    # 1. Enjoined PDF
    try:
        lines = extract_text_from_pdf("https://assets.contentstack.io/v3/assets/blteea73b27b731f985/bltc47cc1e37ac0e54a/Enjoinment%20List.pdf")
        for line in lines:
             # Basic name detection heuristic (Last, First Middle)
            if re.match(r'^[A-Z]+,\s+[A-Z]+', line):
                parts = [p.strip() for p in line.split('   ') if p.strip()]
                if len(parts) >= 1:
                    data.append({'Name': parts[0], 'Date': 'Unknown', 'County': 'Hillsborough', 'Source': 'Hillsborough Enjoined PDF', 'Type': 'Enjoined', 'Details': line})
    except Exception as e: alert_failure(f"Hillsborough PDF failed: {str(e)[:200]}")

    # 2. General Registry (Selenium fallback for dynamic search)
    try:
        with SeleniumDriver() as driver:
            driver.get("https://hcfl.gov/residents/animals-and-pets/animal-abuser-registry/search-the-registry")
            WebDriverWait(driver, SELENIUM_TIMEOUT).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            for row in driver.find_elements(By.CSS_SELECTOR, "table tr")[1:]:
                cols = [c.text for c in row.find_elements(By.TAG_NAME, "td")]
                if len(cols) >= 2:
                    data.append({'Name': cols[0], 'Date': cols[1], 'County': 'Hillsborough', 'Source': 'Hillsborough Registry', 'Type': 'Convicted', 'Details': ' | '.join(cols[2:])})
    except Exception as e: alert_failure(f"Hillsborough Search failed: {str(e)[:200]}")
    return pd.DataFrame(data)

def scrape_volusia():
    data = []
    try:
        lines = extract_text_from_pdf("https://vcservices.vcgov.org/AnimalControlAttachments/VolusiaAnimalAbuse.pdf")
        for line in lines:
             if re.match(r'^[A-Z][a-zA-Z]+,\s*[A-Z]', line):
                date_match = re.search(r'\d{1,2}/\d{1,2}/\d{2,4}', line)
                data.append({'Name': line.split('  ')[0], 'Date': date_match.group(0) if date_match else 'Unknown', 'County': 'Volusia', 'Source': 'Volusia PDF', 'Type': 'Convicted', 'Details': line})
    except Exception as e: alert_failure(f"Volusia PDF failed: {str(e)[:200]}")
    return pd.DataFrame(data)

def scrape_seminole():
    data = []
    # Prefer PDF for Seminole as it's often more complete than the web search
    try:
        lines = extract_text_from_pdf("https://scwebapp2.seminolecountyfl.gov:6443/AnimalCruelty/AnimalCrueltyReporty.pdf")
        for line in lines:
            if re.search(r'\d{1,2}/\d{1,2}/\d{4}', line) and not line.startswith('Run Date'):
                 # Extremely rough heuristic, Seminole PDF is unstructured
                 parts = line.split()
                 if len(parts) > 3:
                     data.append({'Name': f"{parts[0]} {parts[1]}", 'Date': parts[-1] if '/' in parts[-1] else 'Unknown', 'County': 'Seminole', 'Source': 'Seminole PDF', 'Type': 'Convicted', 'Details': line})
    except Exception as e: alert_failure(f"Seminole PDF failed: {str(e)[:200]}")
    return pd.DataFrame(data)

def scrape_pasco():
    data = []
    try:
        with SeleniumDriver() as driver:
            driver.get("https://app.pascoclerk.com/animalabusersearch/")
            # Pasco's new app might need a "Search" button click even if empty
            try: WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button[type='submit'], .btn-search"))).click()
            except: pass
            WebDriverWait(driver, SELENIUM_TIMEOUT).until(EC.presence_of_element_located((By.TAG_NAME, "tr")))
            for row in driver.find_elements(By.CSS_SELECTOR, "table tbody tr"):
                cols = [c.text for c in row.find_elements(By.TAG_NAME, "td")]
                if len(cols) >= 3:
                     data.append({'Name': cols[0], 'Date': cols[2], 'County': 'Pasco', 'Source': 'Pasco Clerk App', 'Type': 'Convicted', 'Details': f"Case: {cols[1]}"})
    except Exception as e: alert_failure(f"Pasco App failed: {str(e)[:200]}")
    return pd.DataFrame(data)

def scrape_collier():
    data = []
    try:
        resp = fetch_url("https://www2.colliersheriff.org/animalabusesearch", verify=False)
        soup = BeautifulSoup(resp.content, 'html.parser')
        for row in soup.select('table tr')[1:]:
            cols = [c.get_text(strip=True) for c in row.find_all('td')]
            if len(cols) >= 6:
                data.append({'Name': cols[1], 'Date': cols[5] if cols[5] not in ['N/A',''] else datetime.now().strftime('%Y-%m-%d'), 'County': 'Collier', 'Source': 'Collier Sheriff', 'Type': cols[0], 'Details': f"DOB: {cols[2]} | Case: {cols[4]}"})
    except Exception as e: alert_failure(f"Collier failed: {str(e)[:200]}")
    return pd.DataFrame(data)

def scrape_osceola():
    data = []
    try:
        lines = extract_text_from_pdf("https://courts.osceolaclerk.com/reports/AnimalCrueltyReportWeb.pdf")
        for line in lines:
            # Look for case number patterns common in Osceola
            if re.search(r'\d{4}-\w{2}-\d+', line):
                parts = [p for p in line.split('  ') if p]
                if len(parts) >= 2:
                     data.append({'Name': parts[0], 'Date': 'Unknown', 'County': 'Osceola', 'Source': 'Osceola Clerk PDF', 'Type': 'Convicted', 'Details': line})
    except Exception as e: alert_failure(f"Osceola PDF failed: {str(e)[:200]}")
    return pd.DataFrame(data)

# --- ORCHESTRATOR ---

def main():
    start_ts = time.time()
    logger.info("Starting DNAFL Scraper Job v3.0...")
    gc = get_gspread_client()
    if not gc and not DRY_RUN: logger.critical("Credentials missing. Aborting."); sys.exit(1)

    tasks = [scrape_lee, scrape_marion, scrape_hillsborough, scrape_volusia, 
             scrape_seminole, scrape_pasco, scrape_collier, scrape_osceola]
    
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {executor.submit(task): task.__name__ for task in tasks}
        for future in as_completed(future_map):
            try:
                df = future.result()
                if not df.empty:
                    logger.info(f"[{future_map[future]}] Success: {len(df)} records.")
                    results.append(df)
                else:
                     logger.warning(f"[{future_map[future]}] yielded 0 records.")
            except Exception as e: alert_failure(f"CRITICAL: {future_map[future]} crashed: {e}")

    if results:
        master_df = standardize_data(pd.concat(results, ignore_index=True))
        logger.info(f"TOTAL UNIQUE RECORDS: {len(master_df)}")
        if not DRY_RUN and gc:
            try:
                sh = gc.open_by_key(SHEET_ID)
                try: wks = sh.worksheet(MASTER_TAB_NAME)
                except gspread.WorksheetNotFound: wks = sh.add_worksheet(MASTER_TAB_NAME, 1, 1)
                wks.clear()
                wks.update([master_df.columns.tolist()] + master_df.astype(str).values.tolist())
                logger.info("Upload to Google Sheets complete.")
            except Exception as e: alert_failure(f"Upload Failed: {e}")
        else:
             master_df.to_csv("dry_run_master.csv", index=False)
             logger.info("Dry run complete, saved to CSV.")
    else:
        alert_failure("Global Failure: No data scraped.")
        if not DRY_RUN: sys.exit(1)

    logger.info(f"Job finished in {time.time() - start_ts:.1f}s")

if __name__ == '__main__':
    main()


