#!/usr/bin/env python3
"""
DNAFL Scraper: Fetches Florida animal abuser registries from 15+ sources, dedupes, and uploads to Google Sheets.
Runs daily via GitHub Actions at 2 AM UTC.
Tech: Python 3.10+, gspread, pandas, Selenium/BS4/pdfplumber.
License: MIT.
"""

import os
import sys
import logging
from datetime import datetime
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from bs4 import BeautifulSoup
import requests
import pdfplumber
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
import time
import re

# Config: Hard-coded for direct runs; override with env if needed
SHEET_ID = '1V0ERkUXzc2G_SvSVUaVac50KyNOpw4N7bL6yAiZospY'  # Your master Google Sheet ID
CREDENTIALS_FILE = 'credentials.json'  # Path to your service account JSON key
SCOPE = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']  # API scopes for read/write
SELENIUM_TIMEOUT = 10  # Seconds to wait for elements (adjust for slow sites)
DRY_RUN = '--dry-run' in sys.argv  # CI flag: Skip writes

# Setup logging: Outputs to console with timestamps
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def auth_gspread():
    """Authenticate and return gspread client."""
    if not os.path.exists(CREDENTIALS_FILE):
        raise ValueError(f"Credentials file '{CREDENTIALS_FILE}' not found. Ensure it's in the working directory.")
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPE)
    client = gspread.authorize(creds)
    return client

def append_to_sheet(df, sheet_name, client):
    """Append DF to Sheet tab; create if missing. Skip in dry-run."""
    if DRY_RUN:
        logger.info(f"DRY RUN: Would append {len(df)} rows to {sheet_name}")
        return None
    try:
        sheet = client.open_by_key(SHEET_ID).worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        sheet = client.open_by_key(SHEET_ID).add_worksheet(title=sheet_name, rows=1000, cols=10)
    sheet.clear()
    if not df.empty:
        sheet.update([df.columns.values.tolist()] + df.values.tolist())
    logger.info(f"Appended {len(df)} rows to {sheet_name}")
    return sheet

def dedupe_df(df):
    """Dedupe on Name + Date; sort by Date desc."""
    if 'Name' in df.columns and 'Date' in df.columns:
        df = df.drop_duplicates(subset=['Name', 'Date'])
    df = df.sort_values('Date', ascending=False)
    return df

def parse_date(date_str):
    """Parse date to YYYY-MM-DD; fallback to original."""
    formats = ['%m/%d/%Y', '%Y-%m-%d', '%d/%m/%Y']
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    return date_str

# Source-specific scrapers

def scrape_brevard():
    """Scrape Brevard County Animal Abuse Database via Selenium empty search."""
    all_data = []
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=options)
    try:
        driver.get("https://www.brevardfl.gov/AnimalAbuseDatabaseSearch")
        search_box = WebDriverWait(driver, SELENIUM_TIMEOUT).until(EC.presence_of_element_located((By.NAME, "defendantName")))
        search_box.clear()
        search_box.send_keys(Keys.RETURN)
        time.sleep(3)
        rows = driver.find_elements(By.TAG_NAME, "tr")[1:]
        for row in rows:
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) >= 2:
                name = cols[0].text.strip()
                date = cols[1].text.strip() if len(cols) > 1 else datetime.now().strftime('%Y-%m-%d')
                all_data.append({
                    'Name': name,
                    'Date': parse_date(date),
                    'County': 'Brevard',
                    'Source': 'Animal Abuse Database',
                    'Details': ' | '.join([c.text.strip() for c in cols[2:]]),
                    'Type': 'Convicted'
                })
    except Exception as e:
        logger.error(f"Brevard scrape failed: {e}")
    finally:
        driver.quit()
    df = pd.DataFrame(all_data)
    return dedupe_df(df)

def scrape_lee_enjoined_and_registry():
    """Scrape Lee County Enjoined (static) and Registry (dynamic)."""
    all_data = []
    
    # Static Enjoined List
    enjoined_url = "https://www.sheriffleefl.org/animal-abuser-registry-enjoined/"
    try:
        response = requests.get(enjoined_url)
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table')
        if table:
            rows = table.find_all('tr')[1:]
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 3:
                    name = cols[0].text.strip()
                    case_num = cols[1].text.strip()
                    date_str = cols[2].text.strip()
                    condition = cols[3].text.strip() if len(cols) > 3 else ''
                    enjoin_date = parse_date(date_str)
                    all_data.append({
                        'Name': name,
                        'Date': enjoin_date,
                        'County': 'Lee',
                        'Source': 'Enjoined List',
                        'Case Number': case_num,
                        'Details': condition,
                        'Type': 'Enjoined'
                    })
    except Exception as e:
        logger.error(f"Lee Enjoined scrape failed: {e}")
    
    # Dynamic Registry
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=options)
    try:
        driver.get("https://www.sheriffleefl.org/animal-abuser-search/")
        wait = WebDriverWait(driver, SELENIUM_TIMEOUT)
        table = wait.until(EC.presence_of_element_located((By.TAG_NAME, 'table')))
        rows = driver.find_elements(By.TAG_NAME, 'tr')[1:]
        for row in rows:
            cols = row.find_elements(By.TAG_NAME, 'td')
            if len(cols) >= 4:
                name = cols[0].text.strip()
                dob = cols[1].text.strip()
                address = cols[2].text.strip()
                charges = cols[3].text.strip()
                entry_date = datetime.now().strftime('%Y-%m-%d')
                all_data.append({
                    'Name': name,
                    'Date': entry_date,
                    'County': 'Lee',
                    'Source': 'Convicted Registry',
                    'Case Number': '',
                    'Details': f"DOB: {dob} | Address: {address} | Charges: {charges}",
                    'Type': 'Convicted'
                })
        time.sleep(2)
    except Exception as e:
        logger.error(f"Lee Registry scrape failed: {e}")
    finally:
        driver.quit()
    
    df = pd.DataFrame(all_data)
    df = dedupe_df(df)
    df['Last Updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return df

def scrape_collier():
    """Scrape Collier County Animal Abuse Search static table."""
    all_data = []
    try:
        response = requests.get("https://www2.colliersheriff.org/animalabusesearch")
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table')
        if table:
            rows = table.find_all('tr')[1:]
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 6:
                    type_ = cols[0].text.strip()
                    name = cols[1].text.strip()
                    dob = cols[2].text.strip()
                    address = cols[3].text.strip()
                    years = cols[4].text.strip()
                    expiration = cols[5].text.strip()
                    charge = cols[6].text.strip() if len(cols) > 6 else ''
                    reg_date = parse_date(expiration) if expiration != 'N/A' else datetime.now().strftime('%Y-%m-%d')
                    all_data.append({
                        'Name': name,
                        'Date': reg_date,
                        'County': 'Collier',
                        'Source': 'Animal Abuse Search',
                        'Details': f"Type: {type_} | DOB: {dob} | Address: {address} | Years: {years} | Charge: {charge}",
                        'Type': type_
                    })
    except Exception as e:
        logger.error(f"Collier scrape failed: {e}")
    df = pd.DataFrame(all_data)
    return dedupe_df(df)

def scrape_hillsborough():
    """Scrape Hillsborough County Registry via Selenium."""
    all_data = []
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=options)
    try:
        driver.get("https://hcfl.gov/residents/animals-and-pets/animal-abuser-registry")
        wait = WebDriverWait(driver, SELENIUM_TIMEOUT)
        table = wait.until(EC.presence_of_element_located((By.TAG_NAME, 'table')))
        rows = driver.find_elements(By.TAG_NAME, 'tr')[1:]
        for row in rows:
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) >= 2:
                name = cols[0].text.strip()
                date = cols[1].text.strip() if len(cols) > 1 else datetime.now().strftime('%Y-%m-%d')
                all_data.append({
                    'Name': name,
                    'Date': parse_date(date),
                    'County': 'Hillsborough',
                    'Source': 'Animal Abuser Registry',
                    'Details': ' | '.join([c.text.strip() for c in cols[2:]]),
                    'Type': 'Convicted'
                })
        time.sleep(2)
    except Exception as e:
        logger.error(f"Hillsborough scrape failed: {e}")
    finally:
        driver.quit()
    df = pd.DataFrame(all_data)
    return dedupe_df(df)

def scrape_miami_dade():
    """Scrape Miami-Dade Registry via Selenium."""
    all_data = []
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=options)
    try:
        driver.get("https://www.miamidade.gov/Apps/ASD/crueltyweb/")
        wait = WebDriverWait(driver, SELENIUM_TIMEOUT)
        rows = driver.find_elements(By.CSS_SELECTOR, ".registry-entry")  # Adjust selector
        for row in rows:
            name = row.find_element(By.CSS_SELECTOR, ".name").text.strip()  # Adjust
            date = row.find_element(By.CSS_SELECTOR, ".date").text.strip()
            all_data.append({
                'Name': name,
                'Date': parse_date(date),
                'County': 'Miami-Dade',
                'Source': 'Animal Abuser Registry',
                'Details': row.text,
                'Type': 'Convicted'
            })
    except Exception as e:
        logger.error(f"Miami-Dade scrape failed: {e}")
    finally:
        driver.quit()
    df = pd.DataFrame(all_data)
    return dedupe_df(df)

def scrape_marion():
    """Scrape Marion County Registry static blocks."""
    all_data = []
    try:
        response = requests.get("https://animalservices.marionfl.org/animal-control/animal-control-and-pet-laws/animal-abuser-registry")
        soup = BeautifulSoup(response.content, 'html.parser')
        entries = soup.find_all('div', class_='registry-entry')  # Adjust selector
        for entry in entries:
            text = entry.get_text()
            name_match = re.search(r'Name:\s*(.+?)(?=\n|$)', text)
            dob_match = re.search(r'Date of Birth:\s*(.+?)(?=\n|$)', text)
            conviction_match = re.search(r'Conviction Date:\s*(.+?)(?=\n|$)', text)
            name = name_match.group(1).strip() if name_match else ''
            date = parse_date(conviction_match.group(1).strip() if conviction_match else datetime.now().strftime('%Y-%m-%d'))
            details = text.replace(name, '').strip()
            all_data.append({
                'Name': name,
                'Date': date,
                'County': 'Marion',
                'Source': 'Animal Abuser Registry',
                'Details': f"DOB: {dob_match.group(1).strip() if dob_match else 'N/A'} | {details}",
                'Type': 'Convicted'
            })
    except Exception as e:
        logger.error(f"Marion scrape failed: {e}")
    df = pd.DataFrame(all_data)
    return dedupe_df(df)

def scrape_pasco():
    """Scrape Pasco County Search via Selenium."""
    all_data = []
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=options)
    try:
        driver.get("https://www.pascoclerk.com/153/Animal-Abuser-Search")
        continue_btn = WebDriverWait(driver, SELENIUM_TIMEOUT).until(EC.element_to_be_clickable((By.LINK_TEXT, "Continue to Search")))
        continue_btn.click()
        time.sleep(3)
        table = driver.find_element(By.TAG_NAME, 'table')
        rows = table.find_elements(By.TAG_NAME, 'tr')[1:]
        for row in rows:
            cols = row.find_elements(By.TAG_NAME, 'td')
            if len(cols) >= 2:
                name = cols[0].text.strip()
                date = cols[1].text.strip() if len(cols) > 1 else datetime.now().strftime('%Y-%m-%d')
                all_data.append({
                    'Name': name,
                    'Date': parse_date(date),
                    'County': 'Pasco',
                    'Source': 'Animal Abuser Search',
                    'Details': ' | '.join([c.text.strip() for c in cols[2:]]),
                    'Type': 'Convicted'
                })
    except Exception as e:
        logger.error(f"Pasco scrape failed: {e}")
    finally:
        driver.quit()
    df = pd.DataFrame(all_data)
    return dedupe_df(df)

def scrape_seminole():
    """Scrape Seminole County Registry via Selenium."""
    all_data = []
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=options)
    try:
        driver.get("https://www.seminolecountyfl.gov/departments-services/prepare-seminole/animal-services/animal-abuse-registry")
        wait = WebDriverWait(driver, SELENIUM_TIMEOUT)
        table = wait.until(EC.presence_of_element_located((By.TAG_NAME, 'table')))
        rows = driver.find_elements(By.TAG_NAME, 'tr')[1:]
        for row in rows:
            cols = row.find_elements(By.TAG_NAME, 'td')
            if len(cols) >= 2:
                name = cols[0].text.strip()
                date = cols[1].text.strip() if len(cols) > 1 else datetime.now().strftime('%Y-%m-%d')
                all_data.append({
                    'Name': name,
                    'Date': parse_date(date),
                    'County': 'Seminole',
                    'Source': 'Animal Abuse Registry',
                    'Details': ' | '.join([c.text.strip() for c in cols[2:]]),
                    'Type': 'Convicted'
                })
        time.sleep(2)
    except Exception as e:
        logger.error(f"Seminole scrape failed: {e}")
    finally:
        driver.quit()
    df = pd.DataFrame(all_data)
    return dedupe_df(df)

def scrape_volusia():
    """Scrape Volusia County PDF database."""
    all_data = []
    pdf_url = "https://vcservices.vcgov.org/AnimalControlAttachments/VolusiaAnimalAbuse.pdf"
    try:
        response = requests.get(pdf_url)
        with pdfplumber.open(response.raw) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                lines = text.split('\n')
                for line in lines:
                    if re.match(r'^[A-Z][a-z]+ [A-Z][a-z]+', line):  # Name pattern
                        parts = line.split(',')  # Assume Name, Date, etc.
                        if len(parts) >= 2:
                            name = parts[0].strip()
                            date = parse_date(parts[1].strip())
                            all_data.append({
                                'Name': name,
                                'Date': date,
                                'County': 'Volusia',
                                'Source': 'Animal Abuse Database PDF',
                                'Details': ' | '.join([p.strip() for p in parts[2:]]),
                                'Type': 'Convicted'
                            })
    except Exception as e:
        logger.error(f"Volusia scrape failed: {e}")
    df = pd.DataFrame(all_data)
    return dedupe_df(df)

def scrape_pinellas():
    """Stub: No public registry for Pinellas."""
    logger.info("Pinellas stub - no public registry; use clerk site manually.")
    return pd.DataFrame()

def scrape_broward():
    """Stub: No public registry for Broward."""
    logger.info("Broward stub - internal DNA list only; manual clerk search.")
    return pd.DataFrame()

def scrape_orange():
    """Stub: No public registry for Orange."""
    logger.info("Orange stub - no live list; manual clerk hunts.")
    return pd.DataFrame()

def scrape_nassau():
    """Stub: No public registry for Nassau."""
    logger.info("Nassau stub - monitor FS 828.27 updates.")
    return pd.DataFrame()

def scrape_monroe():
    """Stub: No public registry for Monroe."""
    logger.info("Monroe stub - no abuser list; check sheriff periodically.")
    return pd.DataFrame()

def scrape_escambia():
    """Stub: No public registry for Escambia."""
    logger.info("Escambia stub - no dedicated list; await statewide registry Jan 2026.")
    return pd.DataFrame()

def scrape_palm_beach():
    """Stub: No public registry for Palm Beach."""
    logger.info("Palm Beach stub - internal 'do not adopt' list; manual checks via Animal Care.")
    return pd.DataFrame()

def scrape_all_sources():
    """Combine all county scrapes, dedupe globally."""
    dfs = [
        scrape_brevard(),
        scrape_lee_enjoined_and_registry(),
        scrape_collier(),
        scrape_hillsborough(),
        scrape_miami_dade(),
        scrape_marion(),
        scrape_pasco(),
        scrape_seminole(),
        scrape_volusia(),
        scrape_pinellas(),
        scrape_broward(),
        scrape_orange(),
        scrape_nassau(),
        scrape_monroe(),
        scrape_escambia(),
        scrape_palm_beach(),
        # Add more as needed
    ]
    combined = pd.concat(dfs, ignore_index=True)
    combined = dedupe_df(combined)
    combined['Last Updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"Combined {len(combined)} unique entries.")
    return combined

def main():
    """Scrape all, upload to Sheet, export CSV."""
    if DRY_RUN:
        logger.info("DRY RUN: Scraping without writes.")
    
    try:
        client = auth_gspread()
        df = scrape_all_sources()
        append_to_sheet(df, 'DNAFL_Master', client)
        df.to_csv('dnafl_latest.csv', index=False)
        logger.info("Scrape done—fresh data ready.")
    except Exception as e:
        logger.error(f"Execution failed: {e}")
        raise

if __name__ == "__main__":
    main()
