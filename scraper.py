import requests  # For HTTP requests (static sites)
import pandas as pd  # For data manipulation (DataFrames to CSV-like)
import gspread  # For Google Sheets API
from oauth2client.service_account import ServiceAccountCredentials  # For auth
import pdfplumber  # For PDF text extraction
from io import BytesIO  # To handle PDF bytes in memory
from datetime import datetime  # For timestamps in logs
import logging  # For console output
from bs4 import BeautifulSoup  # For HTML parsing (post-Selenium)

# Selenium for dynamic/JS sites
from selenium import webdriver  # Browser automation
from selenium.webdriver.common.by import By  # Element locators
from selenium.webdriver.support.ui import WebDriverWait  # Explicit waits
from selenium.webdriver.support import expected_conditions as EC  # Wait conditions
from selenium.webdriver.chrome.options import Options  # Chrome config
from selenium.common.exceptions import TimeoutException, NoSuchElementException  # Error handling
from webdriver_manager.chrome import ChromeDriverManager  # Auto-install ChromeDriver
import time  # For delays

# === CONFIGURATION ===
# Customize these:
SHEET_ID = '1V0ERkUXzc2G_SvSVUaVac50KyNOpw4N7bL6yAiZospY'  # Your master Google Sheet ID
CREDENTIALS_FILE = 'credentials.json'  # Path to your service account JSON key
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']  # API scopes for read/write
SELENIUM_TIMEOUT = 10  # Seconds to wait for elements (adjust for slow sites)

# Setup logging: Outputs to console with timestamps
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Authenticate with Google Sheets API
creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, SCOPE)
client = gspread.authorize(creds)
sheet = client.open_by_key(SHEET_ID)  # Open the master sheet

# === HELPER FUNCTIONS ===
def update_sheet(sheet_name, df):
    """
    Append new data to a Google Sheet worksheet.
    - Dedupes by 'Name' + 'Date' (case-insensitive).
    - Creates worksheet if it doesn't exist.
    """
    try:
        # Get or create worksheet (1000 rows, 10 cols default)
        ws_list = [ws.title for ws in sheet.worksheets()]
        if sheet_name not in ws_list:
            ws = sheet.add_worksheet(title=sheet_name, rows=1000, cols=10)
        else:
            ws = sheet.worksheet(sheet_name)
        
        # Load existing data
        existing = pd.DataFrame(ws.get_all_records())
        if not existing.empty:
            # Create unique key for deduping
            existing['key'] = existing['Name'].str.lower() + '_' + existing['Date'].astype(str)
            df['key'] = df['Name'].str.lower() + '_' + df['Date'].astype(str)
            # Filter out duplicates
            df = df[~df['key'].isin(existing['key'])]
            df = df.drop('key', axis=1)  # Clean up temp column
        
        if df.empty:
            logger.info(f"No new data for {sheet_name}")
            return
        
        # Append rows (RAW option preserves formatting)
        ws.append_rows(df.values.tolist(), value_input_option='RAW')
        logger.info(f"Added {len(df)} rows to {sheet_name}")
    except Exception as e:
        logger.error(f"Error updating {sheet_name}: {e}")

def parse_pdf_to_df(url, county):
    """
    Extract text from PDF URL and parse into DataFrame.
    - Assumes simple line-based format (e.g., "Name,Date,Details").
    - Customize parsing logic per PDF if needed.
    """
    try:
        resp = requests.get(url)
        resp.raise_for_status()  # Raise error on bad HTTP
        
        with pdfplumber.open(BytesIO(resp.content)) as pdf:
            # Extract all text, join pages
            text = '\n'.join(page.extract_text() or '' for page in pdf.pages)
        
        # Split lines, filter valid entries (has commas, >=2 parts)
        lines = [line.strip() for line in text.split('\n') if ',' in line and len(line.split(',')) >= 2]
        data = []
        for line in lines:
            parts = [p.strip() for p in line.split(',')]
            if len(parts) >= 2:
                name = parts[0]
                date = parts[1] if len(parts) > 1 else 'N/A'
                details = ', '.join(parts[2:]) if len(parts) > 2 else 'N/A'
                data.append({
                    'Name': name,
                    'County': county,
                    'Date': date,  # Normalize dates later if needed (e.g., pd.to_datetime)
                    'Details': details,
                    'Link': 'N/A'
                })
        
        df = pd.DataFrame(data)
        if not df.empty:
            df = df.drop_duplicates(subset=['Name', 'Date'])  # Quick in-script dedupe
        return df
    except Exception as e:
        logger.error(f"PDF scrape failed for {url}: {e}")
        return pd.DataFrame()

def parse_html_to_df(url, county, table_selector='table'):
    """
    Extract table from HTML URL into DataFrame.
    - Uses BeautifulSoup for parsing.
    - Assumes first table; renames cols to standard (Name, Date, Details).
    """
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, 'html.parser')
        table = soup.select_one(table_selector)
        if not table:
            logger.warning(f"No table found with selector '{table_selector}' at {url}")
            return pd.DataFrame()
        
        # Parse table to DF (pd.read_html handles <table>)
        df = pd.read_html(str(table))[0]
        df['County'] = county
        df['Link'] = 'N/A'
        
        # Standardize columns (flexible: map first few cols)
        col_map = {df.columns[0]: 'Name', df.columns[1]: 'Date'}
        if len(df.columns) > 2:
            col_map[df.columns[2]] = 'Details'
        df = df.rename(columns=col_map)
        df = df[['Name', 'County', 'Date', 'Details', 'Link']]  # Reorder/trim
        
        # Post-process: Combine extra cols into Details if present
        extra_cols = [col for col in df.columns if col not in ['Name', 'County', 'Date', 'Details', 'Link']]
        if extra_cols:
            df['Details'] += ' | ' + df[extra_cols].astype(str).sum(axis=1)
            df = df.drop(extra_cols, axis=1)
        
        df = df.drop_duplicates(subset=['Name', 'Date'])
        return df
    except Exception as e:
        logger.error(f"HTML scrape failed for {url}: {e}")
        return pd.DataFrame()

def scrape_dynamic_with_selenium(url, county, search_input_id, submit_id, table_selector, search_terms=None):
    """
    Scrape dynamic site with Selenium: Navigate, search (loop terms if provided), parse results.
    - search_terms: List like ['A', 'B', ..., 'Z'] for A-Z scrape; or single wildcard '*'.
    - Returns combined DF from all searches.
    """
    if search_terms is None:
        search_terms = ['*']  # Default: single broad search
    
    options = Options()
    options.add_argument('--headless')  # Run without UI
    options.add_argument('--no-sandbox')  # For stability in containers
    options.add_argument('--disable-dev-shm-usage')  # Avoid crashes
    
    driver = None
    all_data = []
    try:
        # Setup driver with auto-managed Chrome
        driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
        wait = WebDriverWait(driver, SELENIUM_TIMEOUT)
        
        driver.get(url)
        logger.info(f"Loaded dynamic page: {url}")
        
        for term in search_terms:
            # Find and fill search input
            search_input = wait.until(EC.presence_of_element_located((By.ID, search_input_id)))
            search_input.clear()
            search_input.send_keys(term)
            
            # Submit search
            submit_btn = driver.find_element(By.ID, submit_id)
            submit_btn.click()
            
            # Wait for results table
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, table_selector)))
            logger.info(f"Fetched results for search term: {term}")
            
            # Parse table with BeautifulSoup
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            table = soup.select_one(table_selector)
            if table:
                df_temp = pd.read_html(str(table))[0]
                # Standardize: Assume cols like Name, Date, etc.; add County/Link
                df_temp['County'] = county
                df_temp['Link'] = 'N/A'
                col_map = {df_temp.columns[0]: 'Name', df_temp.columns[1]: 'Date'}
                if len(df_temp.columns) > 2:
                    col_map[df_temp.columns[2]] = 'Details'
                df_temp = df_temp.rename(columns=col_map)
                df_temp = df_temp[['Name', 'County', 'Date', 'Details', 'Link']]
                all_data.append(df_temp)
            else:
                logger.warning(f"No table found after search '{term}'")
            
            # Delay between searches (polite scraping)
            time.sleep(2)
        
        # Combine all results
        if all_data:
            df = pd.concat(all_data, ignore_index=True)
            df = df.drop_duplicates(subset=['Name', 'Date'])
            return df
        return pd.DataFrame()
    
    except (TimeoutException, NoSuchElementException) as e:
        logger.error(f"Selenium timeout/error for {url}: {e}. Site may have changed.")
        return pd.DataFrame()
    finally:
        if driver:
            driver.quit()

# === SOURCE-SPECIFIC SCRAPERS ===
# Static/PDF
def scrape_hillsborough():
    """Scrape Hillsborough Enjoined PDF (~500 entries)."""
    url = 'https://assets.contentstack.io/v3/assets/blteea73b27b731f985/bltc47cc1e37ac0e54a/Enjoinment%20List.pdf'
    return parse_pdf_to_df(url, 'Hillsborough')

def scrape_volusia():
    """Scrape Volusia Abuse PDF (~60 entries)."""
    url = 'https://vcservices.vcgov.org/AnimalControlAttachments/VolusiaAnimalAbuse.pdf'
    return parse_pdf_to_df(url, 'Volusia')

def scrape_marion_registry():
    """Scrape Marion HTML registry (~30 entries)."""
    url = 'https://animalservices.marionfl.org/animal-control/animal-control-and-pet-laws/animal-abuser-registry'
    df = parse_html_to_df(url, 'Marion', '.registry-table')  # Adjust selector if site changes
    # Example post-process for Marion (add DOB/Expires to Details)
    if not df.empty and 'DOB' in df.columns:
        df['Details'] = df['Details'].fillna('') + ' | DOB: ' + df['DOB'] + ' | Expires: ' + df.get('Expires', 'N/A')
    return df

def scrape_lee_enjoined():
    """Scrape Lee HTML enjoined list (~14 entries)."""
    url = 'https://www.sheriffleefl.org/animal-abuser-registry-enjoined/'
    return parse_html_to_df(url, 'Lee', 'table')  # Generic table selector

# Dynamic with Selenium
def scrape_collier():
    """Scrape Collier dynamic registry: A-Z search for full list."""
    url = 'https://animalabuserregistry.ccsheriff.org/'
    # Customize: ID of search input/submit, table CSS, search terms (A-Z for names)
    return scrape_dynamic_with_selenium(url, 'Collier', 'searchInput', 'submitBtn', 'table.results-table', 
                                        search_terms=[chr(i) for i in range(65, 91)])  # A-Z

def scrape_brevard():
    """Scrape Brevard dynamic search: A-Z for offenders."""
    url = 'https://www.brevardfl.gov/AnimalAbuseDatabaseSearch'
    return scrape_dynamic_with_selenium(url, 'Brevard', 'txtSearch', 'btnSearch', '#resultsGrid', 
                                        search_terms=[chr(i) for i in range(65, 91)])  # A-Z

def scrape_miami_dade():
    """Scrape Miami-Dade cruelty DB: Broad search."""
    url = 'https://www.miamidade.gov/Apps/ASD/crueltyweb/'
    return scrape_dynamic_with_selenium(url, 'Miami-Dade', 'searchField', 'searchSubmit', '.cruelty-table', 
                                        search_terms=['*'])  # Wildcard for all

# === MAIN EXECUTION ===
if __name__ == '__main__':
    logger.info("Starting automated scrape for DNAFL-app (with Selenium)...")
    start_time = datetime.now()
    
    # Static/PDF scrapers
    update_sheet('Hillsborough Enjoined', scrape_hillsborough())
    update_sheet('Volusia Abuse', scrape_volusia())
    update_sheet('Marion Registry', scrape_marion_registry())
    update_sheet('Lee Enjoined', scrape_lee_enjoined())
    
    # Dynamic Selenium scrapers (comment out if testing without browser)
    update_sheet('Collier Registry', scrape_collier())
    update_sheet('Brevard Registry', scrape_brevard())
    update_sheet('Miami-Dade Cruelty', scrape_miami_dade())
    
    logger.info(f"Scrape complete in {datetime.now() - start_time}. Check your Google Sheet for updates.")
    logger.info("Tip: Update DNAFL-app 'tables' config with new sheet URLs for auto-tabs.")
