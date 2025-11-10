# DNAFL-app: Florida Do Not Adopt (DNA) Lists Dashboard

[![CI/CD](https://github.com/EliteGreyIT67/DNAFL-app/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/EliteGreyIT67/DNAFL-app/actions/workflows/ci-cd.yml) [![Scrape Schedule](https://github.com/EliteGreyIT67/DNAFL-app/actions/workflows/scrape.yml/badge.svg)](https://github.com/EliteGreyIT67/DNAFL-app/actions/workflows/scrape.yml)

A lightweight, client-side web dashboard for exploring and managing Florida animal abuser registries and enjoined lists (under Florida Statute 828.27). Pulls live data from public Google Sheets (aggregated via automated scraping) for real-time viewing, filtering, and exporting. No backend required—runs entirely in the browser.

**Live Demo**: [https://elitedgreyit67.github.io/DNAFL-app/](https://elitedgreyit67.github.io/DNAFL-app/)

## Table of Contents
- [Features](#features)
- [Quick Start](#quick-start)
- [Installation & Setup](#installation--setup)
- [Usage](#usage)
- [Automation & Deployment](#automation--deployment)
- [Contributing](#contributing)
- [License](#license)
- [Roadmap](#roadmap)

## Features
- **Multi-Tab Interface**: Switch between aggregated DNA lists (e.g., main DNA List, BCAC) and county-specific registries (e.g., Lee Enjoined, Collier, Hillsborough, Volusia, Marion, Brevard, Miami-Dade, Seminole, Pasco).
- **Interactive Filtering**:
  - Global keyword search across all columns.
  - County selector (auto-populates from data).
  - Date range picker (handles formats like YYYY-MM-DD, MM-YY).
  - Column sorting (Name A-Z, Date newest/oldest).
- **Pagination & Export**: 50 rows/page; export filtered/sorted results as CSV.
- **Charts Visualization**: Toggle bar charts for record counts by county (powered by Chart.js)—quick insights into data distribution.
- **UX Enhancements**: Dark/light mode (persists via localStorage), responsive design (mobile/desktop), sticky headers, ARIA labels for accessibility.
- **Data Handling**: Real-time CSV fetches from Google Sheets; multiline support, invalid date warnings, error handling.
- **Automation**: Python scraper updates sheets daily via GitHub Actions; CI/CD for linting/testing/deploys.

## Quick Start
1. **Run Locally**:
   ```
   git clone https://github.com/EliteGreyIT67/DNAFL-app.git
   cd DNAFL-app
   python -m http.server 8000  # Or open index.html in browser
   ```
   Visit `http://localhost:8000`.

2. **View Demo**: Click the live link above—tabs load county data on switch. Toggle charts for visuals.

3. **Customize Data**: Edit `SHEET_ID` in `index.html` or `scraper.py` for your Google Sheet.

## Installation & Setup
### For the Web App
- **Dependencies**: None—uses vanilla HTML/JS + Tailwind CSS (CDN) + Chart.js (CDN).
- **Data Source**: Public Google Sheet (ID: `1V0ERkUXzc2G_SvSVUaVac50KyNOpw4N7bL6yAiZospY`). Sheets like "DNA List", "Lee Enjoined" auto-generate tabs.
- **Local Dev**: Serve via any static server (Python, VS Code Live Server). Edit `tables` object in `<script>` to add/remove tabs.

### For the Scraper (Automation)
1. **Python 3.10+**:
   ```
   pip install -r requirements.txt  # gspread, pandas, selenium, etc.
   ```

2. **Google Sheets API**:
   - [Google Cloud Console](https://console.cloud.google.com/): Enable Sheets/Drive APIs.
   - Create service account → Download `credentials.json`.
   - Share your master sheet (Editor access) with the service account email.

3. **Chrome (for Selenium)**: Install Google Chrome (headless mode used).

4. **Run**:
   ```
   python scraper.py  # Updates sheets
   python scraper.py --dry-run  # Test without writes
   ```

**requirements.txt**:
```
gspread
oauth2client
requests
pandas
pdfplumber
beautifulsoup4
selenium
webdriver-manager
```

## Usage
### Web Dashboard
- **Tabs**: Click to load (e.g., "Collier Registry" shows scraped entries).
- **Charts**: Click "📈 Show Charts" (after loading a tab) for bar graph of county/record counts.
- **Filters**:
  - Search: Type keywords (searches Name, County, Details).
  - County: Dropdown from unique values.
  - Date: Pick range (e.g., 2024-01-01 to 2025-11-10).
  - Sort: Name ascending or Date newest.
- **Export**: Click "📊 Export CSV" for filtered data (filename: `DNAFL-[tab]-[date].csv`).
- **Mobile**: Responsive—scroll horizontally for tables; pinch-zoom charts.

### Scraper
- Fetches from 15+ sources (PDFs, HTML tables, dynamic searches via Selenium).
- Outputs to county-specific sheets (e.g., "Hillsborough Enjoined").
- Dedupes by Name + Date; logs additions.

Example Output (Collier):
| Name              | County | Date       | Details                          | Link |
|-------------------|--------|------------|----------------------------------|------|
| MCCORD DEREK     | Collier| 2024-11-08| Charge: ANIMAL ABUSE (828.12-2) | N/A |

## Automation & Deployment
### GitHub Actions
- **CI/CD (App)**: Lints `index.html` (ESLint, htmlhint), tests fetches, deploys to Pages on main push.
- **Scraper Schedule**: Runs daily (2 AM UTC); lints `scraper.py`, updates sheets, commits changes.
- **Full CI**: Tests both app & scraper on PRs/pushes.
- **Manual Trigger**: Actions tab → Run workflow.

View runs: [GitHub Actions](https://github.com/EliteGreyIT67/DNAFL-app/actions).

### Deployment
- **GitHub Pages**: Auto-deploys from `main` to `gh-pages` (or `main` branch).
- **Custom**: Host on Vercel/Netlify (static site).

## Contributing
1. Fork → Branch (e.g., `feature/new-county`).
2. Lint locally:
   - App: `eslint --plugin html index.html && htmlhint index.html`
   - Scraper: `pip install flake8 && flake8 scraper.py`
3. Test: Run app/scraper; check sheets.
4. PR to `main`: Describe changes (e.g., "Add Pasco scraper").

Issues/PRs welcome! Assisted by xAI's Grok.

## License
MIT License. See [LICENSE](LICENSE).

## Roadmap
- Auth: Private Sheets access.
- Backups: Auto-export sheets to repo.
- More Counties: Escambia, Orange, etc.
- Fuzzy Search: For name variations.
- Advanced Charts: Date trends, exportable PNGs.

Questions? Open an issue or ping @EliteGreyIT67. Thanks for using DNAFL-app—protecting animals, one list at a time! 🐾
