# DNA Florida List Dashboard

![GitHub Pages Deploy](https://github.com/EliteGreylT67/DNAFL-app/actions/workflows/ci-cd.yml/badge.svg)
![CI](https://github.com/EliteGreyIT67/DNAFL-app/workflows/CI/CD/badge.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A dynamic, self-contained web dashboard for exploring DNA Florida lists, sourced live from Google Sheets. Features intuitive tabs for "DNA List" and "BCAC DNA List", with powerful search, filtering, sorting, pagination, and CSV export capabilities. Built with vanilla HTML/JS, Tailwind CSS for styling, and optimized for mobile/dark mode. Automated CI/CD via GitHub Actions ensures code quality and effortless deployment to GitHub Pages.

**Live Demo**: [https://EliteGreylT67.github.io/DNAFL-app/](https://EliteGreylT67.github.io/DNAFL-app/)

![Dashboard Screenshot](assets/screenshot-dashboard-light.png)  
Screenshot: DNA List tab in light mode (add your own image to assets/ for visuals).

## Features
- **Tabbed Interface**: Seamless switching between "DNA List" (with columns: Name, County, Date, Details, Link) and "BCAC DNA List".
- **Live Data Fetching**: Pulls real-time CSV exports from a public Google Sheets document—no local files required.
- **Search & Advanced Filters**: Global keyword search, county selector (auto-populated), date range picker, and reset button.
- **Sorting & Pagination**: Column-header sorting (date-aware), with 50 rows/page and Prev/Next navigation.
- **Export Functionality**: Download filtered/sorted data as CSV with a single click.
- **UI/UX Enhancements**: Dark mode toggle (persisted in localStorage), responsive design (mobile scrolling), sticky table headers, and accessible ARIA attributes.
- **Robust Data Handling**: Multiline CSV parsing, graceful invalid date management (logs warnings but includes rows), and error handling for fetches.
- **Automation**: GitHub Actions workflow for linting (ESLint, htmlhint), syntax checks, and auto-deployment to Pages.

## Prerequisites
- Modern browser (Chrome, Firefox, etc.).
- Google Sheets document shared publicly ("Anyone with the link can view") for data access. Default: [Provided Sheets](https://docs.google.com/spreadsheets/d/1V0ERkUXzc2G_SvSVUaVac50KyNOpw4N7bL6yAiZospY/edit?usp=sharing).

## Installation/Setup
1. **Clone the Repository**:
   ```bash
   git clone https://github.com/EliteGreylT67/DNAFL-app.git
   cd DNAFL-app
   ```

2. **Customize Data Source (Optional)**:
   - Edit `index.html` (in the `<script>` section) to update `tables.dna.url` and `tables.bcac.url` if using a different Sheets ID or sheet names.
   - Example: `https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/gviz/tq?tqx=out:csv&sheet=DNA%20List`

3. **Local Development**:
   - Serve locally: Use `python -m http.server` (Python 3+) or a tool like VS Code's Live Server.
   - Open `http://localhost:8000/` in your browser to test.

4. **Deployment**:
   - The `.github/workflows/ci-cd.yml` automates deployment to GitHub Pages on pushes to `main`.
   - Enable Pages in repo Settings > Pages > Source: GitHub Actions.
   - Access at `https://EliteGreylT67.github.io/DNAFL-app/`.

## Usage
1. **Open the Dashboard**: Load `index.html` locally or via the deployed URL.
2. **Navigate Tabs**: Click "DNA List" or "BCAC DNA List" to switch views.
3. **Search Data**: Enter keywords in the search bar for instant results across all columns.
4. **Apply Filters**: 
   - Select a county from the dropdown (auto-fills from data).
   - Pick date ranges using the calendar inputs.
   - Hit "Reset Filters" to clear all.
5. **Sort Columns**: Click headers to toggle ascending/descending (dates sort chronologically).
6. **Paginate**: Use Prev/Next to browse pages; info shows current range.
7. **Export**: Click "Export Filtered CSV" for a downloadable file of visible data.
8. **Toggle Dark Mode**: Click the moon/sun icon—theme persists across sessions.

If loading fails, check browser console (e.g., Sheets not public? Update sharing via File > Share in Google Sheets).

## Troubleshooting
- **Data Not Loading**: Ensure Sheets is public; test URLs directly in browser (should download CSV). Console warns on invalid dates/mismatches.
- **Actions Failures**: Check workflow logs in GitHub Actions tab. Common: YAML syntax—use `act` CLI locally for sims (`brew install act` on macOS).
- **Performance**: For large datasets (>10k rows), filtering/sorting is client-side; consider server-side if scaling.
- **Browser Issues**: Test in incognito for localStorage conflicts.

## Roadmap
- Add charting (e.g., county distribution via Chart.js).
- Support authenticated Sheets access (e.g., via API keys).
- Expand Actions: Add scheduled Sheets backups or PR auto-reviews.
- Contributions: Suggest features via issues!

## Contributing
We welcome contributions! Follow these steps:
1. Fork the repo and create a feature branch (`git checkout -b feature/awesome-addition`).
2. Commit changes (`git commit -am 'Add awesome feature'`).
3. Push to the branch (`git push origin feature/awesome-addition`).
4. Open a Pull Request—Actions will auto-lint/test.

For code reviews: Run local lints with `npm install --save-dev eslint htmlhint`, then `npx eslint index.html` and `npx htmlhint index.html`. Adhere to the [Code of Conduct](CODE_OF_CONDUCT.md) (add if needed).

## License
This project is licensed under the MIT License—see the [LICENSE](LICENSE) file for details.

## Acknowledgments
- **Tech Stack**: Tailwind CSS for styling, vanilla JS for logic, Google Sheets for data.
- **Built By**: xAI's Grok—code development, debugging, reviews, and Actions automation in one.
- **Data Ethics**: Use responsibly; ensure Sheets compliance with privacy laws.

Questions or ideas? Open an issue—let's build and automate together!
