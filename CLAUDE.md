# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python project for processing and visualizing data from the RS System (行政事業レビュー見える化サイト - Administrative Business Review Visualization Site). The RS System is a Japanese government platform that provides budget and project data.

## Commands

### Setup and Environment
```bash
# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt

# Install Playwright browsers (required for automated download)
python -m playwright install
```

### Data Processing
```bash
# Process manually downloaded data (traditional method)
python scripts/process_local_data.py

# Automated download with Playwright (recommended)
python scripts/rs_playwright_downloader.py --year 2024 --out ./data/rs --headless false

# Attempt basic automatic download (usually fails due to SPA)
python scripts/fetch_rs_data.py
```

## Architecture

### Data Processing Pipeline

The project uses three main scripts with distinct responsibilities:

1. **`rs_playwright_downloader.py`** (RSSystemPlaywrightDownloader class) - Recommended
   - Uses Playwright for browser automation to handle JavaScript-heavy SPA
   - Intercepts downloads through both download events and network responses
   - Persists session state to avoid repeated logins
   - Automatically merges CSV files with encoding detection
   - Respects robots.txt and implements polite crawling

2. **`process_local_data.py`** (RSDataProcessor class) - Traditional workflow
   - Processes manually downloaded ZIP files from `downloads/` directory
   - Extracts and analyzes CSV/Excel files
   - Generates analysis reports in HTML and JSON formats
   - Handles multiple Japanese encodings automatically (UTF-8, Shift-JIS, CP932, etc.)

3. **`fetch_rs_data.py`** (RSSystemDataFetcher class) - Limited functionality
   - Attempts basic HTTP-based automatic data download
   - Currently limited due to SPA architecture and dynamic URL generation
   - Falls back to placeholder URLs when automatic download fails

### Data Flow

#### Playwright Automated Flow (Recommended)
1. Browser automation navigates to RS System
2. First run: Manual login if required (session saved)
3. Intercepts download events and network responses
4. Saves files to `data/rs/{year}/downloads/`
5. Extracts ZIPs to `data/rs/{year}/extracted/`
6. Merges CSVs to `data/rs/{year}/merged/merged_{year}.csv`

#### Manual Download Flow
1. User manually downloads ZIP files from https://rssystem.go.jp
2. Places files in `downloads/` directory
3. Script extracts to `data/extracted/`
4. Analyzes and saves reports to `data/reports/`
5. Merged data saved to `data/processed/`

### Key Technical Considerations

- **Encoding Handling**: Japanese government data uses various encodings. The scripts automatically detect and handle UTF-8, Shift-JIS, CP932, UTF-8-BOM, ISO-2022-JP, and EUC-JP.

- **RS System Limitations**: The website is a React SPA with session-based dynamic URLs. Playwright script overcomes this by executing JavaScript directly in the browser context.

- **Data Structure**: RS System provides multiple data types (事業データ/project data, シートデータ/sheet data) that may need different processing approaches.

- **Session Management**: Playwright script saves browser state to `storage_state.json` for session persistence across runs.

- **Polite Crawling**: Scripts implement wait times between requests and check robots.txt to respect server resources.

## Output Files

- `data/reports/analysis_report.json` - Detailed analysis in JSON
- `data/reports/analysis_report.html` - Visual HTML report
- `data/processed/merged_data.csv` - Combined dataset

## Error Handling

The scripts include robust error handling for:
- Missing downloads directory
- Invalid ZIP files (HTML content instead of actual ZIPs)
- Encoding detection failures
- Memory management for large datasets