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
```

### Data Processing
```bash
# Process manually downloaded data (primary method)
python scripts/process_local_data.py

# Attempt automatic download (limited due to SPA architecture)
python scripts/fetch_rs_data.py
```

## Architecture

### Data Processing Pipeline

The project uses two main scripts with distinct responsibilities:

1. **`process_local_data.py`** (RSDataProcessor class) - Primary workflow
   - Processes manually downloaded ZIP files from `downloads/` directory
   - Extracts and analyzes CSV/Excel files
   - Generates analysis reports in HTML and JSON formats
   - Handles multiple Japanese encodings automatically (UTF-8, Shift-JIS, CP932, etc.)
   - Successfully processed 2024 dataset: 15 ZIP files → 553,094 records

2. **`fetch_rs_data.py`** (RSSystemDataFetcher class) - Limited functionality
   - Attempts basic HTTP-based automatic data download
   - Currently limited due to SPA architecture and dynamic URL generation
   - Falls back to placeholder URLs when automatic download fails

### Data Flow

#### Manual Download Flow (Primary Method)
1. User manually downloads ZIP files from https://rssystem.go.jp
2. Places files in `downloads/` directory
3. Script extracts to `data/extracted/`
4. Analyzes and saves reports to `data/reports/`
5. Merged data saved to `data/processed/`

#### Proven Results (2024 Dataset)
- **Downloaded**: 15 ZIP files (各カテゴリの詳細データ)
- **Extracted**: 15 CSV files with automatic encoding detection
- **Processed**: 553,094 total records across all data types
- **Normalized**: 5,664 unique budget projects (matches official count)

### Key Technical Considerations

- **Encoding Handling**: Japanese government data uses various encodings. The scripts automatically detect and handle UTF-8, Shift-JIS, CP932, UTF-8-BOM, ISO-2022-JP, and EUC-JP.

- **RS System Limitations**: The website is a React SPA with session-based dynamic URLs, requiring manual download for reliable data access.

- **Data Structure**: RS System provides multiple data types across 5 categories:
  - Basic Information (組織情報、事業概要等)
  - Budget & Execution (予算・執行)
  - Effect Path (効果発現経路)
  - Expenditure (支出先)
  - Evaluation (点検・評価)

- **Manual Download Success**: Proven workflow with 2024 dataset processing all 15 official data files.

## Output Files

#### Manual Processing (Primary Method)
- `data/reports/analysis_report.json` - Detailed analysis in JSON format
- `data/reports/analysis_report.html` - Visual HTML report
- `data/processed/merged_data.csv` - Combined dataset (concatenated, 553,094 records)
- `downloads/` - Manual download staging area for ZIP files

## Data Processing Results

Based on actual RS System 2024 data processing:

### Dataset Overview
- **Total Files**: 15 ZIP files containing CSV data
- **Data Categories**: 
  - Basic Information (5 files): Organization, project overview, policies, subsidies, related projects
  - Budget & Execution (2 files): Summary, budget types
  - Effect Path (2 files): Goals/performance, goal connections
  - Expenditure (4 files): Payment info, block connections, expenses, contracts
  - Others (2 files): Evaluation, remarks

### Merge Strategies & Record Counts

| Merge Method | Record Count | Description |
|--------------|--------------|-------------|
| **Concatenation (Current)** | **553,094 rows** | Vertical stacking of all files |
| **Budget Project ID + Year** | **5,664 rows** | Normalized by unique projects |

### Key Insights
- **5,664 unique budget projects** in 2024 dataset
- Concatenation preserves all detail records across different data types
- Budget ID merge provides normalized project-centric view
- Budget ID merge results match official RS System project count

## Error Handling

The scripts include robust error handling for:
- Missing downloads directory
- Invalid ZIP files (HTML content instead of actual ZIPs)
- Encoding detection failures (supports UTF-8, Shift-JIS, CP932, EUC-JP, ISO-2022-JP)
- Memory management for large datasets
- Mixed data types in columns (automatic dtype handling)