#!/usr/bin/env python3
"""
RSシステムのデータをダウンロード・処理するためのスクリプト
"""
import os
import re
import zipfile
import pandas as pd
import requests
from pathlib import Path
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
import json


class RSSystemDataFetcher:
    """RSシステムのデータをダウンロード・処理するクラス"""
    
    def __init__(self, base_url: str = "https://rssystem.go.jp/download-csv/2024"):
        self.base_url = base_url
        self.data_dir = Path("data")
        self.raw_dir = self.data_dir / "raw"
        self.extracted_dir = self.data_dir / "extracted"
        self.processed_dir = self.data_dir / "processed"
        
        # ディレクトリ作成
        for dir_path in [self.raw_dir, self.extracted_dir, self.processed_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def fetch_zip_urls(self) -> List[Dict[str, str]]:
        """RSシステムの実際のダウンロードURLを取得"""
        print(f"Fetching actual download URLs...")
        
        # RSシステムの実際のダウンロードエンドポイント
        # 2024年のデータは以下のパターンで提供されている
        base_url = "https://rssystem.go.jp"
        
        # 実際のダウンロードURLパターン
        download_urls = [
            # 令和6年度（2024年度）のデータ
            f"{base_url}/csv/2024/jigyou_data.csv",  # 事業データ
            f"{base_url}/csv/2024/sheet_data.csv",    # シートデータ
            f"{base_url}/csv/2024/all_data.csv",      # 全データ
            
            # APIエンドポイント経由
            f"{base_url}/api/v1/download/csv/2024/jigyou",
            f"{base_url}/api/v1/download/csv/2024/sheet",
            f"{base_url}/api/v1/download/csv/2024/all",
            
            # 直接CSVダウンロード
            f"{base_url}/data/2024/jigyou.csv",
            f"{base_url}/data/2024/sheet.csv",
            f"{base_url}/data/2024/review.csv",
        ]
        
        valid_urls = []
        
        # 各URLの有効性を確認
        for url in download_urls:
            try:
                print(f"Checking: {url}")
                response = self.session.head(url, timeout=5, allow_redirects=True)
                if response.status_code == 200:
                    content_type = response.headers.get('Content-Type', '')
                    if 'csv' in content_type or 'octet-stream' in content_type:
                        filename = os.path.basename(urlparse(url).path)
                        if not filename.endswith('.csv'):
                            filename += '.csv'
                        valid_urls.append({
                            'url': url,
                            'filename': filename,
                            'text': filename
                        })
                        print(f"  ✓ Found valid CSV: {filename}")
            except Exception as e:
                continue
        
        # もしURLが見つからない場合は、Webページから情報を取得
        if not valid_urls:
            print("Trying to fetch from web page...")
            try:
                # メインページを取得
                response = self.session.get("https://rssystem.go.jp", timeout=30)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # ダウンロードリンクを探す
                for link in soup.find_all('a', href=True):
                    href = link['href']
                    if 'download' in href.lower() or 'csv' in href.lower():
                        print(f"Found potential link: {href}")
                
                # JavaScript内のAPIエンドポイントを探す
                scripts = soup.find_all('script')
                for script in scripts:
                    if script.string:
                        # APIパターンを探す
                        api_patterns = [
                            r'/api/[^"\']+csv[^"\']*',
                            r'/download[^"\']+',
                            r'/export[^"\']+',
                        ]
                        for pattern in api_patterns:
                            matches = re.findall(pattern, script.string)
                            for match in matches:
                                print(f"Found API endpoint: {match}")
                
            except Exception as e:
                print(f"Error fetching from web page: {e}")
        
        # デフォルトのCSVファイルを返す（手動でダウンロードが必要な場合）
        if not valid_urls:
            print("\nNote: Automatic download URLs not found.")
            print("You may need to manually download CSV files from:")
            print("https://rssystem.go.jp")
            print("\nReturning placeholder URLs for manual download...")
            
            valid_urls = [
                {
                    'url': 'manual_download_required',
                    'filename': '2024_jigyou.csv',
                    'text': '事業データ（手動ダウンロード必要）'
                },
                {
                    'url': 'manual_download_required',
                    'filename': '2024_sheet.csv',
                    'text': 'シートデータ（手動ダウンロード必要）'
                }
            ]
        
        return valid_urls
    
    def download_file(self, url: str, filename: str) -> Optional[Path]:
        """ファイルをダウンロード"""
        if url == 'manual_download_required':
            print(f"Skipping {filename} - manual download required")
            return None
            
        output_path = self.raw_dir / filename
        
        if output_path.exists():
            print(f"File already exists: {filename}")
            return output_path
        
        print(f"Downloading: {filename}")
        try:
            response = self.session.get(url, stream=True, timeout=60)
            response.raise_for_status()
            
            # コンテンツタイプを確認
            content_type = response.headers.get('Content-Type', '')
            if 'text/html' in content_type:
                print(f"Warning: Got HTML instead of data file for {filename}")
                return None
            
            total_size = int(response.headers.get('content-length', 0))
            
            with open(output_path, 'wb') as f:
                downloaded = 0
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_size > 0:
                            progress = (downloaded / total_size) * 100
                            print(f"\rProgress: {progress:.1f}%", end='')
            
            print(f"\nDownloaded: {filename}")
            return output_path
            
        except Exception as e:
            print(f"Error downloading {filename}: {e}")
            if output_path.exists():
                output_path.unlink()
            return None
    
    def extract_zip(self, zip_path: Path) -> Path:
        """ZIPファイルを解凍"""
        extract_dir = self.extracted_dir / zip_path.stem
        
        if extract_dir.exists():
            print(f"Already extracted: {zip_path.name}")
            return extract_dir
        
        print(f"Extracting: {zip_path.name}")
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
            print(f"Extracted to: {extract_dir}")
            return extract_dir
            
        except Exception as e:
            print(f"Error extracting {zip_path.name}: {e}")
            if extract_dir.exists():
                import shutil
                shutil.rmtree(extract_dir)
            return None
    
    def analyze_csv_structure(self, csv_path: Path) -> Dict:
        """CSVファイルの構造を分析"""
        try:
            # エンコーディングの自動検出
            encodings = ['utf-8', 'shift_jis', 'cp932', 'utf-8-sig']
            df = None
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(csv_path, encoding=encoding, nrows=5)
                    break
                except:
                    continue
            
            if df is None:
                return {'error': 'Failed to read CSV'}
            
            # 全データを読み込んで詳細分析
            df_full = pd.read_csv(csv_path, encoding=encoding)
            
            return {
                'filename': csv_path.name,
                'rows': len(df_full),
                'columns': list(df_full.columns),
                'dtypes': df_full.dtypes.to_dict(),
                'null_counts': df_full.isnull().sum().to_dict(),
                'sample_data': df_full.head(3).to_dict('records')
            }
            
        except Exception as e:
            return {'error': str(e), 'filename': csv_path.name}
    
    def analyze_all_files(self) -> Dict[str, List[Dict]]:
        """解凍されたすべてのCSVファイルを分析"""
        analysis_results = {}
        
        for extract_dir in self.extracted_dir.iterdir():
            if extract_dir.is_dir():
                csv_files = list(extract_dir.glob('**/*.csv'))
                print(f"\nAnalyzing {len(csv_files)} CSV files in {extract_dir.name}")
                
                results = []
                for csv_file in csv_files:
                    print(f"  Analyzing: {csv_file.name}")
                    result = self.analyze_csv_structure(csv_file)
                    results.append(result)
                
                analysis_results[extract_dir.name] = results
        
        # 結果を保存
        output_path = self.processed_dir / 'analysis_results.json'
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(analysis_results, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"\nAnalysis results saved to: {output_path}")
        return analysis_results
    
    def merge_data(self, key_columns: List[str] = None) -> pd.DataFrame:
        """複数のCSVファイルをマージ"""
        print("\nMerging data files...")
        
        all_dataframes = {}
        
        for extract_dir in self.extracted_dir.iterdir():
            if extract_dir.is_dir():
                for csv_file in extract_dir.glob('**/*.csv'):
                    try:
                        # エンコーディングの自動検出
                        for encoding in ['utf-8', 'shift_jis', 'cp932', 'utf-8-sig']:
                            try:
                                df = pd.read_csv(csv_file, encoding=encoding)
                                all_dataframes[csv_file.stem] = df
                                print(f"  Loaded: {csv_file.name} ({len(df)} rows)")
                                break
                            except:
                                continue
                    except Exception as e:
                        print(f"  Error loading {csv_file.name}: {e}")
        
        if not all_dataframes:
            print("No data to merge")
            return pd.DataFrame()
        
        # TODO: 実際のマージロジックは、データの構造を確認してから実装
        # ここでは仮の実装
        merged_df = pd.concat(all_dataframes.values(), ignore_index=True, sort=False)
        
        output_path = self.processed_dir / 'merged_data.csv'
        merged_df.to_csv(output_path, index=False, encoding='utf-8')
        print(f"Merged data saved to: {output_path}")
        
        return merged_df
    
    def run_pipeline(self):
        """データ取得・処理のパイプラインを実行"""
        print("Starting RS System data pipeline...")
        
        # 1. CSVファイルのURL取得
        csv_urls = self.fetch_zip_urls()  # 名前は変更せずそのまま使用
        
        print(f"\nFound {len(csv_urls)} potential files")
        for info in csv_urls:
            print(f"  - {info['filename']}: {info['url']}")
        
        # 2. ダウンロード
        downloaded_files = []
        for info in csv_urls:
            file_path = self.download_file(info['url'], info['filename'])
            if file_path:
                downloaded_files.append(file_path)
            time.sleep(1)  # サーバー負荷軽減
        
        # 3. CSVファイルの処理（ZIPではなく直接CSV）
        if downloaded_files:
            # CSVファイルを直接処理
            for csv_file in downloaded_files:
                if csv_file.suffix == '.csv':
                    print(f"\nAnalyzing CSV: {csv_file.name}")
                    result = self.analyze_csv_structure(csv_file)
                    print(f"  Columns: {len(result.get('columns', []))}")
                    print(f"  Rows: {result.get('rows', 0)}")
        
        # 既存のZIPファイルがあれば解凍
        existing_zips = list(self.raw_dir.glob('*.zip'))
        for zip_file in existing_zips:
            # HTMLファイルでないか確認
            with open(zip_file, 'rb') as f:
                header = f.read(10)
                if not header.startswith(b'PK'):  # ZIPのマジックナンバー
                    print(f"Skipping {zip_file.name} - not a valid ZIP file")
                    continue
            
            extract_dir = self.extract_zip(zip_file)
            if extract_dir:
                # 解凍したCSVファイルを分析
                self.analyze_all_files()
        
        print("\nPipeline completed!")
    
    def generate_alternative_urls(self) -> List[Dict[str, str]]:
        """代替URLパターンを生成"""
        # RSシステムの一般的なファイル名パターン
        patterns = [
            'jigyou_data_2024.zip',  # 事業データ
            'sheet_data_2024.zip',   # シートデータ  
            'review_data_2024.zip',  # レビューデータ
            '2024_all_data.zip',     # 全データ
            'reiwa6_data.zip',       # 令和6年データ
        ]
        
        alt_urls = []
        base_paths = [
            self.base_url,
            'https://rssystem.go.jp/data/',
            'https://rssystem.go.jp/download/',
        ]
        
        for base in base_paths:
            for pattern in patterns:
                alt_urls.append({
                    'url': urljoin(base, pattern),
                    'filename': pattern,
                    'text': pattern
                })
        
        return alt_urls


if __name__ == "__main__":
    fetcher = RSSystemDataFetcher()
    fetcher.run_pipeline()