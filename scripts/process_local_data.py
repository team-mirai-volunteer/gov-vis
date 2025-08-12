#!/usr/bin/env python3
"""
手動でダウンロードしたRSシステムのデータを処理するスクリプト
"""
import os
import zipfile
import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional
import json
import glob


class RSDataProcessor:
    """RSシステムのローカルデータを処理するクラス"""
    
    def __init__(self, input_dir: str = "downloads"):
        self.input_dir = Path(input_dir)
        self.data_dir = Path("data")
        self.extracted_dir = self.data_dir / "extracted"
        self.processed_dir = self.data_dir / "processed"
        self.reports_dir = self.data_dir / "reports"
        
        # ディレクトリ作成
        for dir_path in [self.extracted_dir, self.processed_dir, self.reports_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
    
    def extract_zip_files(self):
        """downloadsフォルダ内のZIPファイルを解凍"""
        if not self.input_dir.exists():
            print(f"Please create '{self.input_dir}' directory and place downloaded ZIP files there.")
            return []
        
        zip_files = list(self.input_dir.glob("*.zip"))
        if not zip_files:
            print(f"No ZIP files found in {self.input_dir}")
            return []
        
        extracted_dirs = []
        for zip_file in zip_files:
            print(f"\nProcessing: {zip_file.name}")
            extract_dir = self.extracted_dir / zip_file.stem
            
            if extract_dir.exists():
                print(f"  Already extracted to: {extract_dir}")
                extracted_dirs.append(extract_dir)
                continue
            
            try:
                with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                    zip_ref.extractall(extract_dir)
                print(f"  Extracted to: {extract_dir}")
                extracted_dirs.append(extract_dir)
            except Exception as e:
                print(f"  Error extracting: {e}")
        
        return extracted_dirs
    
    def analyze_csv_file(self, csv_path: Path) -> Dict:
        """CSVファイルの構造を詳細に分析"""
        print(f"  Analyzing: {csv_path.name}")
        
        try:
            # エンコーディングの自動検出
            encodings = ['utf-8', 'shift_jis', 'cp932', 'utf-8-sig', 'iso-2022-jp']
            df = None
            used_encoding = None
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(csv_path, encoding=encoding, nrows=5)
                    used_encoding = encoding
                    break
                except:
                    continue
            
            if df is None:
                return {
                    'filename': csv_path.name,
                    'error': 'Failed to read CSV with any encoding'
                }
            
            # 全データを読み込んで詳細分析
            df_full = pd.read_csv(csv_path, encoding=used_encoding)
            
            # 数値カラムの統計情報
            numeric_stats = {}
            for col in df_full.select_dtypes(include=['number']).columns:
                numeric_stats[col] = {
                    'mean': float(df_full[col].mean()) if not df_full[col].isna().all() else None,
                    'min': float(df_full[col].min()) if not df_full[col].isna().all() else None,
                    'max': float(df_full[col].max()) if not df_full[col].isna().all() else None,
                    'null_count': int(df_full[col].isna().sum())
                }
            
            # カテゴリカルカラムの情報
            categorical_info = {}
            for col in df_full.select_dtypes(include=['object']).columns:
                unique_values = df_full[col].nunique()
                categorical_info[col] = {
                    'unique_count': int(unique_values),
                    'null_count': int(df_full[col].isna().sum()),
                    'top_values': df_full[col].value_counts().head(5).to_dict() if unique_values < 100 else None
                }
            
            return {
                'filename': csv_path.name,
                'path': str(csv_path),
                'encoding': used_encoding,
                'rows': len(df_full),
                'columns': list(df_full.columns),
                'column_count': len(df_full.columns),
                'dtypes': {k: str(v) for k, v in df_full.dtypes.to_dict().items()},
                'null_counts': df_full.isnull().sum().to_dict(),
                'numeric_stats': numeric_stats,
                'categorical_info': categorical_info,
                'memory_usage_mb': df_full.memory_usage(deep=True).sum() / 1024 / 1024,
                'sample_data': df_full.head(3).to_dict('records')
            }
            
        except Exception as e:
            return {
                'filename': csv_path.name,
                'error': str(e)
            }
    
    def analyze_excel_file(self, excel_path: Path) -> Dict:
        """Excelファイルの構造を分析"""
        print(f"  Analyzing Excel: {excel_path.name}")
        
        try:
            # Excelファイルのシート一覧を取得
            xl_file = pd.ExcelFile(excel_path)
            sheets_info = {}
            
            for sheet_name in xl_file.sheet_names:
                df = pd.read_excel(excel_path, sheet_name=sheet_name)
                sheets_info[sheet_name] = {
                    'rows': len(df),
                    'columns': list(df.columns),
                    'column_count': len(df.columns),
                    'null_percentage': (df.isnull().sum().sum() / (len(df) * len(df.columns)) * 100) if len(df) > 0 else 0
                }
            
            return {
                'filename': excel_path.name,
                'path': str(excel_path),
                'file_type': 'excel',
                'sheets': sheets_info,
                'sheet_count': len(xl_file.sheet_names)
            }
            
        except Exception as e:
            return {
                'filename': excel_path.name,
                'error': str(e)
            }
    
    def process_all_files(self) -> Dict:
        """すべてのデータファイルを処理"""
        analysis_results = {
            'csv_files': [],
            'excel_files': [],
            'summary': {}
        }
        
        # 解凍されたディレクトリを処理
        for extract_dir in self.extracted_dir.iterdir():
            if not extract_dir.is_dir():
                continue
            
            print(f"\n Processing directory: {extract_dir.name}")
            
            # CSVファイルを処理
            csv_files = list(extract_dir.glob('**/*.csv'))
            for csv_file in csv_files:
                result = self.analyze_csv_file(csv_file)
                analysis_results['csv_files'].append(result)
            
            # Excelファイルを処理
            excel_patterns = ['**/*.xlsx', '**/*.xls']
            for pattern in excel_patterns:
                excel_files = list(extract_dir.glob(pattern))
                for excel_file in excel_files:
                    result = self.analyze_excel_file(excel_file)
                    analysis_results['excel_files'].append(result)
        
        # サマリー情報を生成
        analysis_results['summary'] = {
            'total_csv_files': len(analysis_results['csv_files']),
            'total_excel_files': len(analysis_results['excel_files']),
            'total_rows_csv': sum(r.get('rows', 0) for r in analysis_results['csv_files'] if 'error' not in r),
            'total_memory_mb': sum(r.get('memory_usage_mb', 0) for r in analysis_results['csv_files'] if 'error' not in r)
        }
        
        # 結果を保存
        output_path = self.reports_dir / 'analysis_report.json'
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(analysis_results, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"\n Analysis report saved to: {output_path}")
        
        # HTMLレポートも生成
        self.generate_html_report(analysis_results)
        
        return analysis_results
    
    def generate_html_report(self, analysis_results: Dict):
        """分析結果のHTMLレポートを生成"""
        html_content = """
<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <title>RSシステムデータ分析レポート</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        h1 { color: #333; }
        h2 { color: #666; border-bottom: 2px solid #ddd; padding-bottom: 5px; }
        table { border-collapse: collapse; width: 100%; margin: 20px 0; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
        .summary { background-color: #e8f4f8; padding: 15px; border-radius: 5px; margin: 20px 0; }
        .error { color: red; }
        .success { color: green; }
    </style>
</head>
<body>
    <h1>RSシステム データ分析レポート</h1>
    
    <div class="summary">
        <h2>サマリー</h2>
        <ul>
            <li>CSVファイル数: {csv_count}</li>
            <li>Excelファイル数: {excel_count}</li>
            <li>総レコード数（CSV）: {total_rows:,}</li>
            <li>総メモリ使用量: {memory:.2f} MB</li>
        </ul>
    </div>
    
    <h2>CSVファイル詳細</h2>
    <table>
        <tr>
            <th>ファイル名</th>
            <th>行数</th>
            <th>列数</th>
            <th>エンコーディング</th>
            <th>メモリ使用量(MB)</th>
        </tr>
        {csv_rows}
    </table>
    
    <h2>Excelファイル詳細</h2>
    <table>
        <tr>
            <th>ファイル名</th>
            <th>シート数</th>
            <th>シート名</th>
        </tr>
        {excel_rows}
    </table>
</body>
</html>
        """
        
        # CSVファイルの行を生成
        csv_rows = ""
        for csv in analysis_results['csv_files']:
            if 'error' not in csv:
                csv_rows += f"""
        <tr>
            <td>{csv['filename']}</td>
            <td>{csv.get('rows', 0):,}</td>
            <td>{csv.get('column_count', 0)}</td>
            <td>{csv.get('encoding', 'N/A')}</td>
            <td>{csv.get('memory_usage_mb', 0):.2f}</td>
        </tr>"""
            else:
                csv_rows += f"""
        <tr>
            <td>{csv['filename']}</td>
            <td colspan="4" class="error">Error: {csv['error']}</td>
        </tr>"""
        
        # Excelファイルの行を生成
        excel_rows = ""
        for excel in analysis_results['excel_files']:
            if 'error' not in excel:
                sheets = ", ".join(excel.get('sheets', {}).keys())
                excel_rows += f"""
        <tr>
            <td>{excel['filename']}</td>
            <td>{excel.get('sheet_count', 0)}</td>
            <td>{sheets}</td>
        </tr>"""
            else:
                excel_rows += f"""
        <tr>
            <td>{excel['filename']}</td>
            <td colspan="2" class="error">Error: {excel['error']}</td>
        </tr>"""
        
        # HTMLを生成
        html = html_content.format(
            csv_count=analysis_results['summary']['total_csv_files'],
            excel_count=analysis_results['summary']['total_excel_files'],
            total_rows=analysis_results['summary']['total_rows_csv'],
            memory=analysis_results['summary']['total_memory_mb'],
            csv_rows=csv_rows,
            excel_rows=excel_rows
        )
        
        # HTMLファイルを保存
        html_path = self.reports_dir / 'analysis_report.html'
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html)
        
        print(f" HTML report saved to: {html_path}")
    
    def merge_csv_files(self, key_columns: List[str] = None):
        """複数のCSVファイルをマージ"""
        print("\n Merging CSV files...")
        
        all_dataframes = {}
        
        # すべてのCSVファイルを読み込み
        for extract_dir in self.extracted_dir.iterdir():
            if not extract_dir.is_dir():
                continue
            
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
            return None
        
        # キーカラムがある場合はマージ、ない場合は縦結合
        if key_columns and len(all_dataframes) > 1:
            # マージ処理
            dfs = list(all_dataframes.values())
            merged_df = dfs[0]
            for df in dfs[1:]:
                common_cols = set(merged_df.columns) & set(df.columns) & set(key_columns)
                if common_cols:
                    merged_df = pd.merge(merged_df, df, on=list(common_cols), how='outer')
        else:
            # 縦結合
            merged_df = pd.concat(all_dataframes.values(), ignore_index=True, sort=False)
        
        # 結果を保存
        output_path = self.processed_dir / 'merged_data.csv'
        merged_df.to_csv(output_path, index=False, encoding='utf-8')
        print(f" Merged data saved to: {output_path}")
        print(f" Total rows: {len(merged_df):,}")
        print(f" Total columns: {len(merged_df.columns)}")
        
        return merged_df
    
    def run(self):
        """処理パイプラインを実行"""
        print("=" * 50)
        print("RS System Local Data Processor")
        print("=" * 50)
        
        # 1. ZIPファイルの解凍
        print("\n Step 1: Extracting ZIP files...")
        extracted_dirs = self.extract_zip_files()
        
        if not extracted_dirs:
            print("\nNo data to process.")
            print(f"Please download data from https://rssystem.go.jp and place ZIP files in '{self.input_dir}' directory.")
            return
        
        # 2. ファイル分析
        print("\n Step 2: Analyzing data files...")
        analysis_results = self.process_all_files()
        
        # 3. データマージ（オプション）
        if analysis_results['csv_files']:
            print("\n Step 3: Merging CSV files...")
            merged_df = self.merge_csv_files()
        
        print("\n" + "=" * 50)
        print("Processing completed!")
        print("=" * 50)
        print(f"\nReports available at:")
        print(f"  - JSON: {self.reports_dir / 'analysis_report.json'}")
        print(f"  - HTML: {self.reports_dir / 'analysis_report.html'}")
        if (self.processed_dir / 'merged_data.csv').exists():
            print(f"  - Merged CSV: {self.processed_dir / 'merged_data.csv'}")


if __name__ == "__main__":
    processor = RSDataProcessor()
    processor.run()