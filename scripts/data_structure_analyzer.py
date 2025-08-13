#!/usr/bin/env python3
"""
RSシステムデータの構造分析スクリプト
15個のCSVファイルの詳細構造とリレーション性を分析
"""
import pandas as pd
import json
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple, Any
from collections import defaultdict, Counter
import time


class DataStructureAnalyzer:
    """データ構造分析クラス"""
    
    def __init__(self, extracted_dir: str = "data/extracted"):
        self.extracted_dir = Path(extracted_dir)
        self.output_dir = Path("data/structure_analysis")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # CSVファイルの情報
        self.csv_files = []
        self.analysis_results = {}
        
    def discover_csv_files(self):
        """CSVファイルを発見・整理"""
        print("Discovering CSV files...")
        
        for csv_dir in sorted(self.extracted_dir.iterdir()):
            if csv_dir.is_dir():
                csv_files = list(csv_dir.glob("*.csv"))
                if csv_files:
                    csv_file = csv_files[0]  # 通常1つのCSVファイル
                    
                    # ファイル名からカテゴリを抽出
                    filename = csv_file.name
                    if filename.startswith("1-"):
                        category = "基本情報"
                    elif filename.startswith("2-"):
                        category = "予算・執行" 
                    elif filename.startswith("3-"):
                        category = "効果発現経路"
                    elif filename.startswith("4-"):
                        category = "点検・評価"
                    elif filename.startswith("5-"):
                        category = "支出先"
                    elif filename.startswith("6-"):
                        category = "その他"
                    else:
                        category = "不明"
                    
                    self.csv_files.append({
                        'file_path': csv_file,
                        'filename': filename,
                        'category': category,
                        'subcategory': filename.split('_')[2] if '_' in filename else ''
                    })
        
        print(f"Found {len(self.csv_files)} CSV files")
        for file_info in self.csv_files:
            print(f"  {file_info['category']}: {file_info['filename']}")
    
    def analyze_basic_structure(self):
        """各CSVファイルの基本構造を分析"""
        print("\nAnalyzing basic structure of each CSV...")
        
        for file_info in self.csv_files:
            print(f"  Analyzing: {file_info['filename']}")
            
            try:
                # ファイルサイズ
                file_size = file_info['file_path'].stat().st_size / (1024 * 1024)  # MB
                
                # データを読み込み（エラーハンドリング付き）
                try:
                    df = pd.read_csv(file_info['file_path'], encoding='utf-8', dtype=str)
                except UnicodeDecodeError:
                    try:
                        df = pd.read_csv(file_info['file_path'], encoding='shift_jis', dtype=str)
                    except:
                        df = pd.read_csv(file_info['file_path'], encoding='cp932', dtype=str)
                
                # 基本統計
                analysis = {
                    'filename': file_info['filename'],
                    'category': file_info['category'],
                    'subcategory': file_info['subcategory'],
                    'file_size_mb': round(file_size, 2),
                    'row_count': len(df),
                    'column_count': len(df.columns),
                    'columns': list(df.columns),
                    'memory_usage_mb': round(df.memory_usage(deep=True).sum() / (1024 * 1024), 2)
                }
                
                # 予算事業IDの分析
                if '予算事業ID' in df.columns:
                    project_ids = df['予算事業ID'].dropna()
                    analysis['project_id_analysis'] = {
                        'has_project_id': True,
                        'unique_project_ids': int(project_ids.nunique()),
                        'total_records': len(project_ids),
                        'records_per_project_avg': round(len(project_ids) / project_ids.nunique(), 2),
                        'project_id_range': {
                            'min': int(project_ids.min()) if len(project_ids) > 0 else None,
                            'max': int(project_ids.max()) if len(project_ids) > 0 else None
                        },
                        'null_project_ids': int(df['予算事業ID'].isna().sum())
                    }
                else:
                    analysis['project_id_analysis'] = {'has_project_id': False}
                
                # データ型分析
                dtype_counts = Counter()
                text_fields = []
                numeric_fields = []
                
                for col in df.columns:
                    # 実際のデータからデータ型を推定
                    sample_data = df[col].dropna().head(100)
                    if len(sample_data) == 0:
                        dtype_counts['empty'] += 1
                        continue
                    
                    # 数値かどうかチェック
                    try:
                        pd.to_numeric(sample_data)
                        numeric_fields.append(col)
                        dtype_counts['numeric'] += 1
                    except:
                        text_fields.append(col)
                        dtype_counts['text'] += 1
                
                analysis['data_types'] = {
                    'text_fields': text_fields,
                    'numeric_fields': numeric_fields,
                    'type_distribution': dict(dtype_counts)
                }
                
                # NULL値分析
                null_analysis = {}
                for col in df.columns:
                    null_count = df[col].isna().sum()
                    null_percentage = (null_count / len(df)) * 100
                    if null_percentage > 0:
                        null_analysis[col] = {
                            'null_count': int(null_count),
                            'null_percentage': round(null_percentage, 2)
                        }
                
                analysis['null_analysis'] = null_analysis
                
                # サンプルデータ
                analysis['sample_data'] = df.head(3).fillna('').to_dict('records')
                
                self.analysis_results[file_info['filename']] = analysis
                
            except Exception as e:
                print(f"    Error analyzing {file_info['filename']}: {e}")
                self.analysis_results[file_info['filename']] = {
                    'filename': file_info['filename'],
                    'error': str(e)
                }
    
    def analyze_project_id_coverage(self):
        """予算事業IDのカバレッジを分析"""
        print("\nAnalyzing project ID coverage across files...")
        
        all_project_ids = set()
        file_project_ids = {}
        
        # 各ファイルの事業IDを収集
        for filename, analysis in self.analysis_results.items():
            if 'error' not in analysis and analysis.get('project_id_analysis', {}).get('has_project_id'):
                file_info = next(f for f in self.csv_files if f['filename'] == filename)
                try:
                    df = pd.read_csv(file_info['file_path'], encoding='utf-8', dtype=str)
                    project_ids = set(df['予算事業ID'].dropna().astype(str))
                    file_project_ids[filename] = project_ids
                    all_project_ids.update(project_ids)
                except Exception as e:
                    print(f"    Error reading {filename}: {e}")
        
        total_unique_projects = len(all_project_ids)
        print(f"  Total unique project IDs across all files: {total_unique_projects}")
        
        # カバレッジ分析
        coverage_analysis = {
            'total_unique_project_ids': total_unique_projects,
            'file_coverage': {}
        }
        
        for filename, project_ids in file_project_ids.items():
            coverage_percentage = (len(project_ids) / total_unique_projects) * 100
            missing_ids = all_project_ids - project_ids
            
            coverage_analysis['file_coverage'][filename] = {
                'unique_project_ids': len(project_ids),
                'coverage_percentage': round(coverage_percentage, 2),
                'missing_project_count': len(missing_ids),
                'sample_missing_ids': list(missing_ids)[:5] if missing_ids else []
            }
            
            print(f"    {filename}: {len(project_ids)} IDs ({coverage_percentage:.1f}% coverage)")
        
        # 完全カバレッジのファイルを特定
        complete_coverage_files = [
            filename for filename, data in coverage_analysis['file_coverage'].items()
            if data['coverage_percentage'] >= 99.0
        ]
        
        coverage_analysis['complete_coverage_files'] = complete_coverage_files
        coverage_analysis['recommended_master_table'] = complete_coverage_files[0] if complete_coverage_files else None
        
        self.analysis_results['_coverage_analysis'] = coverage_analysis
    
    def analyze_data_relationships(self):
        """データ関係性を分析"""
        print("\nAnalyzing data relationships...")
        
        relationship_analysis = {
            'one_to_one_candidates': [],
            'one_to_many_tables': [],
            'field_overlap_analysis': {}
        }
        
        # 1:1関係の候補特定
        for filename, analysis in self.analysis_results.items():
            if 'error' not in analysis and analysis.get('project_id_analysis', {}).get('has_project_id'):
                records_per_project = analysis['project_id_analysis'].get('records_per_project_avg', 0)
                
                if records_per_project <= 1.1:  # ほぼ1:1の関係
                    relationship_analysis['one_to_one_candidates'].append({
                        'filename': filename,
                        'category': analysis['category'],
                        'records_per_project': records_per_project,
                        'unique_projects': analysis['project_id_analysis']['unique_project_ids']
                    })
                else:
                    relationship_analysis['one_to_many_tables'].append({
                        'filename': filename,
                        'category': analysis['category'],
                        'records_per_project': records_per_project,
                        'unique_projects': analysis['project_id_analysis']['unique_project_ids']
                    })
        
        # フィールド重複分析
        all_fields = {}
        for filename, analysis in self.analysis_results.items():
            if 'error' not in analysis and 'columns' in analysis:
                for field in analysis['columns']:
                    if field not in all_fields:
                        all_fields[field] = []
                    all_fields[field].append(filename)
        
        # 重複フィールドの特定
        overlapping_fields = {
            field: files for field, files in all_fields.items() 
            if len(files) > 1
        }
        
        relationship_analysis['field_overlap_analysis'] = {
            'total_unique_fields': len(all_fields),
            'overlapping_fields_count': len(overlapping_fields),
            'overlapping_fields': overlapping_fields
        }
        
        self.analysis_results['_relationship_analysis'] = relationship_analysis
    
    def generate_normalization_strategy(self):
        """正規化戦略を生成"""
        print("\nGenerating normalization strategy...")
        
        coverage_analysis = self.analysis_results.get('_coverage_analysis', {})
        relationship_analysis = self.analysis_results.get('_relationship_analysis', {})
        
        # マスターテーブル候補
        one_to_one_tables = relationship_analysis.get('one_to_one_candidates', [])
        master_candidates = [
            table for table in one_to_one_tables 
            if table['category'] == '基本情報'
        ]
        
        # 正規化戦略
        normalization_strategy = {
            'recommended_approach': 'hybrid_normalization',
            'master_table_design': {
                'primary_source': master_candidates[0]['filename'] if master_candidates else None,
                'additional_sources': [t['filename'] for t in master_candidates[1:]] if len(master_candidates) > 1 else [],
                'estimated_records': master_candidates[0]['unique_projects'] if master_candidates else 0
            },
            'related_tables': [],
            'data_preservation': {
                'risk_level': 'low',
                'mitigation_strategy': 'preserve_original_structure_as_backup'
            }
        }
        
        # 関連テーブルの設計
        one_to_many_tables = relationship_analysis.get('one_to_many_tables', [])
        for table in one_to_many_tables:
            normalization_strategy['related_tables'].append({
                'table_name': f"{table['category']}_details",
                'source_file': table['filename'],
                'relationship': 'one_to_many',
                'estimated_records': table['unique_projects'] * table['records_per_project']
            })
        
        # リスク評価
        missing_coverage = [
            filename for filename, data in coverage_analysis.get('file_coverage', {}).items()
            if data['coverage_percentage'] < 95.0
        ]
        
        if missing_coverage:
            normalization_strategy['data_preservation']['risk_level'] = 'medium'
            normalization_strategy['data_preservation']['risk_factors'] = missing_coverage
        
        self.analysis_results['_normalization_strategy'] = normalization_strategy
    
    def save_analysis_results(self):
        """分析結果を保存"""
        print("\nSaving analysis results...")
        
        # 詳細JSON結果
        json_path = self.output_dir / 'detailed_structure_analysis.json'
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(self.analysis_results, f, ensure_ascii=False, indent=2, default=str)
        print(f"  Detailed analysis saved: {json_path}")
        
        # サマリーレポート生成
        self.generate_summary_report()
        
        # HTMLレポート生成
        self.generate_html_report()
    
    def generate_summary_report(self):
        """サマリーレポートを生成"""
        summary = {
            'overview': {
                'total_files': len([f for f in self.analysis_results.values() if 'error' not in f]),
                'total_records': sum(f.get('row_count', 0) for f in self.analysis_results.values() if 'error' not in f),
                'total_fields': sum(f.get('column_count', 0) for f in self.analysis_results.values() if 'error' not in f),
                'total_size_mb': sum(f.get('file_size_mb', 0) for f in self.analysis_results.values() if 'error' not in f)
            }
        }
        
        # カテゴリ別サマリー
        category_summary = defaultdict(lambda: {'files': 0, 'records': 0, 'avg_records_per_project': 0})
        
        for analysis in self.analysis_results.values():
            if 'error' not in analysis and 'category' in analysis:
                category = analysis['category']
                category_summary[category]['files'] += 1
                category_summary[category]['records'] += analysis.get('row_count', 0)
                
                project_analysis = analysis.get('project_id_analysis', {})
                if project_analysis.get('has_project_id'):
                    category_summary[category]['avg_records_per_project'] = project_analysis.get('records_per_project_avg', 0)
        
        summary['category_breakdown'] = dict(category_summary)
        
        # 正規化推奨事項
        if '_normalization_strategy' in self.analysis_results:
            summary['normalization_recommendations'] = self.analysis_results['_normalization_strategy']
        
        # サマリー保存
        summary_path = self.output_dir / 'structure_analysis_summary.json'
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2, default=str)
        print(f"  Summary report saved: {summary_path}")
    
    def generate_html_report(self):
        """HTMLレポートを生成"""
        html_content = """
<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <title>RSシステム データ構造分析レポート</title>
    <style>
        body { font-family: 'Segoe UI', Arial, sans-serif; margin: 20px; line-height: 1.6; }
        h1 { color: #2c5aa0; text-align: center; }
        h2 { color: #333; border-bottom: 2px solid #ddd; padding-bottom: 5px; }
        .summary { background-color: #e8f4f8; padding: 20px; border-radius: 8px; margin: 20px 0; }
        .risk-low { color: #28a745; }
        .risk-medium { color: #ffc107; }
        .risk-high { color: #dc3545; }
        table { border-collapse: collapse; width: 100%; margin: 20px 0; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
        .number { font-weight: bold; color: #2c5aa0; }
    </style>
</head>
<body>
    <h1>RSシステム データ構造分析レポート</h1>
    
    <div class="summary">
        <h2>分析サマリー</h2>
"""
        
        # 基本統計を追加
        total_files = len([f for f in self.analysis_results.values() if 'error' not in f])
        total_records = sum(f.get('row_count', 0) for f in self.analysis_results.values() if 'error' not in f)
        
        html_content += f"""
        <p><strong>総ファイル数:</strong> <span class="number">{total_files}</span></p>
        <p><strong>総レコード数:</strong> <span class="number">{total_records:,}</span></p>
"""
        
        # カバレッジ情報
        if '_coverage_analysis' in self.analysis_results:
            coverage = self.analysis_results['_coverage_analysis']
            html_content += f"""
        <p><strong>ユニーク事業ID数:</strong> <span class="number">{coverage['total_unique_project_ids']}</span></p>
"""
        
        html_content += """
    </div>
    
    <h2>ファイル別詳細</h2>
    <table>
        <tr>
            <th>ファイル名</th>
            <th>カテゴリ</th>
            <th>レコード数</th>
            <th>カラム数</th>
            <th>事業ID数</th>
            <th>レコード/事業</th>
        </tr>
"""
        
        # ファイル詳細テーブル
        for analysis in self.analysis_results.values():
            if 'error' not in analysis and 'filename' in analysis:
                project_analysis = analysis.get('project_id_analysis', {})
                html_content += f"""
        <tr>
            <td>{analysis['filename']}</td>
            <td>{analysis.get('category', 'N/A')}</td>
            <td>{analysis.get('row_count', 0):,}</td>
            <td>{analysis.get('column_count', 0)}</td>
            <td>{project_analysis.get('unique_project_ids', 'N/A')}</td>
            <td>{project_analysis.get('records_per_project_avg', 'N/A')}</td>
        </tr>
"""
        
        html_content += """
    </table>
    
    <h2>正規化推奨事項</h2>
"""
        
        # 正規化戦略を追加
        if '_normalization_strategy' in self.analysis_results:
            strategy = self.analysis_results['_normalization_strategy']
            risk_class = f"risk-{strategy['data_preservation']['risk_level']}"
            
            html_content += f"""
    <div class="summary">
        <p><strong>推奨アプローチ:</strong> {strategy['recommended_approach']}</p>
        <p><strong>データ保全リスク:</strong> <span class="{risk_class}">{strategy['data_preservation']['risk_level']}</span></p>
        <p><strong>マスターテーブル候補:</strong> {strategy['master_table_design']['primary_source']}</p>
        <p><strong>関連テーブル数:</strong> {len(strategy['related_tables'])}</p>
    </div>
"""
        
        html_content += """
    <div style="margin-top: 40px; text-align: center; color: #666; font-size: 0.9em;">
        Generated by Data Structure Analyzer
    </div>
</body>
</html>
"""
        
        html_path = self.output_dir / 'structure_analysis_report.html'
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"  HTML report saved: {html_path}")
    
    def run(self):
        """分析パイプラインを実行"""
        print("=" * 60)
        print("RSシステム データ構造分析")
        print("=" * 60)
        
        start_time = time.time()
        
        # 1. CSVファイル発見
        self.discover_csv_files()
        
        # 2. 基本構造分析
        self.analyze_basic_structure()
        
        # 3. 事業IDカバレッジ分析
        self.analyze_project_id_coverage()
        
        # 4. データ関係性分析
        self.analyze_data_relationships()
        
        # 5. 正規化戦略生成
        self.generate_normalization_strategy()
        
        # 6. 結果保存
        self.save_analysis_results()
        
        elapsed = time.time() - start_time
        print(f"\n" + "=" * 60)
        print(f"分析完了! 実行時間: {elapsed:.1f}秒")
        print("=" * 60)
        print(f"出力ディレクトリ: {self.output_dir}")
        print("  - detailed_structure_analysis.json: 詳細分析結果")
        print("  - structure_analysis_summary.json: サマリーレポート")
        print("  - structure_analysis_report.html: HTMLレポート")


if __name__ == "__main__":
    analyzer = DataStructureAnalyzer()
    analyzer.run()