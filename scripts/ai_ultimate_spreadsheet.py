#!/usr/bin/env python3
"""
AI事業 究極の完全スプレッドシート作成
全444カラムを含む完全版
"""
import pandas as pd
import json
import re
from pathlib import Path
from typing import Dict, List, Set, Any
from collections import defaultdict
import time
import warnings
warnings.filterwarnings('ignore')


class AIUltimateSpreadsheetGenerator:
    """AI事業究極のスプレッドシート生成クラス"""
    
    def __init__(self, feather_dir: str = "data/full_feather"):
        self.feather_dir = Path(feather_dir)
        self.output_dir = Path("data/ai_ultimate_spreadsheet")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # 基本形AIパターン
        self.basic_ai_patterns = [
            r'\bAI\b',     # 半角AI（単語境界あり）
            r'\bＡＩ\b',    # 全角AI（単語境界あり）
            r'(?<![a-zA-Z])AI(?![a-zA-Z])',  # より精密な半角AI
            r'(?<![ａ-ｚＡ-Ｚ])ＡＩ(?![ａ-ｚＡ-Ｚ])'  # より精密な全角AI
        ]
        
        self.tables_data = {}
        self.metadata = {}
        self.column_mapping = {}
        self.load_metadata()
    
    def load_metadata(self):
        """メタデータを読み込み"""
        print("Loading metadata...")
        
        # メタデータ読み込み
        metadata_path = self.feather_dir / 'full_feather_metadata.json'
        if metadata_path.exists():
            with open(metadata_path, 'r', encoding='utf-8') as f:
                self.metadata = json.load(f)
        
        # カラムマッピング読み込み
        column_mapping_path = self.feather_dir / 'column_mapping.json'
        if column_mapping_path.exists():
            with open(column_mapping_path, 'r', encoding='utf-8') as f:
                self.column_mapping = json.load(f)
        
        print(f"  Found metadata for {len(self.column_mapping)} tables")
        print(f"  Total columns available: {sum(info['column_count'] for info in self.column_mapping.values())}")
    
    def load_feather_tables(self):
        """全カラム保持のFeatherテーブルを読み込み"""
        print("\\nLoading complete Feather tables...")
        
        total_columns = 0
        
        for table_name, info in self.column_mapping.items():
            feather_path = self.feather_dir / f"{table_name}.feather"
            
            if feather_path.exists():
                print(f"  Loading: {table_name} ({info['japanese_name']})")
                try:
                    df = pd.read_feather(feather_path)
                    self.tables_data[table_name] = df
                    total_columns += len(df.columns)
                    print(f"    Records: {len(df):,}, Columns: {len(df.columns)}")
                except Exception as e:
                    print(f"    Error loading {table_name}: {e}")
            else:
                print(f"  Warning: {feather_path} not found")
        
        print(f"\\nLoaded {len(self.tables_data)} tables with {total_columns} total columns")
    
    def is_basic_ai_match(self, text: str) -> List[Dict]:
        """基本形AIマッチをチェック"""
        if not text or pd.isna(text):
            return []
        
        text_str = str(text)
        matches = []
        
        for pattern in self.basic_ai_patterns:
            try:
                found = re.finditer(pattern, text_str, re.IGNORECASE)
                for match in found:
                    matched_text = match.group()
                    if matched_text.upper() in ['AI', 'ＡＩ']:
                        matches.append({
                            'pattern': pattern,
                            'matched_text': matched_text,
                            'position': f"{match.start()}-{match.end()}"
                        })
            except re.error:
                continue
        
        return matches
    
    def find_ai_projects(self) -> Set[int]:
        """基本形AI事業を特定（メタデータの検索フィールドを使用）"""
        print("\\nFinding AI projects using comprehensive search...")
        
        ai_projects = set()
        search_summary = defaultdict(int)
        
        # メタデータから検索フィールドを取得
        ai_search_fields = self.metadata.get('ai_search_fields', {})
        
        for table_name, search_fields in ai_search_fields.items():
            if table_name not in self.tables_data:
                continue
            
            df = self.tables_data[table_name]
            print(f"  Searching in {table_name} ({self.column_mapping[table_name]['japanese_name']})...")
            
            available_fields = [f for f in search_fields if f in df.columns]
            if not available_fields:
                print(f"    No searchable fields found")
                continue
            
            matches_found = 0
            
            for idx, row in df.iterrows():
                # 予算事業IDの取得
                project_id = None
                for col in ['予算事業ID', '予算事業コード', '事業ID']:
                    if col in row and pd.notna(row[col]):
                        try:
                            project_id = int(row[col])
                            break
                        except (ValueError, TypeError):
                            continue
                
                if not project_id:
                    continue
                
                # AI検索
                for field in available_fields:
                    text = row.get(field, '')
                    if self.is_basic_ai_match(text):
                        ai_projects.add(project_id)
                        matches_found += 1
                        search_summary[table_name] += 1
                        break
            
            print(f"    Found {matches_found} AI matches")
        
        print(f"\\nAI Search Summary:")
        for table_name, count in search_summary.items():
            print(f"  {table_name}: {count} matches")
        print(f"Total unique AI projects: {len(ai_projects)}")
        
        return ai_projects
    
    def collect_ultimate_data(self, project_ids: Set[int]) -> pd.DataFrame:
        """全444カラムのAI事業データを収集"""
        print(f"\\nCollecting ULTIMATE data for {len(project_ids)} projects...")
        print("Including ALL 444 columns from ALL tables...")
        
        all_project_data = []
        column_stats = defaultdict(int)
        
        for project_id in sorted(project_ids):
            project_record = {
                '予算事業ID': project_id,
                'AI_検出_詳細': '',
                'AI_マッチ_数': 0
            }
            
            ai_matches_detail = []
            total_ai_matches = 0
            
            # 全テーブルから全カラムのデータを収集
            for table_name, df in self.tables_data.items():
                # プロジェクトIDカラムを特定
                id_col = None
                for col in ['予算事業ID', '予算事業コード', '事業ID']:
                    if col in df.columns:
                        id_col = col
                        break
                
                if not id_col:
                    continue
                
                # プロジェクトのデータを抽出
                try:
                    project_df = df[df[id_col] == project_id]
                except:
                    continue
                
                if project_df.empty:
                    # データがない場合も全カラムを空値で追加
                    for col in df.columns:
                        if col not in ['予算事業ID', '予算事業コード', '事業ID']:
                            prefixed_col = f"{table_name}_{col}"
                            project_record[prefixed_col] = ''
                    continue
                
                # 全カラムのデータを追加（例外なく全て）
                for col in df.columns:
                    if col in ['予算事業ID', '予算事業コード', '事業ID']:
                        continue
                    
                    prefixed_col = f"{table_name}_{col}"
                    
                    # 複数レコードがある場合の処理
                    values = project_df[col].dropna()
                    
                    if len(values) > 0:
                        try:
                            # 数値型の場合
                            if pd.api.types.is_numeric_dtype(values):
                                unique_values = values.unique()
                                if len(unique_values) == 1:
                                    project_record[prefixed_col] = str(unique_values[0])
                                else:
                                    project_record[prefixed_col] = ' | '.join(map(str, unique_values))
                                column_stats[prefixed_col] += 1
                            else:
                                # 文字列型の場合
                                str_values = values.astype(str).unique()
                                clean_values = [v for v in str_values if v and v != 'nan']
                                if clean_values:
                                    project_record[prefixed_col] = ' | '.join(clean_values)
                                    column_stats[prefixed_col] += 1
                                else:
                                    project_record[prefixed_col] = ''
                        except Exception as e:
                            # エラー時は文字列として処理
                            str_values = values.astype(str).unique()
                            clean_values = [v for v in str_values if v and v != 'nan']
                            project_record[prefixed_col] = ' | '.join(clean_values) if clean_values else ''
                    else:
                        project_record[prefixed_col] = ''
                
                # AI検出詳細の収集
                ai_search_fields = self.metadata.get('ai_search_fields', {})
                if table_name in ai_search_fields:
                    search_fields = ai_search_fields[table_name]
                    available_fields = [f for f in search_fields if f in df.columns]
                    
                    for idx, row in project_df.iterrows():
                        for field in available_fields:
                            text = row.get(field, '')
                            matches = self.is_basic_ai_match(text)
                            if matches:
                                for match in matches:
                                    ai_matches_detail.append(f"{table_name}.{field}: {match['matched_text']}")
                                    total_ai_matches += 1
            
            # AI検出詳細を追加
            project_record['AI_検出_詳細'] = ' | '.join(ai_matches_detail)
            project_record['AI_マッチ_数'] = total_ai_matches
            
            all_project_data.append(project_record)
        
        # DataFrameに変換
        df_ultimate = pd.DataFrame(all_project_data)
        
        # カラムを整理（AI関連、テーブル順）
        ai_cols = ['予算事業ID', 'AI_検出_詳細', 'AI_マッチ_数']
        
        # テーブル順にカラムを整理（カテゴリ順）
        table_order = ['organizations', 'projects', 'policies_laws', 'subsidies', 'related_projects',
                      'budget_summary', 'budget_items', 'goals_performance', 'goal_connections',
                      'evaluations', 'expenditure_info', 'expenditure_connections', 
                      'expenditure_details', 'contracts', 'remarks']
        
        other_cols = []
        for table in table_order:
            table_cols = [col for col in df_ultimate.columns if col.startswith(f"{table}_")]
            other_cols.extend(sorted(table_cols))
        
        # 最終的なカラム順序
        ordered_cols = ai_cols + other_cols
        existing_cols = [col for col in ordered_cols if col in df_ultimate.columns]
        df_ultimate = df_ultimate[existing_cols]
        
        print(f"\\nUltimate data collection complete:")
        print(f"  Projects: {len(df_ultimate)}")
        print(f"  Total columns: {len(df_ultimate.columns)} (target: 444+3)")
        print(f"  Columns with data: {len([col for col, count in column_stats.items() if count > 0])}")
        
        # テーブル別カラム数を表示
        for table in table_order:
            table_cols = [col for col in df_ultimate.columns if col.startswith(f"{table}_")]
            if table_cols and table in self.column_mapping:
                japanese_name = self.column_mapping[table]['japanese_name']
                print(f"  {table} ({japanese_name}): {len(table_cols)} columns")
        
        return df_ultimate
    
    def generate_ultimate_summary(self, df: pd.DataFrame) -> Dict:
        """究極のデータサマリーを生成"""
        print("\\nGenerating ultimate data summary...")
        
        summary = {
            'basic_statistics': {
                'total_projects': len(df),
                'total_columns': len(df.columns),
                'data_columns': len(df.columns) - 3,
                'ai_metadata_columns': 3,
                'total_ai_matches': df['AI_マッチ_数'].sum(),
                'average_ai_matches': round(df['AI_マッチ_数'].mean(), 2),
                'max_ai_matches': df['AI_マッチ_数'].max()
            },
            'table_breakdown': {},
            'data_coverage': {},
            'ministry_distribution': {},
            'completeness_analysis': {}
        }
        
        # テーブル別カラム数と充実度
        for table_name, info in self.column_mapping.items():
            table_cols = [col for col in df.columns if col.startswith(f"{table_name}_")]
            if table_cols:
                # データ充実度計算
                coverage_stats = {}
                for col in table_cols:
                    non_empty = df[col].notna() & (df[col] != '')
                    coverage_pct = (non_empty.sum() / len(df)) * 100
                    coverage_stats[col] = round(coverage_pct, 1)
                
                avg_coverage = round(sum(coverage_stats.values()) / len(coverage_stats), 1) if coverage_stats else 0
                
                summary['table_breakdown'][table_name] = {
                    'japanese_name': info['japanese_name'],
                    'category': info['category'],
                    'total_columns': len(table_cols),
                    'original_columns': info['column_count'],
                    'average_coverage': avg_coverage,
                    'columns_with_data': len([col for col, pct in coverage_stats.items() if pct > 0])
                }
        
        # 全体のデータ充実度（上位50カラム）
        all_coverage = {}
        for col in df.columns:
            if not col.startswith('AI_') and col != '予算事業ID':
                non_empty = df[col].notna() & (df[col] != '')
                coverage_pct = (non_empty.sum() / len(df)) * 100
                if coverage_pct > 0:
                    all_coverage[col] = round(coverage_pct, 1)
        
        # 上位50カラムを選択
        top_coverage = dict(sorted(all_coverage.items(), key=lambda x: x[1], reverse=True)[:50])
        summary['data_coverage'] = top_coverage
        
        # 府省庁分布
        ministry_col = 'projects_府省庁'
        if ministry_col in df.columns:
            ministry_counts = df[ministry_col].value_counts().head(20)
            summary['ministry_distribution'] = ministry_counts.to_dict()
        
        # 完全性分析
        summary['completeness_analysis'] = {
            'perfect_records': len(df[df.notna().all(axis=1)]),  # 全カラムにデータがあるレコード数
            'mostly_complete': len(df[df.notna().sum(axis=1) > len(df.columns) * 0.8]),  # 80%以上のカラムにデータ
            'basic_complete': len(df[df.notna().sum(axis=1) > len(df.columns) * 0.5]),  # 50%以上のカラムにデータ
            'total_non_null_cells': df.notna().sum().sum(),
            'total_possible_cells': len(df) * len(df.columns),
            'overall_completeness': round((df.notna().sum().sum() / (len(df) * len(df.columns))) * 100, 2)
        }
        
        return summary
    
    def save_ultimate_output(self, df: pd.DataFrame, summary: Dict):
        """究極のスプレッドシートを保存"""
        print("\\nSaving ULTIMATE spreadsheet...")
        
        # CSVで保存（全カラム対応）
        csv_path = self.output_dir / 'ai_ultimate_all_444_columns.csv'
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        print(f"  CSV saved: {csv_path} ({len(df.columns)} columns)")
        
        # Parquet保存（効率的）
        parquet_path = self.output_dir / 'ai_ultimate_all_444_columns.parquet'
        df.to_parquet(parquet_path, index=False, compression='snappy')
        print(f"  Parquet saved: {parquet_path}")
        
        # Excel保存（可能な場合）
        excel_path = self.output_dir / 'ai_ultimate_all_444_columns.xlsx'
        if len(df.columns) <= 16384:
            try:
                with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                    df.to_excel(writer, sheet_name='AI事業完全データ', index=False)
                    
                    # サマリーシート
                    summary_data = []
                    for key, value in summary['basic_statistics'].items():
                        summary_data.append([key, value])
                    summary_df = pd.DataFrame(summary_data, columns=['項目', '値'])
                    summary_df.to_excel(writer, sheet_name='基本統計', index=False)
                    
                    # テーブル別統計
                    table_data = []
                    for table, stats in summary['table_breakdown'].items():
                        table_data.append([
                            table, stats['japanese_name'], stats['category'],
                            stats['total_columns'], stats['average_coverage']
                        ])
                    table_df = pd.DataFrame(table_data, 
                        columns=['テーブル', '日本語名', 'カテゴリ', 'カラム数', '平均充実度'])
                    table_df.to_excel(writer, sheet_name='テーブル別統計', index=False)
                
                print(f"  Excel saved: {excel_path}")
            except Exception as e:
                print(f"  Excel save error: {e}")
        else:
            print(f"  Excel column limit exceeded ({len(df.columns)} > 16384)")
        
        # 完全カラムリスト保存
        columns_path = self.output_dir / 'ultimate_columns_list.txt'
        with open(columns_path, 'w', encoding='utf-8') as f:
            f.write(f"AI事業 究極の完全スプレッドシート\\n")
            f.write(f"Total columns: {len(df.columns)}\\n")
            f.write("="*100 + "\\n\\n")
            
            # AI関連カラム
            f.write("[AI関連メタデータ] - 3 columns\\n")
            f.write("-"*50 + "\\n")
            for col in ['予算事業ID', 'AI_検出_詳細', 'AI_マッチ_数']:
                if col in df.columns:
                    f.write(f"  {col}\\n")
            
            # テーブル別カラム
            for table_name, info in self.column_mapping.items():
                table_cols = [col for col in df.columns if col.startswith(f"{table_name}_")]
                if table_cols:
                    f.write(f"\\n[{table_name}] {info['japanese_name']} ({info['category']}) - {len(table_cols)} columns\\n")
                    f.write("-"*80 + "\\n")
                    for col in table_cols:
                        clean_col = col.replace(f"{table_name}_", "")
                        f.write(f"  {clean_col}\\n")
        
        print(f"  Column list saved: {columns_path}")
        
        # サマリーJSON保存
        summary_path = self.output_dir / 'ultimate_summary.json'
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2, default=str)
        print(f"  Summary saved: {summary_path}")
        
        # HTMLレポート生成
        self.generate_ultimate_html_report(df, summary)
    
    def generate_ultimate_html_report(self, df: pd.DataFrame, summary: Dict):
        """究極のHTMLレポートを生成"""
        html_content = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <title>AI事業 究極の完全スプレッドシート</title>
    <style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 20px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); min-height: 100vh; }}
        .container {{ max-width: 1400px; margin: 0 auto; background: white; padding: 40px; border-radius: 15px; box-shadow: 0 10px 30px rgba(0,0,0,0.3); }}
        h1 {{ color: #2c3e50; text-align: center; font-size: 3em; margin-bottom: 20px; text-shadow: 2px 2px 4px rgba(0,0,0,0.1); }}
        .ultimate-banner {{ background: linear-gradient(135deg, #ff6b6b, #feca57, #48dbfb, #ff9ff3); padding: 30px; border-radius: 15px; text-align: center; color: white; font-size: 1.5em; font-weight: bold; margin: 30px 0; text-shadow: 1px 1px 2px rgba(0,0,0,0.5); }}
        .stats-hero {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 30px 0; }}
        .hero-stat {{ background: linear-gradient(135deg, #667eea, #764ba2); color: white; padding: 25px; border-radius: 10px; text-align: center; box-shadow: 0 5px 15px rgba(0,0,0,0.2); }}
        .hero-value {{ font-size: 3em; font-weight: bold; margin: 10px 0; text-shadow: 1px 1px 2px rgba(0,0,0,0.3); }}
        .hero-label {{ font-size: 1em; opacity: 0.9; }}
        .section {{ background: #f8f9fa; padding: 25px; margin: 25px 0; border-radius: 10px; border-left: 5px solid #667eea; }}
        table {{ width: 100%; border-collapse: collapse; margin: 20px 0; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
        th {{ background: linear-gradient(135deg, #667eea, #764ba2); color: white; padding: 15px; text-align: left; font-weight: 600; }}
        td {{ padding: 12px; border-bottom: 1px solid #e0e6ed; }}
        tr:hover {{ background: linear-gradient(90deg, #f8f9fa, #e3f2fd); }}
        .ultimate {{ color: #e74c3c; font-weight: bold; font-size: 1.2em; }}
        .success {{ color: #27ae60; font-weight: bold; }}
        .info {{ color: #3498db; }}
        .progress-bar {{ background: #ecf0f1; border-radius: 10px; height: 10px; margin: 5px 0; }}
        .progress-fill {{ background: linear-gradient(90deg, #667eea, #764ba2); height: 100%; border-radius: 10px; transition: width 0.3s; }}
        .footer {{ text-align: center; margin-top: 50px; padding: 30px; background: linear-gradient(135deg, #667eea, #764ba2); color: white; border-radius: 10px; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 AI事業 究極の完全スプレッドシート</h1>
        
        <div class="ultimate-banner">
            ✨ 全 {summary['basic_statistics']['total_columns']} カラム完全収録 ✨<br>
            🎯 基本形AI事業 {summary['basic_statistics']['total_projects']} 件の完全データ
        </div>
        
        <div class="stats-hero">
            <div class="hero-stat">
                <div class="hero-label">事業数</div>
                <div class="hero-value">{summary['basic_statistics']['total_projects']}</div>
            </div>
            <div class="hero-stat">
                <div class="hero-label">総カラム数</div>
                <div class="hero-value ultimate">{summary['basic_statistics']['total_columns']}</div>
            </div>
            <div class="hero-stat">
                <div class="hero-label">AIマッチ数</div>
                <div class="hero-value">{summary['basic_statistics']['total_ai_matches']}</div>
            </div>
            <div class="hero-stat">
                <div class="hero-label">全体充実度</div>
                <div class="hero-value">{summary['completeness_analysis']['overall_completeness']}%</div>
            </div>
        </div>
        
        <div class="section">
            <h2>📊 データ完全性分析</h2>
            <table>
                <tr>
                    <th>完全性レベル</th>
                    <th>事業数</th>
                    <th>割合</th>
                </tr>
                <tr>
                    <td>完全データ（全カラム）</td>
                    <td>{summary['completeness_analysis']['perfect_records']}</td>
                    <td>{round(summary['completeness_analysis']['perfect_records']/summary['basic_statistics']['total_projects']*100, 1)}%</td>
                </tr>
                <tr>
                    <td>高充実度（80%以上）</td>
                    <td>{summary['completeness_analysis']['mostly_complete']}</td>
                    <td>{round(summary['completeness_analysis']['mostly_complete']/summary['basic_statistics']['total_projects']*100, 1)}%</td>
                </tr>
                <tr>
                    <td>基本充実度（50%以上）</td>
                    <td>{summary['completeness_analysis']['basic_complete']}</td>
                    <td>{round(summary['completeness_analysis']['basic_complete']/summary['basic_statistics']['total_projects']*100, 1)}%</td>
                </tr>
            </table>
        </div>
        
        <div class="section">
            <h2>📁 テーブル別詳細統計</h2>
            <table>
                <thead>
                    <tr>
                        <th>テーブル</th>
                        <th>日本語名</th>
                        <th>カテゴリ</th>
                        <th>カラム数</th>
                        <th>充実度</th>
                        <th>進捗</th>
                    </tr>
                </thead>
                <tbody>"""
        
        for table, stats in summary['table_breakdown'].items():
            coverage = stats['average_coverage']
            html_content += f"""
                    <tr>
                        <td><strong>{table}</strong></td>
                        <td>{stats['japanese_name']}</td>
                        <td>{stats['category']}</td>
                        <td class="ultimate">{stats['total_columns']}</td>
                        <td>{coverage}%</td>
                        <td>
                            <div class="progress-bar">
                                <div class="progress-fill" style="width: {coverage}%"></div>
                            </div>
                        </td>
                    </tr>"""
        
        html_content += f"""
                </tbody>
            </table>
        </div>
        
        <div class="section">
            <h2>🏢 府省庁別分布（上位10）</h2>
            <table>
                <thead>
                    <tr>
                        <th>府省庁</th>
                        <th>事業数</th>
                        <th>割合</th>
                    </tr>
                </thead>
                <tbody>"""
        
        total_projects = summary['basic_statistics']['total_projects']
        for ministry, count in list(summary.get('ministry_distribution', {}).items())[:10]:
            percentage = round((count / total_projects) * 100, 1)
            html_content += f"""
                    <tr>
                        <td>{ministry}</td>
                        <td>{count}</td>
                        <td>{percentage}%</td>
                    </tr>"""
        
        html_content += f"""
                </tbody>
            </table>
        </div>
        
        <div class="section">
            <h2>💾 出力ファイル</h2>
            <div class="stats-hero">
                <div class="hero-stat">
                    <h3>CSV形式</h3>
                    <p>ai_ultimate_all_444_columns.csv</p>
                    <p class="ultimate">全{summary['basic_statistics']['total_columns']}カラム</p>
                </div>
                <div class="hero-stat">
                    <h3>Parquet形式</h3>
                    <p>ai_ultimate_all_444_columns.parquet</p>
                    <p class="info">高速アクセス用</p>
                </div>
                <div class="hero-stat">
                    <h3>カラムリスト</h3>
                    <p>ultimate_columns_list.txt</p>
                    <p>完全カラム一覧</p>
                </div>
            </div>
        </div>
        
        <div class="footer">
            <h2>🎉 究極の完全データ</h2>
            <p>15のCSVファイルから抽出した全 {summary['basic_statistics']['total_columns']} カラムを完全収録</p>
            <p>基本形AI事業の最も包括的なデータセット</p>
            <p class="ultimate">Generated by AI Ultimate Spreadsheet Generator</p>
        </div>
    </div>
</body>
</html>"""
        
        html_path = self.output_dir / 'ultimate_report.html'
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"  HTML report saved: {html_path}")
    
    def run(self):
        """究極のスプレッドシート生成を実行"""
        print("="*90)
        print("🚀 AI事業 究極の完全スプレッドシート生成")
        print("   全444カラムを含む究極のデータセット")
        print("="*90)
        
        start_time = time.time()
        
        # 1. 全カラムFeatherテーブル読み込み
        self.load_feather_tables()
        
        if not self.tables_data:
            print("No tables loaded. Exiting.")
            return None
        
        # 2. AI事業の特定
        ai_projects = self.find_ai_projects()
        
        if not ai_projects:
            print("No AI projects found. Exiting.")
            return None
        
        # 3. 究極のデータ収集（全444カラム）
        ultimate_df = self.collect_ultimate_data(ai_projects)
        
        # 4. 究極のサマリー生成
        ultimate_summary = self.generate_ultimate_summary(ultimate_df)
        
        # 5. 究極のスプレッドシート保存
        self.save_ultimate_output(ultimate_df, ultimate_summary)
        
        elapsed = time.time() - start_time
        
        print(f"\\n{'='*90}")
        print("🎉 究極の完全スプレッドシート生成完了!")
        print(f"{'='*90}")
        print(f"📊 事業数: {len(ultimate_df):,}行")
        print(f"📋 総カラム数: {len(ultimate_df.columns):,}列 (究極の完全版)")
        print(f"🎯 AIマッチ数: {ultimate_summary['basic_statistics']['total_ai_matches']:,}件")
        print(f"📈 全体充実度: {ultimate_summary['completeness_analysis']['overall_completeness']}%")
        print(f"⏱️  実行時間: {elapsed:.1f}秒")
        print(f"📁 出力先: {self.output_dir}")
        print(f"{'='*90}")
        
        return ultimate_df, ultimate_summary


if __name__ == "__main__":
    generator = AIUltimateSpreadsheetGenerator()
    generator.run()