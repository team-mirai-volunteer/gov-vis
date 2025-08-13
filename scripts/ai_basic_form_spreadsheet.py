#!/usr/bin/env python3
"""
基本形AI/ＡＩが使われた事業のスプレッドシート作成
すべての変数を含む完全なデータを出力
"""
import pandas as pd
import json
import re
from pathlib import Path
from typing import Dict, List, Set, Any
from collections import defaultdict
import time


class AIBasicFormSpreadsheetGenerator:
    """基本形AI事業スプレッドシート生成クラス"""
    
    def __init__(self, feather_dir: str = "data/normalized_feather"):
        self.feather_dir = Path(feather_dir)
        self.output_dir = Path("data/ai_basic_form_spreadsheet")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # 基本形AIパターン（統計結果から）
        self.basic_ai_patterns = [
            r'\bAI\b',     # 半角AI（単語境界あり）
            r'\bＡＩ\b',    # 全角AI（単語境界あり）
            r'(?<![a-zA-Z])AI(?![a-zA-Z])',  # より精密な半角AI
            r'(?<![ａ-ｚＡ-Ｚ])ＡＩ(?![ａ-ｚＡ-Ｚ])'  # より精密な全角AI
        ]
        
        self.tables_data = {}
        self.search_config = {}
        self.load_metadata()
    
    def load_metadata(self):
        """メタデータ読み込み"""
        metadata_path = self.feather_dir / 'ai_search_metadata.json'
        if metadata_path.exists():
            with open(metadata_path, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
            self.search_config = metadata.get('ai_search_fields', {})
        else:
            # デフォルト設定
            self.search_config = {
                'projects': ['事業名', '事業の目的', '事業の概要', '現状・課題'],
                'expenditure_info': ['支出先名', '契約概要', '費目', '使途'],
                'goals_performance': ['アクティビティ／活動目標／成果目標', '活動指標／成果指標'],
                'expenditure_connections': ['支出先の支出先ブロック名', '資金の流れの補足情報'],
                'contracts': ['契約先名（国庫債務負担行為等による契約）', '契約概要（契約名）（国庫債務負担行為等による契約）']
            }
    
    def load_feather_tables(self):
        """Featherテーブル読み込み"""
        print("Loading Feather tables...")
        
        for table_name in ['projects', 'expenditure_info', 'goals_performance', 'expenditure_connections', 'contracts']:
            feather_path = self.feather_dir / f"{table_name}.feather"
            if feather_path.exists():
                print(f"  Loading: {table_name}")
                try:
                    df = pd.read_feather(feather_path)
                    self.tables_data[table_name] = df
                    print(f"    Records: {len(df):,}, Columns: {len(df.columns)}")
                    print(f"    Columns: {list(df.columns)}")
                except Exception as e:
                    print(f"    Error loading {table_name}: {e}")
            else:
                print(f"  Warning: {feather_path} not found")
        
        print(f"Loaded {len(self.tables_data)} tables")
    
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
                    # 基本形のみをフィルタ（AIまたはＡＩのみ）
                    if matched_text.upper() in ['AI', 'ＡＩ']:
                        matches.append({
                            'pattern': pattern,
                            'matched_text': matched_text,
                            'position': f"{match.start()}-{match.end()}"
                        })
            except re.error:
                continue
        
        return matches
    
    def find_basic_ai_projects(self) -> Set[int]:
        """基本形AIを含む事業IDを特定"""
        print("Finding projects with basic AI forms...")
        
        basic_ai_projects = set()
        search_summary = defaultdict(int)
        
        for table_name, df in self.tables_data.items():
            print(f"  Searching in {table_name}...")
            
            search_fields = self.search_config.get(table_name, [])
            available_fields = [f for f in search_fields if f in df.columns]
            
            if not available_fields:
                print(f"    No searchable fields in {table_name}")
                continue
            
            table_matches = 0
            
            for idx, record in df.iterrows():
                project_id = record.get('予算事業ID')
                if pd.isna(project_id):
                    continue
                
                project_id = int(project_id)
                
                for field in available_fields:
                    text = record.get(field, '')
                    matches = self.is_basic_ai_match(text)
                    
                    if matches:
                        basic_ai_projects.add(project_id)
                        table_matches += 1
                        search_summary[table_name] += 1
                        break  # プロジェクトごとに一度カウント
            
            print(f"    Found {table_matches} records with basic AI")
        
        print(f"\\nBasic AI Search Summary:")
        for table, count in search_summary.items():
            print(f"  {table}: {count} records")
        print(f"  Total unique projects: {len(basic_ai_projects)}")
        
        return basic_ai_projects
    
    def collect_complete_project_data(self, project_ids: Set[int]) -> pd.DataFrame:
        """基本形AI事業の完全なデータを収集"""
        print(f"Collecting complete data for {len(project_ids)} projects...")
        
        all_project_data = []
        
        for project_id in sorted(project_ids):
            project_record = {
                '予算事業ID': project_id,
                'AI_検出_詳細': '',
                'AI_マッチ_数': 0
            }
            
            ai_matches_detail = []
            total_ai_matches = 0
            
            # 各テーブルからデータを収集
            for table_name, df in self.tables_data.items():
                # プロジェクトIDでフィルタ
                project_df = df[df['予算事業ID'] == project_id]
                
                if project_df.empty:
                    continue
                
                # テーブルの全カラムを追加
                for col in df.columns:
                    if col == '予算事業ID':
                        continue
                    
                    # カラム名にテーブル名をプレフィックス
                    prefixed_col = f"{table_name}_{col}"
                    
                    # 複数レコードがある場合は結合
                    values = project_df[col].dropna().astype(str).unique()
                    if len(values) > 0:
                        # 空文字列や'nan'を除外
                        clean_values = [v for v in values if v and v != 'nan']
                        if clean_values:
                            project_record[prefixed_col] = ' | '.join(clean_values)
                        else:
                            project_record[prefixed_col] = ''
                    else:
                        project_record[prefixed_col] = ''
                
                # AIマッチング詳細の収集
                search_fields = self.search_config.get(table_name, [])
                available_fields = [f for f in search_fields if f in df.columns]
                
                for idx, record in project_df.iterrows():
                    for field in available_fields:
                        text = record.get(field, '')
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
        df_complete = pd.DataFrame(all_project_data)
        
        # カラムを整理（予算事業ID、AI関連、その他の順）
        ai_cols = ['予算事業ID', 'AI_検出_詳細', 'AI_マッチ_数']
        other_cols = [col for col in df_complete.columns if col not in ai_cols]
        ordered_cols = ai_cols + sorted(other_cols)
        
        df_complete = df_complete[ordered_cols]
        
        print(f"Collected complete data: {len(df_complete)} projects, {len(df_complete.columns)} columns")
        
        return df_complete
    
    def generate_data_summary(self, df: pd.DataFrame) -> Dict:
        """データサマリーを生成"""
        print("Generating data summary...")
        
        # 基本統計
        total_projects = len(df)
        total_columns = len(df.columns)
        
        # AI関連統計
        total_ai_matches = df['AI_マッチ_数'].sum()
        avg_ai_matches = df['AI_マッチ_数'].mean()
        max_ai_matches = df['AI_マッチ_数'].max()
        
        # 府省庁別統計（projectsテーブルから）
        ministry_col = 'projects_府省庁'
        if ministry_col in df.columns:
            ministry_stats = df[ministry_col].value_counts().head(20).to_dict()
        else:
            ministry_stats = {}
        
        # 検出テーブル別統計
        table_detection_stats = {}
        for table_name in self.tables_data.keys():
            count = df['AI_検出_詳細'].str.contains(f'{table_name}\\.', na=False).sum()
            table_detection_stats[table_name] = count
        
        # データ品質統計
        non_empty_cols = {}
        for col in df.columns:
            if col.startswith(('projects_', 'expenditure_', 'goals_', 'contracts_')):
                non_empty_count = df[col].notna().sum()
                non_empty_cols[col] = non_empty_count
        
        summary = {
            'basic_statistics': {
                'total_projects': total_projects,
                'total_columns': total_columns,
                'total_ai_matches': total_ai_matches,
                'average_ai_matches_per_project': round(avg_ai_matches, 2),
                'max_ai_matches_per_project': max_ai_matches
            },
            'ministry_distribution': ministry_stats,
            'table_detection_stats': table_detection_stats,
            'data_coverage': {
                'columns_with_data': len([col for col, count in non_empty_cols.items() if count > 0]),
                'top_populated_columns': dict(sorted(non_empty_cols.items(), key=lambda x: x[1], reverse=True)[:20])
            }
        }
        
        return summary
    
    def save_spreadsheet_and_summary(self, df: pd.DataFrame, summary: Dict):
        """スプレッドシートとサマリーを保存"""
        print("Saving spreadsheet and summary...")
        
        # Excel形式で保存
        excel_path = self.output_dir / 'ai_basic_form_complete_data.xlsx'
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            # メインデータ
            df.to_excel(writer, sheet_name='AI基本形事業データ', index=False)
            
            # サマリーシート
            summary_df = pd.DataFrame([
                ['総事業数', summary['basic_statistics']['total_projects']],
                ['総カラム数', summary['basic_statistics']['total_columns']],
                ['総AIマッチ数', summary['basic_statistics']['total_ai_matches']],
                ['1事業あたり平均マッチ数', summary['basic_statistics']['average_ai_matches_per_project']],
                ['最大マッチ数', summary['basic_statistics']['max_ai_matches_per_project']]
            ], columns=['項目', '値'])
            summary_df.to_excel(writer, sheet_name='データサマリー', index=False)
            
            # 府省庁別統計
            if summary['ministry_distribution']:
                ministry_df = pd.DataFrame(list(summary['ministry_distribution'].items()), columns=['府省庁', '事業数'])
                ministry_df.to_excel(writer, sheet_name='府省庁別統計', index=False)
        
        print(f"  Excel saved: {excel_path}")
        
        # CSV形式でも保存
        csv_path = self.output_dir / 'ai_basic_form_complete_data.csv'
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        print(f"  CSV saved: {csv_path}")
        
        # JSON形式のサマリー
        summary_path = self.output_dir / 'ai_basic_form_summary.json'
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2, default=str)
        print(f"  Summary saved: {summary_path}")
        
        # HTMLレポート
        self.generate_html_report(summary, len(df), len(df.columns))
    
    def generate_html_report(self, summary: Dict, total_rows: int, total_cols: int):
        """HTMLレポートを生成"""
        
        html_content = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <title>基本形AI事業スプレッドシート レポート</title>
    <style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 20px; line-height: 1.6; }}
        h1 {{ color: #28a745; text-align: center; border-bottom: 3px solid #28a745; padding-bottom: 10px; }}
        .summary {{ background-color: #e8f5e9; padding: 20px; border-radius: 8px; margin: 20px 0; }}
        .metric {{ font-size: 1.5em; font-weight: bold; text-align: center; margin: 10px 0; color: #2e7d32; }}
        table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
        .highlight {{ background-color: #fff3cd; }}
        ul {{ padding-left: 20px; }}
        li {{ margin: 5px 0; }}
    </style>
</head>
<body>
    <h1>📊 基本形AI事業スプレッドシート レポート</h1>
    
    <div class="summary">
        <h2>📋 データ概要</h2>
        <div class="metric">事業数: {total_rows:,}行</div>
        <div class="metric">変数数: {total_cols:,}列</div>
        <div class="metric">総AIマッチ: {summary['basic_statistics']['total_ai_matches']:,}件</div>
    </div>
    
    <h2>📈 基本統計</h2>
    <table>
        <tr>
            <th>項目</th>
            <th>値</th>
        </tr>
        <tr class="highlight">
            <td><strong>総事業数</strong></td>
            <td>{summary['basic_statistics']['total_projects']:,}事業</td>
        </tr>
        <tr class="highlight">
            <td><strong>総変数数</strong></td>
            <td>{summary['basic_statistics']['total_columns']:,}列</td>
        </tr>
        <tr>
            <td>総AIマッチ数</td>
            <td>{summary['basic_statistics']['total_ai_matches']:,}件</td>
        </tr>
        <tr>
            <td>1事業あたり平均マッチ数</td>
            <td>{summary['basic_statistics']['average_ai_matches_per_project']}</td>
        </tr>
        <tr>
            <td>最大マッチ数</td>
            <td>{summary['basic_statistics']['max_ai_matches_per_project']}</td>
        </tr>
    </table>
    
    <h2>🏢 府省庁別分布</h2>
    <table>
        <tr><th>府省庁</th><th>事業数</th></tr>"""
        
        for ministry, count in list(summary['ministry_distribution'].items())[:15]:
            html_content += f"        <tr><td>{ministry}</td><td>{count}</td></tr>\\n"
        
        html_content += f"""
    </table>
    
    <h2>📁 データ検出テーブル別統計</h2>
    <table>
        <tr><th>テーブル</th><th>AI検出事業数</th></tr>"""
        
        for table, count in summary['table_detection_stats'].items():
            html_content += f"        <tr><td>{table}</td><td>{count}</td></tr>\\n"
        
        html_content += f"""
    </table>
    
    <h2>📋 出力ファイル</h2>
    <ul>
        <li><strong>ai_basic_form_complete_data.xlsx</strong> - Excel形式の完全なスプレッドシート</li>
        <li><strong>ai_basic_form_complete_data.csv</strong> - CSV形式の完全なスプレッドシート</li>
        <li><strong>ai_basic_form_summary.json</strong> - JSON形式のサマリー統計</li>
    </ul>
    
    <h2>💡 データの特徴</h2>
    <ul>
        <li>基本形「AI」「ＡＩ」のみを対象（複合語は除外）</li>
        <li>全5テーブルの完全なデータを統合</li>
        <li>テーブル名をプレフィックスとした変数名</li>
        <li>AI検出詳細とマッチ数を追加</li>
        <li>複数レコードは「|」で結合</li>
    </ul>
    
    <div style="margin-top: 40px; text-align: center; color: #666;">
        Generated by AI Basic Form Spreadsheet Generator
    </div>
</body>
</html>"""
        
        html_path = self.output_dir / 'ai_basic_form_report.html'
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"  HTML report saved: {html_path}")
    
    def run(self):
        """基本形AIスプレッドシート生成パイプライン実行"""
        print("=" * 60)
        print("📊 基本形AI事業スプレッドシート生成")
        print("=" * 60)
        
        start_time = time.time()
        
        # 1. テーブル読み込み
        self.load_feather_tables()
        
        if not self.tables_data:
            print("No tables loaded. Exiting.")
            return None
        
        # 2. 基本形AI事業ID特定
        basic_ai_projects = self.find_basic_ai_projects()
        
        if not basic_ai_projects:
            print("No basic AI projects found. Exiting.")
            return None
        
        # 3. 完全なプロジェクトデータ収集
        complete_data = self.collect_complete_project_data(basic_ai_projects)
        
        # 4. データサマリー生成
        summary = self.generate_data_summary(complete_data)
        
        # 5. スプレッドシート保存
        self.save_spreadsheet_and_summary(complete_data, summary)
        
        elapsed = time.time() - start_time
        
        # 最終結果表示
        print(f"\\n{'='*60}")
        print("🎉 スプレッドシート生成完了!")
        print(f"{'='*60}")
        print(f"📊 事業数: {len(complete_data):,}行")
        print(f"📋 変数数: {len(complete_data.columns):,}列")
        print(f"🎯 AIマッチ: {summary['basic_statistics']['total_ai_matches']:,}件")
        print(f"⏱️  実行時間: {elapsed:.1f}秒")
        print(f"📁 出力先: {self.output_dir}")
        print(f"{'='*60}")
        
        return complete_data, summary


if __name__ == "__main__":
    generator = AIBasicFormSpreadsheetGenerator()
    generator.run()