#!/usr/bin/env python3
"""
列完全性検証スクリプト
CSVからFeather変換、Ultimate Spreadsheet作成時の列の保持状況を厳密に検証
"""
import pandas as pd
import json
from pathlib import Path
from typing import Dict, List, Set, Tuple
import warnings
warnings.filterwarnings('ignore')


class ColumnIntegrityChecker:
    """列完全性チェッカー"""
    
    def __init__(self):
        self.data_dir = Path("data")
        self.extracted_dir = self.data_dir / "extracted"
        self.feather_dir = self.data_dir / "full_feather"
        self.ultimate_dir = self.data_dir / "ai_ultimate_spreadsheet"
        self.output_dir = self.data_dir / "column_integrity_check"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # CSVファイルマッピング
        self.csv_files = {
            'organizations': '1-1_RS_2024_基本情報_組織情報/1-1_RS_2024_基本情報_組織情報.csv',
            'projects': '1-2_RS_2024_基本情報_事業概要等/1-2_RS_2024_基本情報_事業概要等.csv',
            'policies_laws': '1-3_RS_2024_基本情報_政策・施策、法令等/1-3_RS_2024_基本情報_政策・施策、法令等.csv',
            'subsidies': '1-4_RS_2024_基本情報_補助率等/1-4_RS_2024_基本情報_補助率等.csv',
            'related_projects': '1-5_RS_2024_基本情報_関連事業/1-5_RS_2024_基本情報_関連事業.csv',
            'budget_summary': '2-1_RS_2024_予算・執行_サマリ/2-1_RS_2024_予算・執行_サマリ.csv',
            'budget_items': '2-2_RS_2024_予算・執行_予算種別・歳出予算項目/2-2_RS_2024_予算・執行_予算種別・歳出予算項目.csv',
            'goals_performance': '3-1_RS_2024_効果発現経路_目標・実績/3-1_RS_2024_効果発現経路_目標・実績.csv',
            'goal_connections': '3-2_RS_2024_効果発現経路_目標のつながり/3-2_RS_2024_効果発現経路_目標のつながり.csv',
            'evaluations': '4-1_RS_2024_点検・評価/4-1_RS_2024_点検・評価.csv',
            'expenditure_info': '5-1_RS_2024_支出先_支出情報/5-1_RS_2024_支出先_支出情報.csv',
            'expenditure_connections': '5-2_RS_2024_支出先_支出ブロックのつながり/5-2_RS_2024_支出先_支出ブロックのつながり.csv',
            'expenditure_details': '5-3_RS_2024_支出先_費目・使途/5-3_RS_2024_支出先_費目・使途.csv',
            'contracts': '5-4_RS_2024_支出先_国庫債務負担行為等による契約/5-4_RS_2024_支出先_国庫債務負担行為等による契約.csv',
            'remarks': '6-1_RS_2024_その他備考/6-1_RS_2024_その他備考.csv'
        }
        
        self.results = {
            'csv_columns': {},
            'feather_columns': {},
            'column_comparison': {},
            'ultimate_spreadsheet': {},
            'summary': {}
        }
    
    def detect_encoding(self, file_path: Path) -> str:
        """ファイルのエンコーディングを検出"""
        encodings = ['utf-8', 'shift-jis', 'cp932', 'utf-8-sig', 'iso-2022-jp', 'euc-jp']
        
        for encoding in encodings:
            try:
                with open(file_path, 'r', encoding=encoding) as f:
                    f.read(1000)
                return encoding
            except:
                continue
        return 'utf-8'
    
    def check_csv_columns(self):
        """元のCSVファイルの列を確認"""
        print("\n" + "="*80)
        print("1. CSVファイルの列を確認")
        print("="*80)
        
        total_csv_columns = set()
        
        for table_name, csv_path in self.csv_files.items():
            full_path = self.extracted_dir / csv_path
            
            if not full_path.exists():
                print(f"  ✗ {table_name}: CSVファイルが見つかりません")
                continue
            
            try:
                encoding = self.detect_encoding(full_path)
                df = pd.read_csv(full_path, encoding=encoding, nrows=0)
                columns = list(df.columns)
                
                self.results['csv_columns'][table_name] = {
                    'columns': columns,
                    'count': len(columns),
                    'encoding': encoding
                }
                
                total_csv_columns.update(columns)
                print(f"  ✓ {table_name}: {len(columns)}列 (encoding: {encoding})")
                
            except Exception as e:
                print(f"  ✗ {table_name}: エラー - {e}")
        
        self.results['summary']['total_csv_unique_columns'] = len(total_csv_columns)
        print(f"\n合計ユニーク列数: {len(total_csv_columns)}")
    
    def check_feather_columns(self):
        """Featherファイルの列を確認"""
        print("\n" + "="*80)
        print("2. Featherファイルの列を確認")
        print("="*80)
        
        total_feather_columns = set()
        
        for table_name in self.csv_files.keys():
            feather_path = self.feather_dir / f"{table_name}.feather"
            
            if not feather_path.exists():
                print(f"  ✗ {table_name}: Featherファイルが見つかりません")
                continue
            
            try:
                df = pd.read_feather(feather_path)
                columns = list(df.columns)
                
                self.results['feather_columns'][table_name] = {
                    'columns': columns,
                    'count': len(columns),
                    'rows': len(df)
                }
                
                total_feather_columns.update(columns)
                print(f"  ✓ {table_name}: {len(columns)}列, {len(df):,}行")
                
            except Exception as e:
                print(f"  ✗ {table_name}: エラー - {e}")
        
        self.results['summary']['total_feather_unique_columns'] = len(total_feather_columns)
        print(f"\n合計ユニーク列数: {len(total_feather_columns)}")
    
    def compare_columns(self):
        """CSVとFeatherの列を比較"""
        print("\n" + "="*80)
        print("3. CSV vs Feather 列比較")
        print("="*80)
        
        perfect_match = True
        
        for table_name in self.csv_files.keys():
            if table_name not in self.results['csv_columns'] or \
               table_name not in self.results['feather_columns']:
                continue
            
            csv_cols = set(self.results['csv_columns'][table_name]['columns'])
            feather_cols = set(self.results['feather_columns'][table_name]['columns'])
            
            missing_cols = csv_cols - feather_cols
            added_cols = feather_cols - csv_cols
            
            comparison = {
                'csv_count': len(csv_cols),
                'feather_count': len(feather_cols),
                'match': csv_cols == feather_cols,
                'missing_columns': list(missing_cols),
                'added_columns': list(added_cols)
            }
            
            self.results['column_comparison'][table_name] = comparison
            
            if comparison['match']:
                print(f"  ✓ {table_name}: 完全一致 ({len(csv_cols)}列)")
            else:
                perfect_match = False
                print(f"  ✗ {table_name}: 不一致")
                if missing_cols:
                    print(f"    - 欠落列: {missing_cols}")
                if added_cols:
                    print(f"    - 追加列: {added_cols}")
        
        self.results['summary']['perfect_match'] = perfect_match
        
        if perfect_match:
            print("\n✅ 全テーブルで列が完全に保持されています！")
        else:
            print("\n⚠️ 一部のテーブルで列の不一致があります")
    
    def check_ultimate_spreadsheet(self):
        """Ultimate Spreadsheetの列構成を確認"""
        print("\n" + "="*80)
        print("4. Ultimate Spreadsheetの列確認")
        print("="*80)
        
        csv_path = self.ultimate_dir / "ai_ultimate_all_444_columns.csv"
        
        if not csv_path.exists():
            print("  ✗ Ultimate Spreadsheetが見つかりません")
            return
        
        try:
            df = pd.read_csv(csv_path, nrows=0)
            columns = list(df.columns)
            
            # 列の分類
            ai_meta_cols = ['予算事業ID', 'AI_検出_詳細', 'AI_マッチ_数']
            data_cols = [col for col in columns if col not in ai_meta_cols]
            
            # 各テーブルの列を確認
            table_columns = {}
            for table_name in self.csv_files.keys():
                if table_name in self.results['feather_columns']:
                    original_cols = set(self.results['feather_columns'][table_name]['columns'])
                    spreadsheet_cols = set([col for col in data_cols if col in original_cols])
                    if spreadsheet_cols:
                        table_columns[table_name] = len(spreadsheet_cols)
            
            self.results['ultimate_spreadsheet'] = {
                'total_columns': len(columns),
                'ai_metadata_columns': ai_meta_cols,
                'data_columns_count': len(data_cols),
                'table_breakdown': table_columns
            }
            
            print(f"  総列数: {len(columns)}")
            print(f"  - AIメタデータ列: {len(ai_meta_cols)}")
            print(f"  - データ列: {len(data_cols)}")
            print(f"\n  テーブル別列数:")
            for table, count in table_columns.items():
                print(f"    - {table}: {count}列")
            
            # 理論値との比較
            expected_cols = sum(self.results['feather_columns'][t]['count'] 
                              for t in self.csv_files.keys() 
                              if t in self.results['feather_columns'])
            # ID列の重複を考慮（各テーブルに予算事業IDがある）
            expected_unique = expected_cols - 14  # 15テーブル - 1
            expected_with_ai = expected_unique + 3  # AIメタデータ追加
            
            print(f"\n  理論値との比較:")
            print(f"    - 全テーブル列合計: {expected_cols}")
            print(f"    - ID重複除去後: {expected_unique}")
            print(f"    - AIメタ追加後（理論値）: {expected_with_ai}")
            print(f"    - 実際の列数: {len(columns)}")
            
            if abs(len(columns) - expected_with_ai) <= 15:  # 許容誤差
                print(f"    ✓ 列数は期待値とほぼ一致")
            else:
                print(f"    ⚠️ 列数に差異があります")
                
        except Exception as e:
            print(f"  ✗ エラー: {e}")
    
    def generate_reports(self):
        """検証レポートを生成"""
        print("\n" + "="*80)
        print("5. レポート生成")
        print("="*80)
        
        # JSONレポート
        json_path = self.output_dir / "column_integrity_report.json"
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2, default=str)
        print(f"  ✓ JSONレポート: {json_path}")
        
        # HTMLレポート
        html_path = self.output_dir / "column_integrity_report.html"
        self.generate_html_report(html_path)
        print(f"  ✓ HTMLレポート: {html_path}")
        
        # サマリーレポート
        summary_path = self.output_dir / "integrity_summary.txt"
        self.generate_summary(summary_path)
        print(f"  ✓ サマリー: {summary_path}")
    
    def generate_html_report(self, output_path: Path):
        """HTMLレポートを生成"""
        html_content = """<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <title>列完全性検証レポート</title>
    <style>
        body { font-family: 'Segoe UI', Arial, sans-serif; margin: 20px; background: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        h1 { color: #2c3e50; text-align: center; border-bottom: 3px solid #3498db; padding-bottom: 15px; }
        h2 { color: #34495e; margin-top: 30px; border-left: 5px solid #3498db; padding-left: 10px; }
        .summary { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 8px; margin: 20px 0; }
        .metric { display: inline-block; margin: 10px 20px; }
        .metric-value { font-size: 2em; font-weight: bold; }
        .metric-label { font-size: 0.9em; opacity: 0.9; }
        table { width: 100%; border-collapse: collapse; margin: 20px 0; }
        th { background: #34495e; color: white; padding: 12px; text-align: left; }
        td { padding: 10px; border-bottom: 1px solid #ddd; }
        tr:hover { background: #f8f9fa; }
        .match { color: #27ae60; font-weight: bold; }
        .mismatch { color: #e74c3c; font-weight: bold; }
        .warning { background: #fff3cd; border-left: 5px solid #ffc107; padding: 15px; margin: 20px 0; }
        .success { background: #d4edda; border-left: 5px solid #28a745; padding: 15px; margin: 20px 0; }
        .code { background: #f4f4f4; padding: 10px; border-radius: 5px; font-family: monospace; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🔍 列完全性検証レポート</h1>
"""
        
        # サマリー
        perfect_match = self.results['summary'].get('perfect_match', False)
        if perfect_match:
            html_content += """
        <div class="success">
            <strong>✅ 検証成功！</strong> 全てのテーブルでCSVからFeatherへの変換時に列が完全に保持されています。
        </div>
"""
        else:
            html_content += """
        <div class="warning">
            <strong>⚠️ 注意</strong> 一部のテーブルで列の不一致が検出されました。
        </div>
"""
        
        # 統計サマリー
        html_content += f"""
        <div class="summary">
            <h2 style="color: white; margin-top: 0;">検証統計</h2>
            <div class="metric">
                <div class="metric-value">{self.results['summary'].get('total_csv_unique_columns', 0)}</div>
                <div class="metric-label">CSV総ユニーク列数</div>
            </div>
            <div class="metric">
                <div class="metric-value">{self.results['summary'].get('total_feather_unique_columns', 0)}</div>
                <div class="metric-label">Feather総ユニーク列数</div>
            </div>
            <div class="metric">
                <div class="metric-value">{len(self.results['column_comparison'])}</div>
                <div class="metric-label">検証テーブル数</div>
            </div>
        </div>
"""
        
        # テーブル別比較結果
        html_content += """
        <h2>📊 テーブル別列数比較</h2>
        <table>
            <tr>
                <th>テーブル名</th>
                <th>CSV列数</th>
                <th>Feather列数</th>
                <th>状態</th>
                <th>詳細</th>
            </tr>
"""
        
        for table_name, comparison in self.results['column_comparison'].items():
            status = "match" if comparison['match'] else "mismatch"
            status_text = "✓ 一致" if comparison['match'] else "✗ 不一致"
            details = ""
            if comparison['missing_columns']:
                details += f"欠落: {len(comparison['missing_columns'])}列 "
            if comparison['added_columns']:
                details += f"追加: {len(comparison['added_columns'])}列"
            
            html_content += f"""
            <tr>
                <td><strong>{table_name}</strong></td>
                <td>{comparison['csv_count']}</td>
                <td>{comparison['feather_count']}</td>
                <td class="{status}">{status_text}</td>
                <td>{details}</td>
            </tr>
"""
        
        html_content += """
        </table>
"""
        
        # Ultimate Spreadsheet情報
        if self.results['ultimate_spreadsheet']:
            us = self.results['ultimate_spreadsheet']
            html_content += f"""
        <h2>📑 Ultimate Spreadsheet構成</h2>
        <div class="code">
            <p><strong>総列数:</strong> {us['total_columns']}</p>
            <p><strong>AIメタデータ列:</strong> {', '.join(us['ai_metadata_columns'])}</p>
            <p><strong>データ列数:</strong> {us['data_columns_count']}</p>
        </div>
"""
        
        html_content += """
        <div style="text-align: center; margin-top: 40px; color: #7f8c8d; font-size: 0.9em;">
            列完全性検証レポート - RS Visualization System
        </div>
    </div>
</body>
</html>
"""
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
    
    def generate_summary(self, output_path: Path):
        """テキストサマリーを生成"""
        summary = []
        summary.append("="*80)
        summary.append("列完全性検証サマリー")
        summary.append("="*80)
        
        # 基本統計
        summary.append("\n[基本統計]")
        summary.append(f"CSV総ユニーク列数: {self.results['summary'].get('total_csv_unique_columns', 0)}")
        summary.append(f"Feather総ユニーク列数: {self.results['summary'].get('total_feather_unique_columns', 0)}")
        
        # 検証結果
        summary.append("\n[検証結果]")
        perfect_match = self.results['summary'].get('perfect_match', False)
        if perfect_match:
            summary.append("✅ 全テーブルで列が完全に保持されています")
        else:
            summary.append("⚠️ 一部のテーブルで列の不一致があります")
            
            # 不一致の詳細
            for table_name, comparison in self.results['column_comparison'].items():
                if not comparison['match']:
                    summary.append(f"\n  {table_name}:")
                    if comparison['missing_columns']:
                        summary.append(f"    欠落列: {comparison['missing_columns']}")
                    if comparison['added_columns']:
                        summary.append(f"    追加列: {comparison['added_columns']}")
        
        # Ultimate Spreadsheet
        if self.results['ultimate_spreadsheet']:
            us = self.results['ultimate_spreadsheet']
            summary.append(f"\n[Ultimate Spreadsheet]")
            summary.append(f"総列数: {us['total_columns']}")
            summary.append(f"AIメタデータ列: {len(us['ai_metadata_columns'])}")
            summary.append(f"データ列: {us['data_columns_count']}")
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(summary))
    
    def run(self):
        """検証を実行"""
        print("\n" + "="*80)
        print("列完全性検証を開始")
        print("="*80)
        
        # 各ステップを実行
        self.check_csv_columns()
        self.check_feather_columns()
        self.compare_columns()
        self.check_ultimate_spreadsheet()
        self.generate_reports()
        
        print("\n" + "="*80)
        print("検証完了")
        print("="*80)
        print(f"\nレポートは以下に保存されました:")
        print(f"  - {self.output_dir}/column_integrity_report.json")
        print(f"  - {self.output_dir}/column_integrity_report.html")
        print(f"  - {self.output_dir}/integrity_summary.txt")


def main():
    checker = ColumnIntegrityChecker()
    checker.run()


if __name__ == "__main__":
    main()