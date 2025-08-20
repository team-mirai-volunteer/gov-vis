#!/usr/bin/env python3
"""
事業マスターリスト作成（1行1事業 + JSON詳細保持）
予算事業IDをキーとして全テーブルを統合し、
複数レコードテーブルはJSON形式で詳細を保持
"""
import pandas as pd
import json
from pathlib import Path
from typing import Dict, List, Any
import warnings
warnings.filterwarnings('ignore')


class ProjectMasterCreator:
    """事業マスターリスト作成クラス"""
    
    def __init__(self):
        self.data_dir = Path("data")
        self.feather_dir = self.data_dir / "full_feather"
        self.output_dir = self.data_dir / "project_master"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # テーブル分類
        self.one_to_one_tables = {
            'organizations': '組織情報',
            'policies_laws': '政策・施策・法令等', 
            'subsidies': '補助率等',
            'related_projects': '関連事業',
            'remarks': 'その他備考'
        }
        
        self.one_to_many_tables = {
            'budget_summary': '予算・執行サマリ',
            'budget_items': '予算種別・歳出予算項目',
            'goals_performance': '目標・実績',
            'goal_connections': '目標のつながり',
            'evaluations': '点検・評価',
            'expenditure_info': '支出情報',
            'expenditure_connections': '支出ブロックのつながり',
            'expenditure_details': '費目・使途',
            'contracts': '国庫債務負担行為等による契約'
        }
        
        self.master_data = None
        self.statistics = {}
    
    def load_feather_data(self, table_name: str) -> pd.DataFrame:
        """Featherファイルを読み込み"""
        feather_path = self.feather_dir / f"{table_name}.feather"
        if not feather_path.exists():
            print(f"  ⚠️  {table_name}: Featherファイルが見つかりません")
            return pd.DataFrame()
        
        try:
            df = pd.read_feather(feather_path)
            print(f"  ✓ {table_name}: {len(df):,}行読み込み")
            return df
        except Exception as e:
            print(f"  ✗ {table_name}: 読み込みエラー - {e}")
            return pd.DataFrame()
    
    def create_base_master(self):
        """メインテーブル（projects）をベースにマスターを作成"""
        print("\n" + "="*80)
        print("1. ベースマスター作成（projects テーブル）")
        print("="*80)
        
        projects_df = self.load_feather_data('projects')
        if projects_df.empty:
            raise ValueError("projects テーブルが読み込めません")
        
        # 予算事業IDをキーとして重複除去（最初のレコードを保持）
        self.master_data = projects_df.drop_duplicates(subset=['予算事業ID'], keep='first').copy()
        
        print(f"ベースマスター作成完了: {len(self.master_data):,}事業")
        self.statistics['base_projects'] = len(self.master_data)
        
        return self.master_data
    
    def merge_one_to_one_tables(self):
        """1:1関係のテーブルを結合"""
        print("\n" + "="*80)
        print("2. 基本情報テーブル結合（1:1関係）")
        print("="*80)
        
        original_columns = len(self.master_data.columns)
        
        for table_name, japanese_name in self.one_to_one_tables.items():
            df = self.load_feather_data(table_name)
            if df.empty:
                continue
            
            # 予算事業IDで重複除去
            df_unique = df.drop_duplicates(subset=['予算事業ID'], keep='first')
            
            # 共通列を除外して結合
            common_cols = set(self.master_data.columns) & set(df_unique.columns)
            merge_cols = [col for col in df_unique.columns if col not in common_cols or col == '予算事業ID']
            df_to_merge = df_unique[merge_cols]
            
            # 左結合
            before_count = len(self.master_data)
            self.master_data = pd.merge(
                self.master_data, 
                df_to_merge, 
                on='予算事業ID', 
                how='left',
                suffixes=('', f'_{table_name}')
            )
            
            added_cols = len(df_to_merge.columns) - 1  # 予算事業IDを除く
            print(f"    {japanese_name}: +{added_cols}列追加")
            
            if len(self.master_data) != before_count:
                print(f"    ⚠️  行数が変化しました: {before_count} → {len(self.master_data)}")
        
        new_columns = len(self.master_data.columns)
        print(f"\n基本情報結合完了: {original_columns}列 → {new_columns}列")
        self.statistics['after_basic_merge'] = {
            'rows': len(self.master_data),
            'columns': new_columns
        }
    
    def convert_to_json_records(self, df: pd.DataFrame, exclude_cols: List[str] = None) -> List[Dict]:
        """DataFrameをJSON形式のレコードリストに変換"""
        if exclude_cols is None:
            exclude_cols = ['予算事業ID', 'シート種別', '事業年度', '事業名']
        
        # 除外列以外を保持
        cols_to_keep = [col for col in df.columns if col not in exclude_cols]
        df_filtered = df[['予算事業ID'] + cols_to_keep].copy()
        
        # NaN値をNoneに変換
        df_filtered = df_filtered.where(pd.notnull(df_filtered), None)
        
        # レコードリストに変換
        records = []
        for _, row in df_filtered.iterrows():
            record = {col: row[col] for col in cols_to_keep if pd.notnull(row[col])}
            if record:  # 空でないレコードのみ追加
                records.append(record)
        
        return records
    
    def add_json_columns(self):
        """1:多関係のテーブルをJSON列として追加"""
        print("\n" + "="*80)
        print("3. 詳細情報テーブル追加（1:多関係 → JSON化）")
        print("="*80)
        
        for table_name, japanese_name in self.one_to_many_tables.items():
            print(f"\n処理中: {japanese_name} ({table_name})")
            
            df = self.load_feather_data(table_name)
            if df.empty:
                # 空の列を追加
                self.master_data[f'{table_name}_json'] = None
                self.master_data[f'{table_name}_count'] = 0
                continue
            
            # 予算事業IDでグループ化してJSON化
            json_data = {}
            count_data = {}
            
            for budget_id in self.master_data['予算事業ID'].unique():
                project_records = df[df['予算事業ID'] == budget_id]
                
                if len(project_records) > 0:
                    # JSON形式に変換
                    records = self.convert_to_json_records(project_records)
                    json_data[budget_id] = json.dumps(records, ensure_ascii=False) if records else None
                    count_data[budget_id] = len(records)
                else:
                    json_data[budget_id] = None
                    count_data[budget_id] = 0
            
            # マスターデータに追加
            self.master_data[f'{table_name}_json'] = self.master_data['予算事業ID'].map(json_data)
            self.master_data[f'{table_name}_count'] = self.master_data['予算事業ID'].map(count_data)
            
            # 統計情報
            total_records = len(df)
            projects_with_data = sum(1 for count in count_data.values() if count > 0)
            
            print(f"    総レコード数: {total_records:,}")
            print(f"    データ有り事業: {projects_with_data:,}/{len(self.master_data):,}")
            print(f"    平均レコード数/事業: {total_records/len(self.master_data):.1f}")
            
            self.statistics[table_name] = {
                'total_records': total_records,
                'projects_with_data': projects_with_data,
                'avg_records_per_project': total_records/len(self.master_data)
            }
    
    def add_summary_columns(self):
        """サマリー情報を追加"""
        print("\n" + "="*80)
        print("4. サマリー情報追加")
        print("="*80)
        
        # JSON列の総件数
        json_columns = [col for col in self.master_data.columns if col.endswith('_count')]
        self.master_data['total_related_records'] = self.master_data[json_columns].sum(axis=1)
        
        # データ有無フラグ
        for table_name in self.one_to_many_tables.keys():
            count_col = f'{table_name}_count'
            if count_col in self.master_data.columns:
                self.master_data[f'has_{table_name}'] = self.master_data[count_col] > 0
        
        # 基本統計
        print(f"  ✓ 関連レコード総数列追加")
        print(f"  ✓ データ有無フラグ追加")
        print(f"  ✓ 最終列数: {len(self.master_data.columns)}")
        
        self.statistics['final_structure'] = {
            'rows': len(self.master_data),
            'columns': len(self.master_data.columns),
            'json_columns': len([col for col in self.master_data.columns if col.endswith('_json')]),
            'count_columns': len([col for col in self.master_data.columns if col.endswith('_count')]),
            'flag_columns': len([col for col in self.master_data.columns if col.startswith('has_')])
        }
    
    def save_outputs(self):
        """結果を保存"""
        print("\n" + "="*80)
        print("5. 結果保存")
        print("="*80)
        
        # CSV保存
        csv_path = self.output_dir / "rs_project_master_with_details.csv"
        self.master_data.to_csv(csv_path, index=False, encoding='utf-8')
        print(f"  ✓ CSV保存: {csv_path}")
        
        # Feather保存
        feather_path = self.output_dir / "rs_project_master_with_details.feather"
        self.master_data.to_feather(feather_path)
        print(f"  ✓ Feather保存: {feather_path}")
        
        # 統計情報保存
        stats_path = self.output_dir / "project_master_statistics.json"
        with open(stats_path, 'w', encoding='utf-8') as f:
            json.dump(self.statistics, f, ensure_ascii=False, indent=2, default=str)
        print(f"  ✓ 統計情報保存: {stats_path}")
        
        # 列リスト保存
        columns_path = self.output_dir / "project_master_columns.txt"
        with open(columns_path, 'w', encoding='utf-8') as f:
            f.write("事業マスターリスト列構成\n")
            f.write("="*50 + "\n\n")
            
            f.write(f"総列数: {len(self.master_data.columns)}\n")
            f.write(f"総事業数: {len(self.master_data)}\n\n")
            
            # 列種別ごとに分類
            basic_cols = [col for col in self.master_data.columns 
                         if not (col.endswith('_json') or col.endswith('_count') or col.startswith('has_'))]
            json_cols = [col for col in self.master_data.columns if col.endswith('_json')]
            count_cols = [col for col in self.master_data.columns if col.endswith('_count')]
            flag_cols = [col for col in self.master_data.columns if col.startswith('has_')]
            
            f.write(f"基本情報列 ({len(basic_cols)}列):\n")
            for col in basic_cols:
                f.write(f"  - {col}\n")
            
            f.write(f"\nJSON詳細列 ({len(json_cols)}列):\n")
            for col in json_cols:
                f.write(f"  - {col}\n")
            
            f.write(f"\n件数列 ({len(count_cols)}列):\n")
            for col in count_cols:
                f.write(f"  - {col}\n")
            
            f.write(f"\nデータ有無フラグ ({len(flag_cols)}列):\n")
            for col in flag_cols:
                f.write(f"  - {col}\n")
        
        print(f"  ✓ 列リスト保存: {columns_path}")
        
        # サイズ情報
        csv_size = csv_path.stat().st_size
        feather_size = feather_path.stat().st_size
        print(f"\nファイルサイズ:")
        print(f"  CSV: {csv_size:,} bytes ({csv_size/1024/1024:.1f} MB)")
        print(f"  Feather: {feather_size:,} bytes ({feather_size/1024/1024:.1f} MB)")
        print(f"  圧縮率: {(1-feather_size/csv_size)*100:.1f}%")
    
    def generate_html_report(self):
        """HTMLレポート生成"""
        html_content = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <title>事業マスターリスト作成レポート</title>
    <style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
        .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
        h1 {{ color: #2c3e50; text-align: center; border-bottom: 3px solid #3498db; padding-bottom: 15px; }}
        .summary {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 8px; margin: 20px 0; }}
        .metric {{ display: inline-block; margin: 10px 20px; }}
        .metric-value {{ font-size: 2em; font-weight: bold; }}
        .metric-label {{ font-size: 0.9em; opacity: 0.9; }}
        table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        th {{ background: #34495e; color: white; padding: 12px; text-align: left; }}
        td {{ padding: 10px; border-bottom: 1px solid #ddd; }}
        tr:hover {{ background: #f8f9fa; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 事業マスターリスト作成レポート</h1>
        
        <div class="summary">
            <h2 style="color: white; margin-top: 0;">作成結果</h2>
            <div class="metric">
                <div class="metric-value">{self.statistics['final_structure']['rows']:,}</div>
                <div class="metric-label">事業数</div>
            </div>
            <div class="metric">
                <div class="metric-value">{self.statistics['final_structure']['columns']}</div>
                <div class="metric-label">総列数</div>
            </div>
            <div class="metric">
                <div class="metric-value">{self.statistics['final_structure']['json_columns']}</div>
                <div class="metric-label">JSON詳細列</div>
            </div>
        </div>
        
        <h2>📋 テーブル別データ保有状況</h2>
        <table>
            <tr>
                <th>テーブル名</th>
                <th>総レコード数</th>
                <th>データ有り事業数</th>
                <th>平均レコード/事業</th>
            </tr>
"""
        
        for table_name, stats in self.statistics.items():
            if isinstance(stats, dict) and 'total_records' in stats:
                html_content += f"""
            <tr>
                <td>{self.one_to_many_tables.get(table_name, table_name)}</td>
                <td>{stats['total_records']:,}</td>
                <td>{stats['projects_with_data']:,}</td>
                <td>{stats['avg_records_per_project']:.1f}</td>
            </tr>
"""
        
        html_content += """
        </table>
        
        <div style="text-align: center; margin-top: 40px; color: #7f8c8d; font-size: 0.9em;">
            事業マスターリスト作成レポート - RS Visualization System
        </div>
    </div>
</body>
</html>
"""
        
        report_path = self.output_dir / "project_master_report.html"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"  ✓ HTMLレポート保存: {report_path}")
    
    def run(self):
        """メイン処理を実行"""
        print("\n" + "="*80)
        print("事業マスターリスト作成開始")
        print("="*80)
        print("目標: 1行1事業形式 + JSON詳細保持")
        
        try:
            # 1. ベースマスター作成
            self.create_base_master()
            
            # 2. 基本情報テーブル結合
            self.merge_one_to_one_tables()
            
            # 3. 詳細情報をJSON化
            self.add_json_columns()
            
            # 4. サマリー情報追加
            self.add_summary_columns()
            
            # 5. 結果保存
            self.save_outputs()
            
            # 6. HTMLレポート生成
            self.generate_html_report()
            
            print("\n" + "="*80)
            print("事業マスターリスト作成完了！")
            print("="*80)
            print(f"結果: {len(self.master_data):,}事業 × {len(self.master_data.columns)}列")
            print(f"保存先: {self.output_dir}/")
            print("  - rs_project_master_with_details.csv")
            print("  - rs_project_master_with_details.feather")
            print("  - project_master_statistics.json")
            print("  - project_master_columns.txt")
            print("  - project_master_report.html")
            
        except Exception as e:
            print(f"\n❌ エラー: {e}")
            raise


def main():
    creator = ProjectMasterCreator()
    creator.run()


if __name__ == "__main__":
    main()