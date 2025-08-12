#!/usr/bin/env python3
"""
RSシステムデータの予算事業ID・年度マージのテスト
"""
import pandas as pd
from pathlib import Path


def analyze_merge_keys():
    """各ファイルのマージキーを分析"""
    extracted_dir = Path("data/extracted")
    
    print("=== マージキー分析 ===")
    
    all_files = []
    key_stats = {}
    
    for extract_dir in extracted_dir.iterdir():
        if extract_dir.is_dir():
            for csv_file in extract_dir.glob('**/*.csv'):
                try:
                    df = pd.read_csv(csv_file, encoding='utf-8')
                    
                    # マージキーの存在確認
                    has_budget_id = '予算事業ID' in df.columns
                    has_year = '事業年度' in df.columns
                    
                    if has_budget_id and has_year:
                        # ユニークな事業IDと年度の組み合わせ
                        unique_keys = df[['予算事業ID', '事業年度']].drop_duplicates()
                        unique_count = len(unique_keys)
                        
                        file_info = {
                            'filename': csv_file.name,
                            'total_rows': len(df),
                            'unique_keys': unique_count,
                            'has_both_keys': True,
                            'sample_ids': sorted(df['予算事業ID'].unique())[:5]  # 最初の5個のID
                        }
                        
                        key_stats[csv_file.stem] = unique_count
                        
                    else:
                        file_info = {
                            'filename': csv_file.name,
                            'total_rows': len(df),
                            'unique_keys': 0,
                            'has_both_keys': False,
                            'missing_keys': []
                        }
                        
                        if not has_budget_id:
                            file_info['missing_keys'].append('予算事業ID')
                        if not has_year:
                            file_info['missing_keys'].append('事業年度')
                    
                    all_files.append(file_info)
                    
                except Exception as e:
                    print(f"Error reading {csv_file.name}: {e}")
    
    # 結果表示
    print(f"\n総ファイル数: {len(all_files)}")
    
    mergeable_files = [f for f in all_files if f['has_both_keys']]
    print(f"マージ可能ファイル数: {len(mergeable_files)}")
    
    if mergeable_files:
        print("\n【マージ可能ファイル】")
        for file_info in mergeable_files:
            print(f"  {file_info['filename']}")
            print(f"    総行数: {file_info['total_rows']:,}")
            print(f"    ユニークキー数: {file_info['unique_keys']:,}")
            print(f"    サンプルID: {file_info['sample_ids']}")
        
        # 共通の予算事業IDを分析
        print("\n【共通予算事業ID分析】")
        all_budget_ids = set()
        file_budget_ids = {}
        
        for extract_dir in extracted_dir.iterdir():
            if extract_dir.is_dir():
                for csv_file in extract_dir.glob('**/*.csv'):
                    try:
                        df = pd.read_csv(csv_file, encoding='utf-8')
                        if '予算事業ID' in df.columns:
                            ids = set(df['予算事業ID'].unique())
                            file_budget_ids[csv_file.stem] = ids
                            all_budget_ids.update(ids)
                    except:
                        pass
        
        print(f"全体の予算事業ID数: {len(all_budget_ids)}")
        
        # ファイル間の重複分析
        if len(file_budget_ids) >= 2:
            file_names = list(file_budget_ids.keys())
            common_ids = file_budget_ids[file_names[0]]
            
            for name in file_names[1:]:
                common_ids = common_ids.intersection(file_budget_ids[name])
            
            print(f"全ファイル共通の予算事業ID数: {len(common_ids)}")
            if len(common_ids) > 0:
                print(f"共通IDサンプル: {sorted(list(common_ids))[:10]}")
    
    non_mergeable = [f for f in all_files if not f['has_both_keys']]
    if non_mergeable:
        print("\n【マージ不可ファイル】")
        for file_info in non_mergeable:
            print(f"  {file_info['filename']}")
            print(f"    欠損キー: {file_info.get('missing_keys', [])}")
    
    return key_stats


def test_actual_merge():
    """実際のマージテスト"""
    print("\n=== 実際のマージテスト ===")
    
    extracted_dir = Path("data/extracted")
    
    # マージ可能なファイルを読み込み
    dataframes = {}
    
    for extract_dir in extracted_dir.iterdir():
        if extract_dir.is_dir():
            for csv_file in extract_dir.glob('**/*.csv'):
                try:
                    df = pd.read_csv(csv_file, encoding='utf-8')
                    
                    if '予算事業ID' in df.columns and '事業年度' in df.columns:
                        dataframes[csv_file.stem] = df
                        
                except Exception as e:
                    print(f"Error reading {csv_file.name}: {e}")
    
    if len(dataframes) == 0:
        print("マージ可能なファイルがありません")
        return
    
    print(f"マージ対象ファイル数: {len(dataframes)}")
    
    # マージ実行
    dfs = list(dataframes.values())
    merged_df = dfs[0].copy()
    
    print(f"初期データフレーム: {len(merged_df)} rows")
    
    for i, df in enumerate(dfs[1:], 1):
        print(f"\nマージ {i}: {len(df)} rows を結合中...")
        
        # 共通カラムを確認
        common_cols = ['予算事業ID', '事業年度']
        before_rows = len(merged_df)
        
        merged_df = pd.merge(
            merged_df, 
            df, 
            on=common_cols, 
            how='outer',
            suffixes=('', f'_file{i}')
        )
        
        after_rows = len(merged_df)
        print(f"  結合後: {after_rows} rows (増加: {after_rows - before_rows})")
    
    print(f"\n【最終結果】")
    print(f"マージ後の総行数: {len(merged_df):,}")
    print(f"マージ後の総列数: {len(merged_df.columns):,}")
    print(f"ユニークな予算事業ID数: {merged_df['予算事業ID'].nunique():,}")
    
    # 一部データを保存
    output_path = Path("data/processed/merged_by_budget_id.csv")
    merged_df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"結果を保存: {output_path}")
    
    return len(merged_df)


if __name__ == "__main__":
    # 1. マージキー分析
    analyze_merge_keys()
    
    # 2. 実際のマージテスト
    record_count = test_actual_merge()
    
    print(f"\n予算事業ID・年度でマージした場合のレコード数: {record_count:,}")