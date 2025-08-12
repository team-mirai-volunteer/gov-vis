#!/usr/bin/env python3
"""
軽量版：マージキー分析
"""
import pandas as pd
from pathlib import Path


def quick_analysis():
    """軽量な分析"""
    extracted_dir = Path("data/extracted")
    
    print("=== 軽量マージ分析 ===")
    
    files_with_keys = []
    unique_budget_ids = set()
    
    # 各ファイルを個別に分析
    for extract_dir in extracted_dir.iterdir():
        if extract_dir.is_dir():
            for csv_file in extract_dir.glob('**/*.csv'):
                try:
                    # 最初の1000行だけ読み込み
                    df_sample = pd.read_csv(csv_file, encoding='utf-8', nrows=1000)
                    
                    if '予算事業ID' in df_sample.columns and '事業年度' in df_sample.columns:
                        # 全体のサイズを取得（行数のみ）
                        total_rows = sum(1 for _ in open(csv_file, 'r', encoding='utf-8')) - 1
                        
                        # ユニークIDの概算
                        sample_unique = df_sample['予算事業ID'].nunique()
                        sample_ids = set(df_sample['予算事業ID'].unique())
                        
                        unique_budget_ids.update(sample_ids)
                        
                        files_with_keys.append({
                            'filename': csv_file.name,
                            'total_rows': total_rows,
                            'sample_unique_ids': sample_unique,
                            'sample_size': len(df_sample)
                        })
                        
                        print(f"{csv_file.name}")
                        print(f"  総行数: {total_rows:,}")
                        print(f"  サンプル(1000行)のユニークID数: {sample_unique}")
                        
                except Exception as e:
                    print(f"エラー {csv_file.name}: {e}")
    
    print(f"\n【サマリー】")
    print(f"マージ可能ファイル数: {len(files_with_keys)}")
    print(f"サンプルから見つかったユニークID数: {len(unique_budget_ids)}")
    
    # 推定計算
    total_sample_rows = sum(f['total_rows'] for f in files_with_keys)
    print(f"マージ対象の総行数: {total_sample_rows:,}")
    
    # マージ後のレコード数を推定
    if len(files_with_keys) > 0:
        # 最も保守的な推定：ユニークIDの数
        min_estimate = len(unique_budget_ids)
        
        # 最も楽観的な推定：最大ファイルの行数
        max_estimate = max(f['total_rows'] for f in files_with_keys)
        
        print(f"\n【マージ後レコード数の推定】")
        print(f"最小推定（全ID共通の場合）: {min_estimate:,} レコード")
        print(f"最大推定（IDが全く重複しない場合）: {max_estimate:,} レコード")
        
        # より現実的な推定
        avg_rows = total_sample_rows // len(files_with_keys)
        realistic_estimate = len(unique_budget_ids) * 2  # 重複があると仮定
        
        print(f"現実的推定: {realistic_estimate:,} レコード")


if __name__ == "__main__":
    quick_analysis()