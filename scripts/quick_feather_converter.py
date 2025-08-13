#!/usr/bin/env python3
"""
高速Feather変換スクリプト（簡略版）
最重要テーブルのみを効率的に変換
"""
import pandas as pd
import numpy as np
from pathlib import Path
import json
import time


class QuickFeatherConverter:
    """高速Feather変換クラス"""
    
    def __init__(self, extracted_dir: str = "data/extracted"):
        self.extracted_dir = Path(extracted_dir)
        self.output_dir = Path("data/normalized_feather")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # 重要テーブルのみに絞る
        self.priority_tables = {
            'projects': {
                'source': '1-2_RS_2024_基本情報_事業概要等.csv',
                'type': 'master',
                'key_fields': ['予算事業ID', '事業名', '府省庁', '事業の目的', '事業の概要', '現状・課題']
            },
            'expenditure_info': {
                'source': '5-1_RS_2024_支出先_支出情報.csv', 
                'type': 'detail',
                'key_fields': ['予算事業ID', '支出先名', '契約概要', '費目', '使途']
            },
            'goals_performance': {
                'source': '3-1_RS_2024_効果発現経路_目標・実績.csv',
                'type': 'detail', 
                'key_fields': ['予算事業ID', 'アクティビティ／活動目標／成果目標', '活動指標／成果指標']
            },
            'expenditure_connections': {
                'source': '5-2_RS_2024_支出先_支出ブロックのつながり.csv',
                'type': 'detail',
                'key_fields': ['予算事業ID', '支出先の支出先ブロック名', '資金の流れの補足情報']
            },
            'contracts': {
                'source': '5-4_RS_2024_支出先_国庫債務負担行為等による契約.csv',
                'type': 'detail',
                'key_fields': ['予算事業ID', '契約先名（国庫債務負担行為等による契約）', '契約概要（契約名）（国庫債務負担行為等による契約）']
            }
        }
        
        self.conversion_stats = {}
    
    def find_csv_file(self, filename: str) -> Path:
        """CSVファイルを検索"""
        for csv_dir in self.extracted_dir.iterdir():
            if csv_dir.is_dir() and filename.replace('.csv', '') in csv_dir.name:
                csv_files = list(csv_dir.glob("*.csv"))
                if csv_files:
                    return csv_files[0]
        raise FileNotFoundError(f"CSV file not found: {filename}")
    
    def load_csv_optimized(self, filename: str, key_fields: list) -> pd.DataFrame:
        """最適化されたCSV読み込み"""
        csv_path = self.find_csv_file(filename)
        print(f"  Loading: {csv_path.name}")
        
        # チャンクサイズで読み込み
        chunk_size = 50000
        chunks = []
        
        try:
            for chunk in pd.read_csv(csv_path, encoding='utf-8', dtype=str, chunksize=chunk_size):
                # 予算事業IDフィルタリング
                if '予算事業ID' in chunk.columns:
                    chunk = chunk[pd.to_numeric(chunk['予算事業ID'], errors='coerce').notna()]
                    chunk['予算事業ID'] = chunk['予算事業ID'].astype(int)
                
                # 必要フィールドのみ選択
                available_fields = [f for f in key_fields if f in chunk.columns]
                if available_fields:
                    chunk_filtered = chunk[available_fields].copy()
                    
                    # 空行除去
                    text_cols = [c for c in chunk_filtered.columns if c != '予算事業ID']
                    if text_cols:
                        mask = chunk_filtered[text_cols].fillna('').astype(str).apply(
                            lambda row: row.str.strip().ne('').any(), axis=1
                        )
                        chunk_filtered = chunk_filtered[mask]
                    
                    if len(chunk_filtered) > 0:
                        chunks.append(chunk_filtered)
            
            if chunks:
                df = pd.concat(chunks, ignore_index=True)
            else:
                df = pd.DataFrame(columns=key_fields)
                
        except UnicodeDecodeError:
            # フォールバック：shift_jis
            chunks = []
            for chunk in pd.read_csv(csv_path, encoding='shift_jis', dtype=str, chunksize=chunk_size):
                if '予算事業ID' in chunk.columns:
                    chunk = chunk[pd.to_numeric(chunk['予算事業ID'], errors='coerce').notna()]
                    chunk['予算事業ID'] = chunk['予算事業ID'].astype(int)
                
                available_fields = [f for f in key_fields if f in chunk.columns]
                if available_fields:
                    chunk_filtered = chunk[available_fields].copy()
                    text_cols = [c for c in chunk_filtered.columns if c != '予算事業ID']
                    if text_cols:
                        mask = chunk_filtered[text_cols].fillna('').astype(str).apply(
                            lambda row: row.str.strip().ne('').any(), axis=1
                        )
                        chunk_filtered = chunk_filtered[mask]
                    if len(chunk_filtered) > 0:
                        chunks.append(chunk_filtered)
            
            df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame(columns=key_fields)
        
        # データクリーニング
        for col in df.columns:
            if col != '予算事業ID':
                df[col] = df[col].fillna('').astype(str)
        
        print(f"    Loaded: {len(df):,} records, {len(df.columns)} columns")
        return df
    
    def create_projects_table(self) -> pd.DataFrame:
        """プロジェクトマスターテーブル作成"""
        print("\nCreating projects master table...")
        
        config = self.priority_tables['projects']
        df = self.load_csv_optimized(config['source'], config['key_fields'])
        
        # 重複除去（1事業1レコード）
        if len(df) > df['予算事業ID'].nunique():
            print(f"    Deduplicating: {len(df)} -> {df['予算事業ID'].nunique()}")
            df = df.groupby('予算事業ID').first().reset_index()
        
        # インデックス設定
        df = df.set_index('予算事業ID').sort_index()
        
        # 統計記録
        self.conversion_stats['projects'] = {
            'records': len(df),
            'columns': len(df.columns),
            'unique_projects': len(df),
            'memory_mb': df.memory_usage(deep=True).sum() / (1024 * 1024)
        }
        
        print(f"    Created: {len(df):,} projects")
        return df
    
    def create_detail_table(self, table_name: str) -> pd.DataFrame:
        """詳細テーブル作成"""
        print(f"\nCreating {table_name} table...")
        
        config = self.priority_tables[table_name]
        df = self.load_csv_optimized(config['source'], config['key_fields'])
        
        # 統計記録
        self.conversion_stats[table_name] = {
            'records': len(df),
            'columns': len(df.columns),
            'unique_projects': df['予算事業ID'].nunique() if len(df) > 0 else 0,
            'avg_records_per_project': len(df) / df['予算事業ID'].nunique() if len(df) > 0 and df['予算事業ID'].nunique() > 0 else 0,
            'memory_mb': df.memory_usage(deep=True).sum() / (1024 * 1024)
        }
        
        print(f"    Created: {len(df):,} records, {df['予算事業ID'].nunique():,} projects")
        return df
    
    def optimize_and_save(self, df: pd.DataFrame, table_name: str):
        """最適化してFeather保存"""
        print(f"  Optimizing and saving {table_name}...")
        
        # カテゴリ型最適化
        for col in df.columns:
            if df[col].dtype == 'object' and col != '予算事業ID':
                unique_ratio = df[col].nunique() / len(df) if len(df) > 0 else 0
                if unique_ratio < 0.3 and df[col].nunique() < 500:
                    df[col] = df[col].astype('category')
        
        # Feather保存
        output_path = self.output_dir / f"{table_name}.feather"
        df.reset_index().to_feather(output_path)
        
        # ファイルサイズ記録
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        self.conversion_stats[table_name]['feather_size_mb'] = file_size_mb
        
        print(f"    Saved: {output_path.name} ({file_size_mb:.2f} MB)")
    
    def create_ai_search_metadata(self):
        """AI検索用メタデータ作成"""
        metadata = {
            'creation_time': pd.Timestamp.now().isoformat(),
            'tables': self.conversion_stats,
            'ai_search_fields': {
                'projects': ['事業名', '事業の目的', '事業の概要', '現状・課題'],
                'expenditure_info': ['支出先名', '契約概要', '費目', '使途'],
                'goals_performance': ['アクティビティ／活動目標／成果目標', '活動指標／成果指標'],
                'expenditure_connections': ['支出先の支出先ブロック名', '資金の流れの補足情報'],
                'contracts': ['契約先名（国庫債務負担行為等による契約）', '契約概要（契約名）（国庫債務負担行為等による契約）']
            },
            'total_records': sum(stats['records'] for stats in self.conversion_stats.values()),
            'total_projects': self.conversion_stats['projects']['unique_projects']
        }
        
        metadata_path = self.output_dir / 'ai_search_metadata.json'
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"\nAI search metadata saved: {metadata_path}")
        return metadata
    
    def run(self):
        """高速変換パイプライン実行"""
        print("=" * 60)
        print("高速Feather変換（AI検索特化）")
        print("=" * 60)
        
        start_time = time.time()
        
        try:
            # 1. プロジェクトマスターテーブル
            projects_df = self.create_projects_table()
            self.optimize_and_save(projects_df, 'projects')
            
            # 2. 詳細テーブル（AI検索に重要なもの）
            for table_name in ['expenditure_info', 'goals_performance', 'expenditure_connections', 'contracts']:
                try:
                    detail_df = self.create_detail_table(table_name)
                    if len(detail_df) > 0:
                        self.optimize_and_save(detail_df, table_name)
                    else:
                        print(f"    Warning: {table_name} is empty")
                except Exception as e:
                    print(f"    Error with {table_name}: {e}")
                    continue
            
            # 3. AI検索用メタデータ
            metadata = self.create_ai_search_metadata()
            
            elapsed = time.time() - start_time
            
            # 結果サマリー
            total_records = sum(stats['records'] for stats in self.conversion_stats.values())
            total_size = sum(stats.get('feather_size_mb', 0) for stats in self.conversion_stats.values())
            
            print(f"\n{'='*60}")
            print("変換完了!")
            print(f"{'='*60}")
            print(f"実行時間: {elapsed:.1f}秒")
            print(f"テーブル数: {len(self.conversion_stats)}")
            print(f"総レコード数: {total_records:,}")
            print(f"総サイズ: {total_size:.1f} MB")
            print(f"出力先: {self.output_dir}")
            print(f"{'='*60}")
            
        except Exception as e:
            print(f"Error during conversion: {e}")
            raise


if __name__ == "__main__":
    converter = QuickFeatherConverter()
    converter.run()