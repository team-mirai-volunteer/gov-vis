#!/usr/bin/env python3
"""
全カラムを保持したFeatherファイル作成
元CSVの全ての列を保持する
"""
import pandas as pd
import json
from pathlib import Path
from typing import Dict, List, Tuple
import time
import warnings
warnings.filterwarnings('ignore')


class FullFeatherConverter:
    """全カラム保持のFeatherコンバーター"""
    
    def __init__(self, extracted_dir: str = "data/extracted"):
        self.extracted_dir = Path(extracted_dir)
        self.output_dir = Path("data/full_feather")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # CSVファイルマッピング
        self.csv_files = {
            'organizations': {
                'path': '1-1_RS_2024_基本情報_組織情報/1-1_RS_2024_基本情報_組織情報.csv',
                'name': '組織情報',
                'category': '基本情報'
            },
            'projects': {
                'path': '1-2_RS_2024_基本情報_事業概要等/1-2_RS_2024_基本情報_事業概要等.csv',
                'name': '事業概要等',
                'category': '基本情報'
            },
            'policies_laws': {
                'path': '1-3_RS_2024_基本情報_政策・施策、法令等/1-3_RS_2024_基本情報_政策・施策、法令等.csv',
                'name': '政策・施策・法令等',
                'category': '基本情報'
            },
            'subsidies': {
                'path': '1-4_RS_2024_基本情報_補助率等/1-4_RS_2024_基本情報_補助率等.csv',
                'name': '補助率等',
                'category': '基本情報'
            },
            'related_projects': {
                'path': '1-5_RS_2024_基本情報_関連事業/1-5_RS_2024_基本情報_関連事業.csv',
                'name': '関連事業',
                'category': '基本情報'
            },
            'budget_summary': {
                'path': '2-1_RS_2024_予算・執行_サマリ/2-1_RS_2024_予算・執行_サマリ.csv',
                'name': '予算・執行サマリ',
                'category': '予算・執行'
            },
            'budget_items': {
                'path': '2-2_RS_2024_予算・執行_予算種別・歳出予算項目/2-2_RS_2024_予算・執行_予算種別・歳出予算項目.csv',
                'name': '予算種別・歳出予算項目',
                'category': '予算・執行'
            },
            'goals_performance': {
                'path': '3-1_RS_2024_効果発現経路_目標・実績/3-1_RS_2024_効果発現経路_目標・実績.csv',
                'name': '目標・実績',
                'category': '効果発現経路'
            },
            'goal_connections': {
                'path': '3-2_RS_2024_効果発現経路_目標のつながり/3-2_RS_2024_効果発現経路_目標のつながり.csv',
                'name': '目標のつながり',
                'category': '効果発現経路'
            },
            'evaluations': {
                'path': '4-1_RS_2024_点検・評価/4-1_RS_2024_点検・評価.csv',
                'name': '点検・評価',
                'category': '点検・評価'
            },
            'expenditure_info': {
                'path': '5-1_RS_2024_支出先_支出情報/5-1_RS_2024_支出先_支出情報.csv',
                'name': '支出情報',
                'category': '支出先'
            },
            'expenditure_connections': {
                'path': '5-2_RS_2024_支出先_支出ブロックのつながり/5-2_RS_2024_支出先_支出ブロックのつながり.csv',
                'name': '支出ブロックのつながり',
                'category': '支出先'
            },
            'expenditure_details': {
                'path': '5-3_RS_2024_支出先_費目・使途/5-3_RS_2024_支出先_費目・使途.csv',
                'name': '費目・使途',
                'category': '支出先'
            },
            'contracts': {
                'path': '5-4_RS_2024_支出先_国庫債務負担行為等による契約/5-4_RS_2024_支出先_国庫債務負担行為等による契約.csv',
                'name': '国庫債務負担行為等による契約',
                'category': '支出先'
            },
            'remarks': {
                'path': '6-1_RS_2024_その他備考/6-1_RS_2024_その他備考.csv',
                'name': 'その他備考',
                'category': 'その他'
            }
        }
        
        self.conversion_stats = {}
    
    def try_encodings(self, file_path: Path) -> Tuple[pd.DataFrame, str]:
        """複数のエンコーディングを試してCSVを読み込む"""
        encodings = ['utf-8', 'shift-jis', 'cp932', 'utf-8-sig', 'iso-2022-jp', 'euc-jp']
        
        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, encoding=encoding, low_memory=False)
                return df, encoding
            except (UnicodeDecodeError, UnicodeError):
                continue
            except Exception as e:
                print(f"    Error with {encoding}: {e}")
                continue
        
        raise ValueError(f"Could not read file with any encoding: {file_path}")
    
    def convert_csv_to_feather(self, table_name: str, csv_info: Dict) -> bool:
        """CSVファイルを全カラム保持でFeatherに変換"""
        csv_path = self.extracted_dir / csv_info['path']
        
        if not csv_path.exists():
            print(f"  Warning: {csv_path} not found")
            return False
        
        print(f"  Converting: {table_name} ({csv_info['name']})")
        
        try:
            # CSVファイルを読み込み（全カラム）
            df, encoding = self.try_encodings(csv_path)
            original_shape = df.shape
            
            # データ型の最適化
            # 数値列の最適化
            for col in df.columns:
                if df[col].dtype == 'object':
                    # 文字列として保持（数値変換はしない）
                    continue
                elif pd.api.types.is_numeric_dtype(df[col]):
                    # 数値列の最適化
                    if df[col].dtype == 'float64':
                        # NaNがある場合はfloat32に変換
                        if df[col].isna().any():
                            df[col] = pd.to_numeric(df[col], downcast='float')
                        else:
                            # NaNがない場合はintに変換可能かチェック
                            if df[col].notnull().all() and (df[col] % 1 == 0).all():
                                df[col] = pd.to_numeric(df[col], downcast='integer')
                            else:
                                df[col] = pd.to_numeric(df[col], downcast='float')
                    elif df[col].dtype == 'int64':
                        df[col] = pd.to_numeric(df[col], downcast='integer')
            
            # Featherファイルとして保存
            feather_path = self.output_dir / f"{table_name}.feather"
            df.to_feather(feather_path)
            
            # 統計を記録
            self.conversion_stats[table_name] = {
                'original_shape': original_shape,
                'final_shape': df.shape,
                'encoding': encoding,
                'category': csv_info['category'],
                'japanese_name': csv_info['name'],
                'columns': list(df.columns),
                'file_size_csv': csv_path.stat().st_size,
                'file_size_feather': feather_path.stat().st_size
            }
            
            print(f"    ✓ {original_shape[0]:,} rows × {original_shape[1]} columns → Feather")
            print(f"    Encoding: {encoding}, Size: {csv_path.stat().st_size:,} → {feather_path.stat().st_size:,} bytes")
            
            return True
            
        except Exception as e:
            print(f"    ✗ Error converting {table_name}: {e}")
            return False
    
    def save_metadata(self):
        """メタデータと統計情報を保存"""
        print("\nSaving metadata and statistics...")
        
        # 全体統計
        total_files = len(self.conversion_stats)
        successful_conversions = len([s for s in self.conversion_stats.values() if s['final_shape'][0] > 0])
        total_rows = sum(s['final_shape'][0] for s in self.conversion_stats.values())
        total_columns = sum(s['final_shape'][1] for s in self.conversion_stats.values())
        total_csv_size = sum(s['file_size_csv'] for s in self.conversion_stats.values())
        total_feather_size = sum(s['file_size_feather'] for s in self.conversion_stats.values())
        
        # AI検索フィールド設定
        ai_search_fields = {
            'projects': ['事業名', '事業の目的', '事業の概要', '現状・課題'],
            'expenditure_info': ['支出先名', '法人番号'],
            'expenditure_details': ['費目', '使途', '契約概要'],
            'goals_performance': ['アクティビティ／活動目標／成果目標', '活動指標／成果指標'],
            'expenditure_connections': ['支出先の支出先ブロック名', '資金の流れの補足情報'],
            'contracts': ['契約先名（国庫債務負担行為等による契約）', '契約概要（契約名）（国庫債務負担行為等による契約）']
        }
        
        # メタデータ作成
        metadata = {
            'conversion_info': {
                'timestamp': pd.Timestamp.now().isoformat(),
                'total_files': total_files,
                'successful_conversions': successful_conversions,
                'total_rows': total_rows,
                'total_columns': total_columns,
                'compression_ratio': round((1 - total_feather_size / total_csv_size) * 100, 1) if total_csv_size > 0 else 0,
                'size_reduction': f"{total_csv_size:,} → {total_feather_size:,} bytes"
            },
            'table_details': self.conversion_stats,
            'ai_search_fields': ai_search_fields,
            'category_summary': {}
        }
        
        # カテゴリ別サマリー
        categories = {}
        for table_name, stats in self.conversion_stats.items():
            category = stats['category']
            if category not in categories:
                categories[category] = {'tables': 0, 'rows': 0, 'columns': 0}
            categories[category]['tables'] += 1
            categories[category]['rows'] += stats['final_shape'][0]
            categories[category]['columns'] += stats['final_shape'][1]
        
        metadata['category_summary'] = categories
        
        # JSON保存
        metadata_path = self.output_dir / 'full_feather_metadata.json'
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"  Metadata saved: {metadata_path}")
        
        # カラムマッピング保存
        column_mapping_path = self.output_dir / 'column_mapping.json'
        column_mapping = {}
        for table_name, stats in self.conversion_stats.items():
            column_mapping[table_name] = {
                'japanese_name': stats['japanese_name'],
                'category': stats['category'],
                'columns': stats['columns'],
                'column_count': len(stats['columns'])
            }
        
        with open(column_mapping_path, 'w', encoding='utf-8') as f:
            json.dump(column_mapping, f, ensure_ascii=False, indent=2)
        
        print(f"  Column mapping saved: {column_mapping_path}")
        
        return metadata
    
    def generate_html_report(self, metadata: Dict):
        """HTMLレポート生成"""
        html_content = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <title>全カラム保持 Feather変換レポート</title>
    <style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 20px; background: #f8f9fa; }}
        .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
        h1 {{ color: #2c3e50; text-align: center; margin-bottom: 30px; }}
        .summary {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 25px; border-radius: 8px; margin: 20px 0; }}
        .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin: 20px 0; }}
        .metric {{ text-align: center; background: rgba(255,255,255,0.1); padding: 15px; border-radius: 5px; }}
        .metric-value {{ font-size: 2em; font-weight: bold; }}
        .metric-label {{ font-size: 0.9em; opacity: 0.9; }}
        table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        th {{ background: #34495e; color: white; padding: 12px; text-align: left; }}
        td {{ padding: 10px; border-bottom: 1px solid #ecf0f1; }}
        tr:hover {{ background: #f8f9fa; }}
        .success {{ color: #27ae60; font-weight: bold; }}
        .category {{ background: #e3f2fd; padding: 15px; margin: 10px 0; border-radius: 5px; }}
        .footer {{ text-align: center; margin-top: 30px; color: #7f8c8d; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 全カラム保持 Feather変換レポート</h1>
        
        <div class="summary">
            <h2 style="color: white; margin-top: 0;">変換サマリー</h2>
            <div class="grid">
                <div class="metric">
                    <div class="metric-label">変換ファイル数</div>
                    <div class="metric-value">{metadata['conversion_info']['successful_conversions']}</div>
                </div>
                <div class="metric">
                    <div class="metric-label">総レコード数</div>
                    <div class="metric-value">{metadata['conversion_info']['total_rows']:,}</div>
                </div>
                <div class="metric">
                    <div class="metric-label">総カラム数</div>
                    <div class="metric-value success">{metadata['conversion_info']['total_columns']}</div>
                </div>
                <div class="metric">
                    <div class="metric-label">圧縮率</div>
                    <div class="metric-value">{metadata['conversion_info']['compression_ratio']}%</div>
                </div>
            </div>
        </div>
        
        <h2>📁 カテゴリ別統計</h2>
        <div class="grid">"""
        
        for category, stats in metadata['category_summary'].items():
            html_content += f"""
            <div class="category">
                <h3>{category}</h3>
                <p>テーブル数: {stats['tables']}</p>
                <p>レコード数: {stats['rows']:,}</p>
                <p>カラム数: {stats['columns']}</p>
            </div>"""
        
        html_content += f"""
        </div>
        
        <h2>📋 テーブル詳細</h2>
        <table>
            <thead>
                <tr>
                    <th>テーブル名</th>
                    <th>日本語名</th>
                    <th>カテゴリ</th>
                    <th>レコード数</th>
                    <th>カラム数</th>
                    <th>サイズ削減</th>
                </tr>
            </thead>
            <tbody>"""
        
        for table_name, stats in metadata['table_details'].items():
            size_reduction = round((1 - stats['file_size_feather'] / stats['file_size_csv']) * 100, 1) if stats['file_size_csv'] > 0 else 0
            html_content += f"""
                <tr>
                    <td><strong>{table_name}</strong></td>
                    <td>{stats['japanese_name']}</td>
                    <td>{stats['category']}</td>
                    <td style="text-align: right;">{stats['final_shape'][0]:,}</td>
                    <td style="text-align: right;" class="success">{stats['final_shape'][1]}</td>
                    <td style="text-align: right;">{size_reduction}%</td>
                </tr>"""
        
        html_content += f"""
            </tbody>
        </table>
        
        <h2>🎯 AI検索対象フィールド</h2>
        <table>
            <thead>
                <tr>
                    <th>テーブル</th>
                    <th>検索対象フィールド</th>
                </tr>
            </thead>
            <tbody>"""
        
        for table, fields in metadata['ai_search_fields'].items():
            html_content += f"""
                <tr>
                    <td><strong>{table}</strong></td>
                    <td>{', '.join(fields)}</td>
                </tr>"""
        
        html_content += f"""
            </tbody>
        </table>
        
        <div class="footer">
            <p>Generated by Full Feather Converter</p>
            <p>全 {metadata['conversion_info']['total_columns']} カラムを完全保持</p>
        </div>
    </div>
</body>
</html>"""
        
        html_path = self.output_dir / 'full_feather_report.html'
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"  HTML report saved: {html_path}")
    
    def run(self):
        """全カラム保持Feather変換を実行"""
        print("="*80)
        print("🔄 全カラム保持 Feather変換")
        print("   元CSVファイルの全てのカラムを保持")
        print("="*80)
        
        start_time = time.time()
        
        # 全CSVファイルを変換
        successful_conversions = 0
        
        for table_name, csv_info in self.csv_files.items():
            if self.convert_csv_to_feather(table_name, csv_info):
                successful_conversions += 1
        
        if successful_conversions == 0:
            print("No files were successfully converted.")
            return None
        
        # メタデータ保存
        metadata = self.save_metadata()
        
        # HTMLレポート生成
        self.generate_html_report(metadata)
        
        elapsed = time.time() - start_time
        
        print(f"\n{'='*80}")
        print("✅ 全カラム保持 Feather変換完了!")
        print(f"{'='*80}")
        print(f"📁 変換ファイル数: {successful_conversions}/{len(self.csv_files)}")
        print(f"📊 総レコード数: {metadata['conversion_info']['total_rows']:,}")
        print(f"📋 総カラム数: {metadata['conversion_info']['total_columns']}")
        print(f"💾 サイズ削減: {metadata['conversion_info']['compression_ratio']}%")
        print(f"⏱️  実行時間: {elapsed:.1f}秒")
        print(f"📁 出力先: {self.output_dir}")
        print(f"{'='*80}")
        
        return metadata


if __name__ == "__main__":
    converter = FullFeatherConverter()
    converter.run()