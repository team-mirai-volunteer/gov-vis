#!/usr/bin/env python3
"""
Feather形式データを使った高速AI検索システム
正規化されたテーブル構造で包括的なAI関連事業検索
"""
import pandas as pd
import json
import re
from pathlib import Path
from typing import Dict, List, Set, Any
from collections import defaultdict, Counter
import time


class FeatherAISearcher:
    """Feather形式データでのAI検索クラス"""
    
    def __init__(self, feather_dir: str = "data/normalized_feather"):
        self.feather_dir = Path(feather_dir)
        self.output_dir = Path("data/ai_analysis_feather")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # AI関連用語定義（包括的）
        self.ai_broad_terms = [
            # 基本AI用語
            r'\bAI\b', r'\bA\.I\.\b', r'人工知能', r'じんこうちのう',
            
            # 機械学習関連
            r'機械学習', r'きかいがくしゅう', r'マシンラーニング', r'\bML\b',
            r'深層学習', r'しんそうがくしゅう', r'ディープラーニング', r'Deep Learning',
            r'ニューラルネットワーク', r'Neural Network', r'\bNN\b',
            
            # 自然言語処理
            r'自然言語処理', r'しぜんげんごしょり', r'\bNLP\b', r'Natural Language Processing',
            r'テキストマイニング', r'Text Mining',
            
            # データ分析・予測
            r'ビッグデータ', r'Big Data', r'ビッグ・データ',
            r'データマイニング', r'Data Mining', r'データ解析', r'データ分析',
            r'予測分析', r'予測モデル', r'アルゴリズム', r'Algorithm',
            r'統計的学習', r'パターン認識',
            
            # 自動化・ロボティクス
            r'RPA', r'Robotic Process Automation', r'ロボティック・プロセス・オートメーション',
            r'自動化', r'じどうか', r'オートメーション', r'Automation',
            r'ロボット', r'Robot', r'ロボティクス', r'Robotics',
            
            # IoT・スマート技術
            r'IoT', r'Internet of Things', r'モノのインターネット',
            r'スマート', r'Smart', r'インテリジェント', r'Intelligent',
            r'センサー', r'Sensor', r'センシング',
            
            # DX・デジタル変革
            r'DX', r'デジタルトランスフォーメーション', r'Digital Transformation', 
            r'デジタル変革', r'デジタル化',
            
            # AI応用分野
            r'画像認識', r'がぞうにんしき', r'Image Recognition', r'コンピュータビジョン', r'Computer Vision',
            r'音声認識', r'おんせいにんしき', r'Voice Recognition', r'Speech Recognition',
            r'チャットボット', r'Chatbot', r'ボット', r'\bBot\b',
            r'レコメンド', r'Recommendation', r'推薦システム',
            
            # 最新AI技術
            r'生成AI', r'Generative AI', r'ChatGPT', r'GPT', r'LLM', r'大規模言語モデル',
            r'Transformer', r'BERT', r'機械翻訳', r'Machine Translation'
        ]
        
        self.ai_only_terms = [r'\bAI\b', r'\bA\.I\.\b']
        
        # テーブル・検索フィールド設定
        self.search_config = {}
        self.tables_data = {}
        self.load_metadata()
    
    def load_metadata(self):
        """メタデータを読み込み検索設定を初期化"""
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
        """Featherテーブルを読み込み"""
        print("Loading Feather tables...")
        
        for table_name in self.search_config.keys():
            feather_path = self.feather_dir / f"{table_name}.feather"
            if feather_path.exists():
                print(f"  Loading: {table_name}")
                try:
                    df = pd.read_feather(feather_path)
                    self.tables_data[table_name] = df
                    print(f"    Records: {len(df):,}, Columns: {len(df.columns)}")
                except Exception as e:
                    print(f"    Error loading {table_name}: {e}")
            else:
                print(f"  Warning: {feather_path} not found")
        
        print(f"Loaded {len(self.tables_data)} tables")
    
    def compile_search_patterns(self, terms: List[str]) -> List[re.Pattern]:
        """検索パターンをコンパイル（高速化）"""
        compiled_patterns = []
        for term in terms:
            try:
                pattern = re.compile(term, re.IGNORECASE)
                compiled_patterns.append((term, pattern))
            except re.error as e:
                print(f"Warning: Invalid regex pattern '{term}': {e}")
        return compiled_patterns
    
    def search_text_with_patterns(self, text: str, compiled_patterns: List[tuple]) -> List[str]:
        """コンパイル済みパターンでテキスト検索"""
        if not text or pd.isna(text):
            return []
        
        text_str = str(text)
        found_terms = []
        
        for term_name, pattern in compiled_patterns:
            if pattern.search(text_str):
                found_terms.append(term_name)
        
        return found_terms
    
    def search_table_for_ai(self, table_name: str, df: pd.DataFrame, compiled_patterns: List[tuple]) -> Dict[int, Dict]:
        """テーブル内でAI関連用語を検索"""
        print(f"  Searching in {table_name}...")
        start_time = time.time()
        
        search_fields = self.search_config.get(table_name, [])
        available_fields = [f for f in search_fields if f in df.columns]
        
        if not available_fields:
            print(f"    No searchable fields in {table_name}")
            return {}
        
        results = {}
        processed_records = 0
        
        # 予算事業IDでグループ化して検索
        if '予算事業ID' in df.columns:
            for project_id, group in df.groupby('予算事業ID'):
                project_matches = {}
                total_matches = 0
                all_found_terms = set()
                
                # グループ内の各レコードを検索
                for idx, record in group.iterrows():
                    for field in available_fields:
                        text = record.get(field, '')
                        found_terms = self.search_text_with_patterns(text, compiled_patterns)
                        
                        if found_terms:
                            if field not in project_matches:
                                project_matches[field] = []
                            
                            project_matches[field].append({
                                'text': str(text)[:300],  # 最初の300文字
                                'found_terms': found_terms,
                                'record_index': idx
                            })
                            
                            total_matches += len(found_terms)
                            all_found_terms.update(found_terms)
                
                if project_matches:
                    results[int(project_id)] = {
                        'project_id': int(project_id),
                        'table_name': table_name,
                        'matches': project_matches,
                        'total_matches': total_matches,
                        'matched_fields': list(project_matches.keys()),
                        'all_found_terms': list(all_found_terms),
                        'record_count': len(group)
                    }
                
                processed_records += len(group)
        
        elapsed = time.time() - start_time
        print(f"    Found {len(results)} projects with AI terms in {elapsed:.2f}s")
        
        return results
    
    def comprehensive_ai_search(self, compiled_patterns: List[tuple]) -> Dict[int, Dict]:
        """全テーブルを対象とした包括的AI検索"""
        print("\nComprehensive AI search across all tables...")
        
        all_results = defaultdict(lambda: {
            'project_id': 0,
            'tables_found': [],
            'total_matches': 0,
            'all_found_terms': set(),
            'table_details': {}
        })
        
        # 各テーブルで検索
        for table_name, df in self.tables_data.items():
            table_results = self.search_table_for_ai(table_name, df, compiled_patterns)
            
            for project_id, result in table_results.items():
                # プロジェクト結果を統合
                all_results[project_id]['project_id'] = project_id
                all_results[project_id]['tables_found'].append(table_name)
                all_results[project_id]['total_matches'] += result['total_matches']
                all_results[project_id]['all_found_terms'].update(result['all_found_terms'])
                all_results[project_id]['table_details'][table_name] = result
        
        # set を list に変換
        for project_id in all_results:
            all_results[project_id]['all_found_terms'] = list(all_results[project_id]['all_found_terms'])
        
        return dict(all_results)
    
    def merge_with_project_master(self, search_results: Dict[int, Dict]) -> Dict[int, Dict]:
        """プロジェクトマスター情報と統合"""
        print("Merging with project master data...")
        
        if 'projects' not in self.tables_data:
            print("  Warning: Projects master table not available")
            return search_results
        
        projects_df = self.tables_data['projects']
        
        # インデックスを予算事業IDに設定（必要に応じて）
        if '予算事業ID' in projects_df.columns:
            projects_indexed = projects_df.set_index('予算事業ID')
        else:
            projects_indexed = projects_df
        
        enhanced_results = {}
        
        for project_id, result in search_results.items():
            enhanced_result = result.copy()
            
            # マスター情報を追加
            if project_id in projects_indexed.index:
                master_info = projects_indexed.loc[project_id]
                enhanced_result['master_info'] = {
                    '事業名': master_info.get('事業名', ''),
                    '府省庁': master_info.get('府省庁', ''),
                    '局・庁': master_info.get('局・庁', ''),
                    '事業の目的': master_info.get('事業の目的', ''),
                    '事業の概要': master_info.get('事業の概要', ''),
                    '現状・課題': master_info.get('現状・課題', '')
                }
            else:
                enhanced_result['master_info'] = {}
            
            enhanced_results[project_id] = enhanced_result
        
        return enhanced_results
    
    def generate_search_statistics(self, broad_results: Dict, only_results: Dict) -> Dict:
        """検索統計を生成"""
        print("Generating search statistics...")
        
        # 基本統計
        total_projects = len(self.tables_data.get('projects', pd.DataFrame()))
        
        # 府省庁別統計
        ministry_stats_broad = Counter()
        ministry_stats_only = Counter()
        
        for result in broad_results.values():
            ministry = result.get('master_info', {}).get('府省庁', '不明')
            ministry_stats_broad[ministry] += 1
        
        for result in only_results.values():
            ministry = result.get('master_info', {}).get('府省庁', '不明')
            ministry_stats_only[ministry] += 1
        
        # 用語別統計
        term_stats_broad = Counter()
        term_stats_only = Counter()
        
        for result in broad_results.values():
            for term in result['all_found_terms']:
                term_stats_broad[term] += 1
        
        for result in only_results.values():
            for term in result['all_found_terms']:
                term_stats_only[term] += 1
        
        # テーブル別統計
        table_stats_broad = Counter()
        table_stats_only = Counter()
        
        for result in broad_results.values():
            for table in result['tables_found']:
                table_stats_broad[table] += 1
        
        for result in only_results.values():
            for table in result['tables_found']:
                table_stats_only[table] += 1
        
        statistics = {
            'summary': {
                'total_projects': total_projects,
                'ai_broad_projects': len(broad_results),
                'ai_only_projects': len(only_results),
                'ai_broad_percentage': (len(broad_results) / total_projects * 100) if total_projects > 0 else 0,
                'ai_only_percentage': (len(only_results) / total_projects * 100) if total_projects > 0 else 0
            },
            'ministry_distribution': {
                'ai_broad': dict(ministry_stats_broad.most_common(20)),
                'ai_only': dict(ministry_stats_only.most_common(20))
            },
            'term_frequency': {
                'ai_broad': dict(term_stats_broad.most_common(30)),
                'ai_only': dict(term_stats_only.most_common(30))
            },
            'table_distribution': {
                'ai_broad': dict(table_stats_broad.most_common()),
                'ai_only': dict(table_stats_only.most_common())
            }
        }
        
        return statistics
    
    def save_results(self, broad_results: Dict, only_results: Dict, statistics: Dict):
        """結果を保存"""
        print("Saving search results...")
        
        # AI関連事業（広範囲）
        broad_path = self.output_dir / 'ai_related_projects_feather.json'
        with open(broad_path, 'w', encoding='utf-8') as f:
            json.dump(broad_results, f, ensure_ascii=False, indent=2, default=str)
        print(f"  Saved: {broad_path} ({len(broad_results):,} projects)")
        
        # AI関連事業（限定）
        only_path = self.output_dir / 'ai_only_projects_feather.json'
        with open(only_path, 'w', encoding='utf-8') as f:
            json.dump(only_results, f, ensure_ascii=False, indent=2, default=str)
        print(f"  Saved: {only_path} ({len(only_results):,} projects)")
        
        # 統計レポート
        stats_path = self.output_dir / 'feather_search_statistics.json'
        with open(stats_path, 'w', encoding='utf-8') as f:
            json.dump(statistics, f, ensure_ascii=False, indent=2, default=str)
        print(f"  Saved: {stats_path}")
        
        # HTMLレポート生成
        self.generate_html_report(statistics)
    
    def generate_html_report(self, statistics: Dict):
        """HTMLレポートを生成"""
        html_content = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <title>Feather AI検索結果レポート</title>
    <style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 20px; line-height: 1.6; }}
        h1 {{ color: #2c5aa0; text-align: center; }}
        .summary {{ background-color: #e8f4f8; padding: 20px; border-radius: 8px; }}
        .number {{ font-weight: bold; color: #2c5aa0; font-size: 1.2em; }}
        .improvement {{ color: #28a745; font-weight: bold; }}
        table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
        .term-tag {{ background-color: #e1ecf4; padding: 2px 8px; border-radius: 12px; margin: 2px; display: inline-block; }}
    </style>
</head>
<body>
    <h1>🚀 Feather AI検索結果レポート</h1>
    
    <div class="summary">
        <h2>検索結果サマリー</h2>
        <p><strong>総事業数:</strong> <span class="number">{statistics['summary']['total_projects']:,}</span></p>
        <p><strong>AI関連事業（広範囲）:</strong> <span class="number improvement">{statistics['summary']['ai_broad_projects']:,}</span> 
           <span class="improvement">({statistics['summary']['ai_broad_percentage']:.2f}%)</span></p>
        <p><strong>AI関連事業（限定）:</strong> <span class="number improvement">{statistics['summary']['ai_only_projects']:,}</span> 
           <span class="improvement">({statistics['summary']['ai_only_percentage']:.2f}%)</span></p>
    </div>
    
    <h2>府省庁別分布（AI関連・広範囲）</h2>
    <table>
        <tr><th>府省庁</th><th>事業数</th></tr>
"""
        
        for ministry, count in list(statistics['ministry_distribution']['ai_broad'].items())[:15]:
            html_content += f"        <tr><td>{ministry}</td><td>{count}</td></tr>\n"
        
        html_content += """
    </table>
    
    <h2>AI用語検出頻度（上位20）</h2>
    <div style="display: flex; flex-wrap: wrap; gap: 5px;">
"""
        
        for term, count in list(statistics['term_frequency']['ai_broad'].items())[:20]:
            html_content += f'        <span class="term-tag">{term} ({count})</span>\n'
        
        html_content += f"""
    </div>
    
    <h2>テーブル別検出分布</h2>
    <table>
        <tr><th>テーブル</th><th>検出事業数</th></tr>
"""
        
        for table, count in statistics['table_distribution']['ai_broad'].items():
            html_content += f"        <tr><td>{table}</td><td>{count}</td></tr>\n"
        
        html_content += """
    </table>
    
    <div style="margin-top: 40px; text-align: center; color: #666;">
        Generated by Feather AI Searcher
    </div>
</body>
</html>"""
        
        html_path = self.output_dir / 'feather_search_report.html'
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"  Saved: {html_path}")
    
    def run(self):
        """包括的AI検索パイプライン実行"""
        print("=" * 60)
        print("🔍 Feather高速AI検索システム")
        print("=" * 60)
        
        total_start = time.time()
        
        # 1. Featherテーブル読み込み
        self.load_feather_tables()
        
        if not self.tables_data:
            print("No tables loaded. Exiting.")
            return
        
        # 2. 検索パターンコンパイル
        print("\nCompiling search patterns...")
        broad_patterns = self.compile_search_patterns(self.ai_broad_terms)
        only_patterns = self.compile_search_patterns(self.ai_only_terms)
        print(f"  Broad search: {len(broad_patterns)} patterns")
        print(f"  AI-only search: {len(only_patterns)} patterns")
        
        # 3. 包括的AI検索（広範囲）
        print("\n" + "="*50)
        print("🔍 BROAD AI SEARCH")
        print("="*50)
        broad_results = self.comprehensive_ai_search(broad_patterns)
        broad_enhanced = self.merge_with_project_master(broad_results)
        
        # 4. AI限定検索
        print("\n" + "="*50)
        print("🎯 AI-ONLY SEARCH")
        print("="*50)
        only_results = self.comprehensive_ai_search(only_patterns)
        only_enhanced = self.merge_with_project_master(only_results)
        
        # 5. 統計生成
        statistics = self.generate_search_statistics(broad_enhanced, only_enhanced)
        
        # 6. 結果保存
        self.save_results(broad_enhanced, only_enhanced, statistics)
        
        total_elapsed = time.time() - total_start
        
        # 最終結果表示
        print(f"\n{'='*60}")
        print("🎉 検索完了!")
        print(f"{'='*60}")
        print(f"⏱️  実行時間: {total_elapsed:.1f}秒")
        print(f"🔍 AI関連事業（広範囲）: {len(broad_enhanced):,}件")
        print(f"🎯 AI関連事業（限定）: {len(only_enhanced):,}件")
        print(f"📊 検索対象テーブル: {len(self.tables_data)}")
        print(f"📁 出力先: {self.output_dir}")
        print(f"{'='*60}")
        
        return broad_enhanced, only_enhanced, statistics


if __name__ == "__main__":
    searcher = FeatherAISearcher()
    searcher.run()