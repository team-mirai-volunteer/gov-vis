#!/usr/bin/env python3
"""
改善されたAI検索システム
調査結果に基づく包括的なAI関連事業検索
"""
import pandas as pd
import json
import re
from pathlib import Path
from typing import Dict, List, Set, Any
from collections import defaultdict, Counter
import time


class ImprovedAISearcher:
    """改善されたAI検索クラス"""
    
    def __init__(self, feather_dir: str = "data/normalized_feather"):
        self.feather_dir = Path(feather_dir)
        self.output_dir = Path("data/improved_ai_search")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # 改善されたAI検索パターン（調査結果に基づく）
        self.ai_exact_patterns = [
            r'AI',           # 基本形（境界制限なし）
            r'ＡＩ',          # 全角
            r'A\.I\.',       # 略記（半角）
            r'Ａ\.Ｉ\.'       # 略記（全角）
        ]
        
        # AI複合語・派生語パターン
        self.ai_compound_patterns = [
            r'生成AI', r'生成ＡＩ',
            r'AI[ア-ン\w]*',  # AI+何か（AI搭載、AI活用等）
            r'ＡＩ[ア-ン\w]*',  # 全角版
            r'[ア-ン\w]*AI',  # 何か+AI
            r'[ア-ン\w]*ＡＩ'   # 全角版
        ]
        
        # 完全なAIパターン（exact + compound）
        self.all_ai_patterns = self.ai_exact_patterns + self.ai_compound_patterns
        
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
    
    def compile_search_patterns(self, patterns: List[str]) -> List[re.Pattern]:
        """検索パターンをコンパイル"""
        compiled_patterns = []
        for pattern in patterns:
            try:
                compiled = re.compile(pattern, re.IGNORECASE)
                compiled_patterns.append((pattern, compiled))
            except re.error as e:
                print(f"Warning: Invalid regex pattern '{pattern}': {e}")
        return compiled_patterns
    
    def search_text_with_patterns(self, text: str, compiled_patterns: List[tuple]) -> List[Dict]:
        """テキスト内でパターンを検索（マッチ詳細付き）"""
        if not text or pd.isna(text):
            return []
        
        text_str = str(text)
        found_matches = []
        
        for pattern_name, pattern in compiled_patterns:
            matches = pattern.finditer(text_str)
            for match in matches:
                found_matches.append({
                    'pattern': pattern_name,
                    'matched_text': match.group(),
                    'start': match.start(),
                    'end': match.end()
                })
        
        return found_matches
    
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
                all_found_patterns = set()
                all_matched_texts = set()
                
                # グループ内の各レコードを検索
                for idx, record in group.iterrows():
                    for field in available_fields:
                        text = record.get(field, '')
                        found_matches = self.search_text_with_patterns(text, compiled_patterns)
                        
                        if found_matches:
                            if field not in project_matches:
                                project_matches[field] = []
                            
                            for match in found_matches:
                                project_matches[field].append({
                                    'text': str(text)[:300],  # 最初の300文字
                                    'pattern': match['pattern'],
                                    'matched_text': match['matched_text'],
                                    'record_index': idx,
                                    'position': f"{match['start']}-{match['end']}"
                                })
                                
                                all_found_patterns.add(match['pattern'])
                                all_matched_texts.add(match['matched_text'])
                                total_matches += 1
                
                if project_matches:
                    results[int(project_id)] = {
                        'project_id': int(project_id),
                        'table_name': table_name,
                        'matches': project_matches,
                        'total_matches': total_matches,
                        'matched_fields': list(project_matches.keys()),
                        'all_found_patterns': list(all_found_patterns),
                        'all_matched_texts': list(all_matched_texts),
                        'record_count': len(group)
                    }
                
                processed_records += len(group)
        
        elapsed = time.time() - start_time
        print(f"    Found {len(results)} projects with AI terms in {elapsed:.2f}s")
        
        return results
    
    def comprehensive_ai_search(self, compiled_patterns: List[tuple]) -> Dict[int, Dict]:
        """全テーブルを対象とした包括的AI検索"""
        print("\\nComprehensive improved AI search across all tables...")
        
        all_results = defaultdict(lambda: {
            'project_id': 0,
            'tables_found': [],
            'total_matches': 0,
            'all_found_patterns': set(),
            'all_matched_texts': set(),
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
                all_results[project_id]['all_found_patterns'].update(result['all_found_patterns'])
                all_results[project_id]['all_matched_texts'].update(result['all_matched_texts'])
                all_results[project_id]['table_details'][table_name] = result
        
        # set を list に変換
        for project_id in all_results:
            all_results[project_id]['all_found_patterns'] = list(all_results[project_id]['all_found_patterns'])
            all_results[project_id]['all_matched_texts'] = list(all_results[project_id]['all_matched_texts'])
        
        return dict(all_results)
    
    def merge_with_project_master(self, search_results: Dict[int, Dict]) -> Dict[int, Dict]:
        """プロジェクトマスター情報と統合"""
        print("Merging with project master data...")
        
        if 'projects' not in self.tables_data:
            print("  Warning: Projects master table not available")
            return search_results
        
        projects_df = self.tables_data['projects']
        
        # インデックスを予算事業IDに設定
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
    
    def generate_search_statistics(self, exact_results: Dict, compound_results: Dict, all_results: Dict) -> Dict:
        """検索統計を生成"""
        print("Generating improved search statistics...")
        
        # 基本統計
        total_projects = len(self.tables_data.get('projects', pd.DataFrame()))
        
        # 府省庁別統計
        ministry_stats_exact = Counter()
        ministry_stats_compound = Counter()
        ministry_stats_all = Counter()
        
        for result in exact_results.values():
            ministry = result.get('master_info', {}).get('府省庁', '不明')
            ministry_stats_exact[ministry] += 1
        
        for result in compound_results.values():
            ministry = result.get('master_info', {}).get('府省庁', '不明')
            ministry_stats_compound[ministry] += 1
        
        for result in all_results.values():
            ministry = result.get('master_info', {}).get('府省庁', '不明')
            ministry_stats_all[ministry] += 1
        
        # パターン別統計
        pattern_stats_exact = Counter()
        pattern_stats_compound = Counter()
        pattern_stats_all = Counter()
        
        for result in exact_results.values():
            for pattern in result['all_found_patterns']:
                pattern_stats_exact[pattern] += 1
        
        for result in compound_results.values():
            for pattern in result['all_found_patterns']:
                pattern_stats_compound[pattern] += 1
        
        for result in all_results.values():
            for pattern in result['all_found_patterns']:
                pattern_stats_all[pattern] += 1
        
        # マッチテキスト統計
        matched_text_stats = Counter()
        for result in all_results.values():
            for text in result['all_matched_texts']:
                matched_text_stats[text] += 1
        
        statistics = {
            'summary': {
                'total_projects': total_projects,
                'ai_exact_projects': len(exact_results),
                'ai_compound_projects': len(compound_results),
                'ai_all_projects': len(all_results),
                'ai_exact_percentage': (len(exact_results) / total_projects * 100) if total_projects > 0 else 0,
                'ai_compound_percentage': (len(compound_results) / total_projects * 100) if total_projects > 0 else 0,
                'ai_all_percentage': (len(all_results) / total_projects * 100) if total_projects > 0 else 0
            },
            'ministry_distribution': {
                'ai_exact': dict(ministry_stats_exact.most_common(20)),
                'ai_compound': dict(ministry_stats_compound.most_common(20)),
                'ai_all': dict(ministry_stats_all.most_common(20))
            },
            'pattern_frequency': {
                'ai_exact': dict(pattern_stats_exact.most_common()),
                'ai_compound': dict(pattern_stats_compound.most_common()),
                'ai_all': dict(pattern_stats_all.most_common())
            },
            'matched_text_frequency': dict(matched_text_stats.most_common(50)),
            'improvement_analysis': {
                'old_exact_count': 57,  # 調査結果から
                'new_exact_count': len(exact_results),
                'improvement_absolute': len(exact_results) - 57,
                'improvement_percentage': ((len(exact_results) - 57) / 57 * 100) if 57 > 0 else 0
            }
        }
        
        return statistics
    
    def save_results(self, exact_results: Dict, compound_results: Dict, all_results: Dict, statistics: Dict):
        """結果を保存"""
        print("Saving improved search results...")
        
        # AI exact検索結果
        exact_path = self.output_dir / 'ai_exact_improved.json'
        with open(exact_path, 'w', encoding='utf-8') as f:
            json.dump(exact_results, f, ensure_ascii=False, indent=2, default=str)
        print(f"  Saved: {exact_path} ({len(exact_results):,} projects)")
        
        # AI複合語検索結果
        compound_path = self.output_dir / 'ai_compound_improved.json'
        with open(compound_path, 'w', encoding='utf-8') as f:
            json.dump(compound_results, f, ensure_ascii=False, indent=2, default=str)
        print(f"  Saved: {compound_path} ({len(compound_results):,} projects)")
        
        # AI包括検索結果
        all_path = self.output_dir / 'ai_all_improved.json'
        with open(all_path, 'w', encoding='utf-8') as f:
            json.dump(all_results, f, ensure_ascii=False, indent=2, default=str)
        print(f"  Saved: {all_path} ({len(all_results):,} projects)")
        
        # 統計レポート
        stats_path = self.output_dir / 'improved_search_statistics.json'
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
    <title>改善されたAI検索結果レポート</title>
    <style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 20px; line-height: 1.6; }}
        h1 {{ color: #28a745; text-align: center; border-bottom: 3px solid #28a745; padding-bottom: 10px; }}
        h2 {{ color: #333; border-bottom: 2px solid #ddd; padding-bottom: 5px; }}
        .improvement {{ background-color: #d4edda; border: 1px solid #c3e6cb; padding: 20px; border-radius: 8px; margin: 20px 0; }}
        .metric {{ font-size: 1.5em; font-weight: bold; text-align: center; margin: 10px 0; }}
        .success {{ color: #28a745; }}
        .old {{ color: #dc3545; }}
        table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
        .term-tag {{ background-color: #e1ecf4; padding: 2px 8px; border-radius: 12px; margin: 2px; display: inline-block; }}
        .highlight {{ background-color: #fff3cd; }}
    </style>
</head>
<body>
    <h1>🚀 改善されたAI検索結果レポート</h1>
    
    <div class="improvement">
        <h2>📈 検索改善結果</h2>
        <div class="metric old">旧パターン: 57件</div>
        <div class="metric success">新パターン: {statistics['summary']['ai_exact_projects']}件</div>
        <div class="metric success">改善: +{statistics['improvement_analysis']['improvement_absolute']}件 ({statistics['improvement_analysis']['improvement_percentage']:.1f}%)</div>
    </div>
    
    <h2>📊 検索結果サマリー</h2>
    <table>
        <tr>
            <th>検索カテゴリ</th>
            <th>事業数</th>
            <th>割合</th>
            <th>説明</th>
        </tr>
        <tr class="highlight">
            <td><strong>AI Exact</strong></td>
            <td class="success">{statistics['summary']['ai_exact_projects']}</td>
            <td class="success">{statistics['summary']['ai_exact_percentage']:.2f}%</td>
            <td>AI、ＡＩ、A.I.等の基本形</td>
        </tr>
        <tr>
            <td><strong>AI複合語</strong></td>
            <td>{statistics['summary']['ai_compound_projects']}</td>
            <td>{statistics['summary']['ai_compound_percentage']:.2f}%</td>
            <td>生成AI、AI搭載等の複合語</td>
        </tr>
        <tr>
            <td><strong>AI全体</strong></td>
            <td>{statistics['summary']['ai_all_projects']}</td>
            <td>{statistics['summary']['ai_all_percentage']:.2f}%</td>
            <td>すべてのAI関連パターン</td>
        </tr>
    </table>
    
    <h2>🏢 府省庁別分布（AI Exact）</h2>
    <table>
        <tr><th>府省庁</th><th>事業数</th></tr>"""
        
        for ministry, count in list(statistics['ministry_distribution']['ai_exact'].items())[:15]:
            html_content += f"        <tr><td>{ministry}</td><td>{count}</td></tr>\\n"
        
        html_content += f"""
    </table>
    
    <h2>🔍 検出パターン頻度</h2>
    <div style="display: flex; flex-wrap: wrap; gap: 5px;">"""
        
        for pattern, count in list(statistics['pattern_frequency']['ai_exact'].items())[:20]:
            html_content += f'        <span class="term-tag">{pattern} ({count})</span>\\n'
        
        html_content += f"""
    </div>
    
    <h2>📝 実際のマッチテキスト（上位20）</h2>
    <div style="display: flex; flex-wrap: wrap; gap: 5px;">"""
        
        for text, count in list(statistics['matched_text_frequency'].items())[:20]:
            html_content += f'        <span class="term-tag">{text} ({count})</span>\\n'
        
        html_content += f"""
    </div>
    
    <div style="margin-top: 40px; text-align: center; color: #666;">
        Generated by Improved AI Searcher
    </div>
</body>
</html>"""
        
        html_path = self.output_dir / 'improved_search_report.html'
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"  Saved: {html_path}")
    
    def run(self):
        """改善されたAI検索パイプライン実行"""
        print("=" * 60)
        print("🔍 Improved AI Search System")
        print("=" * 60)
        
        total_start = time.time()
        
        # 1. Featherテーブル読み込み
        self.load_feather_tables()
        
        if not self.tables_data:
            print("No tables loaded. Exiting.")
            return
        
        # 2. 検索パターンコンパイル
        print("\\nCompiling improved search patterns...")
        exact_patterns = self.compile_search_patterns(self.ai_exact_patterns)
        compound_patterns = self.compile_search_patterns(self.ai_compound_patterns)
        all_patterns = self.compile_search_patterns(self.all_ai_patterns)
        
        print(f"  AI exact patterns: {len(exact_patterns)}")
        print(f"  AI compound patterns: {len(compound_patterns)}")
        print(f"  All AI patterns: {len(all_patterns)}")
        
        # 3. AI exact検索
        print("\\n" + "="*50)
        print("🎯 AI EXACT SEARCH (Improved)")
        print("="*50)
        exact_results = self.comprehensive_ai_search(exact_patterns)
        exact_enhanced = self.merge_with_project_master(exact_results)
        
        # 4. AI複合語検索
        print("\\n" + "="*50)
        print("🔧 AI COMPOUND SEARCH")
        print("="*50)
        compound_results = self.comprehensive_ai_search(compound_patterns)
        compound_enhanced = self.merge_with_project_master(compound_results)
        
        # 5. AI包括検索
        print("\\n" + "="*50)
        print("🚀 AI ALL COMPREHENSIVE SEARCH")
        print("="*50)
        all_results = self.comprehensive_ai_search(all_patterns)
        all_enhanced = self.merge_with_project_master(all_results)
        
        # 6. 統計生成
        statistics = self.generate_search_statistics(exact_enhanced, compound_enhanced, all_enhanced)
        
        # 7. 結果保存
        self.save_results(exact_enhanced, compound_enhanced, all_enhanced, statistics)
        
        total_elapsed = time.time() - total_start
        
        # 最終結果表示
        print(f"\\n{'='*60}")
        print("🎉 改善された検索完了!")
        print(f"{'='*60}")
        print(f"⏱️  実行時間: {total_elapsed:.1f}秒")
        print(f"🎯 AI Exact: {len(exact_enhanced):,}件 (旧:57件 → 改善:+{len(exact_enhanced)-57}件)")
        print(f"🔧 AI複合語: {len(compound_enhanced):,}件")
        print(f"🚀 AI包括: {len(all_enhanced):,}件")
        print(f"📊 改善率: {((len(exact_enhanced)-57)/57*100):.1f}%" if len(exact_enhanced) > 57 else "0%")
        print(f"📁 出力先: {self.output_dir}")
        print(f"{'='*60}")
        
        return exact_enhanced, compound_enhanced, all_enhanced, statistics


if __name__ == "__main__":
    searcher = ImprovedAISearcher()
    searcher.run()