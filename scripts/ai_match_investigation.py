#!/usr/bin/env python3
"""
AI exact match検索の原因調査スクリプト
現在57件しか見つからない原因を詳細分析
"""
import pandas as pd
import json
import re
from pathlib import Path
from typing import Dict, List, Set, Any
from collections import defaultdict, Counter
import time


class AIMatchInvestigator:
    """AI検索マッチング問題の調査クラス"""
    
    def __init__(self, feather_dir: str = "data/normalized_feather"):
        self.feather_dir = Path(feather_dir)
        self.output_dir = Path("data/ai_investigation")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # 現在の検索パターン（問題のあるもの）
        self.current_pattern = r'\bAI\b'
        
        # 改善された検索パターン（テスト用）
        self.improved_patterns = [
            r'AI',  # 単純なAI（境界なし）
            r'ＡＩ',  # 全角AI
            r'A\.I\.',  # A.I.
            r'Ａ\.Ｉ\.',  # 全角A.I.
        ]
        
        # AI関連複合語パターン
        self.compound_patterns = [
            r'生成AI', r'生成ＡＩ',
            r'AI[ア-ン\w]*', r'ＡＩ[ア-ン\w]*',  # AI+何か
            r'[ア-ン\w]*AI', r'[ア-ン\w]*ＡＩ',  # 何か+AI
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
        print("Loading Feather tables for investigation...")
        
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
    
    def search_pattern_in_text(self, text: str, pattern: str) -> List[str]:
        """テキスト内でパターンを検索"""
        if not text or pd.isna(text):
            return []
        
        text_str = str(text)
        try:
            matches = re.findall(pattern, text_str, re.IGNORECASE)
            return matches
        except re.error:
            return []
    
    def analyze_current_pattern_limitations(self) -> Dict:
        """現在のパターンの制限を分析"""
        print("\n=== Analyzing Current Pattern Limitations ===")
        
        current_matches = defaultdict(list)
        improved_matches = defaultdict(list)
        compound_matches = defaultdict(list)
        
        total_records_checked = 0
        
        for table_name, df in self.tables_data.items():
            print(f"\nAnalyzing table: {table_name}")
            search_fields = self.search_config.get(table_name, [])
            available_fields = [f for f in search_fields if f in df.columns]
            
            if not available_fields:
                continue
            
            for idx, record in df.iterrows():
                project_id = record.get('予算事業ID', f'unknown_{idx}')
                
                for field in available_fields:
                    text = record.get(field, '')
                    if not text or pd.isna(text):
                        continue
                    
                    text_str = str(text)
                    total_records_checked += 1
                    
                    # 現在のパターン
                    current = self.search_pattern_in_text(text_str, self.current_pattern)
                    if current:
                        current_matches[project_id].append({
                            'table': table_name,
                            'field': field,
                            'text': text_str[:200],
                            'matches': current
                        })
                    
                    # 改善されたパターン
                    for pattern in self.improved_patterns:
                        improved = self.search_pattern_in_text(text_str, pattern)
                        if improved:
                            improved_matches[project_id].append({
                                'table': table_name,
                                'field': field,
                                'text': text_str[:200],
                                'pattern': pattern,
                                'matches': improved
                            })
                    
                    # 複合語パターン
                    for pattern in self.compound_patterns:
                        compound = self.search_pattern_in_text(text_str, pattern)
                        if compound:
                            compound_matches[project_id].append({
                                'table': table_name,
                                'field': field,
                                'text': text_str[:200],
                                'pattern': pattern,
                                'matches': compound
                            })
        
        analysis = {
            'total_records_checked': total_records_checked,
            'current_pattern_matches': len(current_matches),
            'improved_pattern_matches': len(improved_matches),
            'compound_pattern_matches': len(compound_matches),
            'current_pattern_projects': list(current_matches.keys()),
            'improved_pattern_projects': list(improved_matches.keys()),
            'compound_pattern_projects': list(compound_matches.keys()),
            'current_matches_detail': dict(current_matches),
            'improved_matches_detail': dict(improved_matches),
            'compound_matches_detail': dict(compound_matches)
        }
        
        print(f"\nAnalysis Results:")
        print(f"  Current pattern (\\bAI\\b): {analysis['current_pattern_matches']} projects")
        print(f"  Improved patterns: {analysis['improved_pattern_matches']} projects")
        print(f"  Compound patterns: {analysis['compound_pattern_matches']} projects")
        
        return analysis
    
    def find_missed_ai_instances(self, analysis: Dict) -> Dict:
        """見落とされたAIインスタンスを特定"""
        print("\n=== Finding Missed AI Instances ===")
        
        current_projects = set(analysis['current_pattern_projects'])
        improved_projects = set(analysis['improved_pattern_projects'])
        compound_projects = set(analysis['compound_pattern_projects'])
        
        # 見落とされたプロジェクト
        missed_by_current = improved_projects - current_projects
        missed_compounds = compound_projects - current_projects
        
        # 具体例の収集
        missed_examples = []
        
        # 改善されたパターンで見つかったが現在のパターンで見落とされた例
        for project_id in list(missed_by_current)[:10]:  # 最初の10件
            if project_id in analysis['improved_matches_detail']:
                for match in analysis['improved_matches_detail'][project_id][:3]:  # 各プロジェクトの最初の3件
                    missed_examples.append({
                        'type': 'improved_pattern',
                        'project_id': project_id,
                        'table': match['table'],
                        'field': match['field'],
                        'text': match['text'],
                        'pattern': match['pattern'],
                        'matches': match['matches']
                    })
        
        # 複合語で見つかったが現在のパターンで見落とされた例
        for project_id in list(missed_compounds)[:10]:  # 最初の10件
            if project_id in analysis['compound_matches_detail']:
                for match in analysis['compound_matches_detail'][project_id][:3]:  # 各プロジェクトの最初の3件
                    missed_examples.append({
                        'type': 'compound_pattern',
                        'project_id': project_id,
                        'table': match['table'],
                        'field': match['field'],
                        'text': match['text'],
                        'pattern': match['pattern'],
                        'matches': match['matches']
                    })
        
        missed_analysis = {
            'missed_by_improved_patterns': len(missed_by_current),
            'missed_by_compound_patterns': len(missed_compounds),
            'total_unique_missed': len(missed_by_current | missed_compounds),
            'missed_examples': missed_examples,
            'potential_total': len(current_projects | improved_projects | compound_projects)
        }
        
        print(f"Missed AI Instances:")
        print(f"  Missed by improved patterns: {missed_analysis['missed_by_improved_patterns']}")
        print(f"  Missed by compound patterns: {missed_analysis['missed_by_compound_patterns']}")
        print(f"  Total unique missed: {missed_analysis['total_unique_missed']}")
        print(f"  Potential total: {missed_analysis['potential_total']}")
        
        return missed_analysis
    
    def generate_detailed_statistics(self, analysis: Dict, missed_analysis: Dict) -> Dict:
        """詳細統計を生成"""
        
        # パターン別統計
        pattern_stats = Counter()
        for matches in analysis['improved_matches_detail'].values():
            for match in matches:
                pattern_stats[match['pattern']] += 1
        
        compound_stats = Counter()
        for matches in analysis['compound_matches_detail'].values():
            for match in matches:
                compound_stats[match['pattern']] += 1
        
        # テーブル別統計
        table_stats = Counter()
        for matches in analysis['improved_matches_detail'].values():
            for match in matches:
                table_stats[match['table']] += 1
        
        statistics = {
            'summary': {
                'current_method_projects': analysis['current_pattern_matches'],
                'improved_method_projects': analysis['improved_pattern_matches'],
                'compound_method_projects': analysis['compound_pattern_matches'],
                'total_potential_projects': missed_analysis['potential_total'],
                'improvement_absolute': missed_analysis['total_unique_missed'],
                'improvement_percentage': (missed_analysis['total_unique_missed'] / analysis['current_pattern_matches'] * 100) if analysis['current_pattern_matches'] > 0 else 0
            },
            'pattern_frequency': dict(pattern_stats.most_common()),
            'compound_frequency': dict(compound_stats.most_common()),
            'table_distribution': dict(table_stats.most_common()),
            'problem_analysis': {
                'word_boundary_issue': analysis['current_pattern_matches'] < analysis['improved_pattern_matches'],
                'compound_term_issue': analysis['compound_pattern_matches'] > 0,
                'full_width_issue': pattern_stats.get('ＡＩ', 0) > 0
            }
        }
        
        return statistics
    
    def save_investigation_results(self, analysis: Dict, missed_analysis: Dict, statistics: Dict):
        """調査結果を保存"""
        print("\nSaving investigation results...")
        
        # 完全な調査結果
        full_report = {
            'investigation_summary': {
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                'current_pattern': self.current_pattern,
                'improved_patterns': self.improved_patterns,
                'compound_patterns': self.compound_patterns
            },
            'pattern_analysis': analysis,
            'missed_instances': missed_analysis,
            'statistics': statistics
        }
        
        # JSON保存
        json_path = self.output_dir / 'ai_match_investigation_report.json'
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(full_report, f, ensure_ascii=False, indent=2, default=str)
        print(f"  Full report saved: {json_path}")
        
        # 簡潔なサマリー保存
        summary = {
            'current_matches': analysis['current_pattern_matches'],
            'potential_matches': missed_analysis['potential_total'],
            'missed_count': missed_analysis['total_unique_missed'],
            'improvement_needed': missed_analysis['total_unique_missed'] > 0,
            'key_issues': {
                'word_boundary_restrictive': analysis['improved_pattern_matches'] > analysis['current_pattern_matches'],
                'compound_terms_missed': analysis['compound_pattern_matches'] > 0,
                'full_width_missed': 'ＡＩ' in str(analysis['improved_matches_detail'])
            },
            'recommended_patterns': self.improved_patterns + self.compound_patterns
        }
        
        summary_path = self.output_dir / 'ai_investigation_summary.json'
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        print(f"  Summary saved: {summary_path}")
        
        # HTMLレポート生成
        self.generate_html_report(statistics, missed_analysis)
    
    def generate_html_report(self, statistics: Dict, missed_analysis: Dict):
        """HTML調査レポートを生成"""
        
        html_content = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <title>AI検索マッチング問題調査レポート</title>
    <style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 20px; line-height: 1.6; }}
        h1 {{ color: #dc3545; text-align: center; border-bottom: 3px solid #dc3545; padding-bottom: 10px; }}
        h2 {{ color: #333; border-bottom: 2px solid #ddd; padding-bottom: 5px; }}
        .alert {{ background-color: #fff3cd; border: 1px solid #ffeaa7; padding: 15px; border-radius: 5px; margin: 15px 0; }}
        .success {{ background-color: #d4edda; border: 1px solid #c3e6cb; }}
        .error {{ background-color: #f8d7da; border: 1px solid #f5c6cb; }}
        .metric {{ font-size: 1.5em; font-weight: bold; text-align: center; margin: 10px 0; }}
        .current {{ color: #dc3545; }}
        .potential {{ color: #28a745; }}
        .missed {{ color: #fd7e14; }}
        table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
        .example {{ background-color: #f8f9fa; padding: 10px; margin: 10px 0; border-left: 4px solid #007bff; }}
        .code {{ font-family: monospace; background-color: #f8f9fa; padding: 2px 4px; }}
    </style>
</head>
<body>
    <h1>🔍 AI検索マッチング問題調査レポート</h1>
    
    <div class="alert error">
        <h3>⚠️ 問題の概要</h3>
        <p>現在の検索パターン <span class="code">\\bAI\\b</span> では<strong>{statistics['summary']['current_method_projects']}件</strong>しか見つからないが、
        改善されたパターンでは<strong>{statistics['summary']['total_potential_projects']}件</strong>の可能性があります。</p>
        <div class="metric missed">見落とし: +{statistics['summary']['improvement_absolute']}件 ({statistics['summary']['improvement_percentage']:.1f}%改善)</div>
    </div>
    
    <h2>📊 検索結果比較</h2>
    <table>
        <tr>
            <th>検索方法</th>
            <th>マッチ数</th>
            <th>特徴</th>
        </tr>
        <tr>
            <td><strong>現在のパターン</strong><br><span class="code">\\bAI\\b</span></td>
            <td class="current">{statistics['summary']['current_method_projects']}件</td>
            <td>単語境界あり・複合語除外</td>
        </tr>
        <tr>
            <td><strong>改善されたパターン</strong><br><span class="code">AI|ＡＩ|A\\.I\\.</span></td>
            <td class="potential">{statistics['summary']['improved_method_projects']}件</td>
            <td>境界制限緩和・全角対応</td>
        </tr>
        <tr>
            <td><strong>複合語パターン</strong><br><span class="code">生成AI|AI搭載</span></td>
            <td class="potential">{statistics['summary']['compound_method_projects']}件</td>
            <td>複合語・派生語対応</td>
        </tr>
        <tr class="success">
            <td><strong>統合結果</strong></td>
            <td class="potential">{statistics['summary']['total_potential_projects']}件</td>
            <td>包括的AI検索</td>
        </tr>
    </table>
    
    <h2>🎯 発見された問題</h2>
    
    <div class="alert">
        <h4>1. 単語境界制限の問題</h4>
        <p>現在の <span class="code">\\bAI\\b</span> パターンは以下を除外：</p>
        <ul>
            <li>「生成AI」「AIシステム」などの複合語</li>
            <li>「AI搭載」「AI活用」などの連続表記</li>
            <li>文の途中での自然な使用</li>
        </ul>
    </div>
    
    <div class="alert">
        <h4>2. 全角文字の未対応</h4>
        <p>日本語文書でよく使われる全角「ＡＩ」が検索対象外</p>
    </div>
    
    <div class="alert">
        <h4>3. 表記バリエーションの見落とし</h4>
        <p>「A.I.」「Ａ.Ｉ.」などの略記表記が未対応</p>
    </div>
    
    <h2>📝 見落とし例（サンプル）</h2>
"""

        # 見落とし例を追加
        if missed_analysis.get('missed_examples'):
            for i, example in enumerate(missed_analysis['missed_examples'][:5]):
                html_content += f"""
    <div class="example">
        <strong>例 {i+1}: {example['type']}</strong><br>
        <strong>プロジェクトID:</strong> {example['project_id']}<br>
        <strong>テーブル:</strong> {example['table']}<br>
        <strong>フィールド:</strong> {example['field']}<br>
        <strong>マッチパターン:</strong> <span class="code">{example['pattern']}</span><br>
        <strong>テキスト:</strong> {example['text'][:150]}...<br>
        <strong>マッチ:</strong> {', '.join(example['matches'])}
    </div>
"""

        html_content += f"""
    <h2>🔧 推奨改善案</h2>
    
    <div class="alert success">
        <h4>新しい検索パターン</h4>
        <ul>
            <li><span class="code">AI|ＡＩ</span> - 基本形・全角対応</li>
            <li><span class="code">A\\.I\\.|Ａ\\.Ｉ\\.</span> - 略記対応</li>
            <li><span class="code">生成AI|AIシステム|AI活用</span> - 複合語対応</li>
        </ul>
        <p><strong>期待効果:</strong> {statistics['summary']['total_potential_projects']}件（+{statistics['summary']['improvement_absolute']}件）</p>
    </div>
    
    <div style="margin-top: 40px; text-align: center; color: #666; font-size: 0.9em;">
        Generated by AI Match Investigator
    </div>
</body>
</html>"""
        
        html_path = self.output_dir / 'ai_investigation_report.html'
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"  HTML report saved: {html_path}")
    
    def run(self):
        """調査パイプライン実行"""
        print("=" * 60)
        print("🔍 AI Exact Match Investigation")
        print("=" * 60)
        
        start_time = time.time()
        
        # 1. テーブル読み込み
        self.load_feather_tables()
        
        if not self.tables_data:
            print("No tables loaded. Exiting.")
            return None
        
        # 2. パターン制限分析
        analysis = self.analyze_current_pattern_limitations()
        
        # 3. 見落としインスタンス特定
        missed_analysis = self.find_missed_ai_instances(analysis)
        
        # 4. 詳細統計生成
        statistics = self.generate_detailed_statistics(analysis, missed_analysis)
        
        # 5. 結果保存
        self.save_investigation_results(analysis, missed_analysis, statistics)
        
        elapsed = time.time() - start_time
        
        # 結果表示
        print(f"\n{'='*60}")
        print("🎯 調査結果サマリー")
        print(f"{'='*60}")
        print(f"現在のマッチ数: {statistics['summary']['current_method_projects']}件")
        print(f"潜在的マッチ数: {statistics['summary']['total_potential_projects']}件")
        print(f"見落とし: +{statistics['summary']['improvement_absolute']}件 ({statistics['summary']['improvement_percentage']:.1f}%)")
        print(f"実行時間: {elapsed:.1f}秒")
        print(f"出力先: {self.output_dir}")
        print(f"{'='*60}")
        
        return analysis, missed_analysis, statistics


if __name__ == "__main__":
    investigator = AIMatchInvestigator()
    investigator.run()