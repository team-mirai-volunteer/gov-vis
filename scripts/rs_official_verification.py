#!/usr/bin/env python3
"""
RSシステム公式AI検索結果との照合検証スクリプト
152事業が今回作成したデータに含まれているかを検証
"""
import pandas as pd
import json
import re
from pathlib import Path
from typing import Dict, List, Set, Any, Tuple
from collections import defaultdict, Counter
import time
from difflib import SequenceMatcher


class RSOfficalVerificationEngine:
    """RSシステム公式結果との照合検証クラス"""
    
    def __init__(self):
        self.official_list_path = Path("data/ai_investigation/AI_record_list.txt")
        self.improved_search_path = Path("data/improved_ai_search/ai_exact_improved.json")
        self.basic_form_path = Path("data/ai_basic_form_spreadsheet/ai_basic_form_complete_data.csv")
        self.feather_dir = Path("data/normalized_feather")
        self.output_dir = Path("data/rs_official_verification")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # データ格納
        self.official_projects = []
        self.improved_search_data = {}
        self.basic_form_data = None
        self.feather_tables = {}
    
    def load_official_ai_list(self) -> List[str]:
        """RSシステム公式AI検索結果152事業を読み込み"""
        print("Loading RS System official AI search results...")
        
        if not self.official_list_path.exists():
            print(f"Error: Official list not found at {self.official_list_path}")
            return []
        
        with open(self.official_list_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # 事業名をクリーンアップ
        projects = []
        for line in lines:
            project_name = line.strip()
            if project_name:  # 空行を除外
                projects.append(project_name)
        
        print(f"  Loaded {len(projects)} official AI projects")
        self.official_projects = projects
        return projects
    
    def load_improved_search_data(self) -> Dict:
        """改善されたAI検索結果を読み込み"""
        print("Loading improved AI search results...")
        
        if not self.improved_search_path.exists():
            print(f"Error: Improved search data not found at {self.improved_search_path}")
            return {}
        
        with open(self.improved_search_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        print(f"  Loaded {len(data)} improved AI search projects")
        self.improved_search_data = data
        return data
    
    def load_basic_form_data(self) -> pd.DataFrame:
        """基本形AIスプレッドシートを読み込み"""
        print("Loading basic form AI spreadsheet...")
        
        if not self.basic_form_path.exists():
            print(f"Error: Basic form data not found at {self.basic_form_path}")
            return pd.DataFrame()
        
        df = pd.read_csv(self.basic_form_path, encoding='utf-8')
        print(f"  Loaded {len(df)} basic form AI projects")
        self.basic_form_data = df
        return df
    
    def load_feather_tables(self):
        """必要に応じてFeatherテーブルを読み込み"""
        print("Loading Feather tables for detailed verification...")
        
        projects_path = self.feather_dir / "projects.feather"
        if projects_path.exists():
            self.feather_tables['projects'] = pd.read_feather(projects_path)
            print(f"  Loaded projects table: {len(self.feather_tables['projects'])} records")
    
    def fuzzy_match_project_name(self, official_name: str, candidate_name: str, threshold: float = 0.8) -> float:
        """事業名のファジーマッチング"""
        if not official_name or not candidate_name:
            return 0.0
        
        # 基本的な類似度
        similarity = SequenceMatcher(None, official_name, candidate_name).ratio()
        
        # 部分一致の確認
        if official_name in candidate_name or candidate_name in official_name:
            similarity = max(similarity, 0.9)
        
        return similarity
    
    def extract_project_names_from_improved_search(self) -> Dict[int, str]:
        """改善されたAI検索結果から事業名を抽出"""
        project_names = {}
        
        for project_id, data in self.improved_search_data.items():
            master_info = data.get('master_info', {})
            project_name = master_info.get('事業名', '')
            if project_name:
                project_names[int(project_id)] = project_name
        
        return project_names
    
    def extract_project_names_from_basic_form(self) -> Dict[int, str]:
        """基本形スプレッドシートから事業名を抽出"""
        project_names = {}
        
        if self.basic_form_data is not None and 'projects_事業名' in self.basic_form_data.columns:
            for idx, row in self.basic_form_data.iterrows():
                project_id = row.get('予算事業ID')
                project_name = row.get('projects_事業名', '')
                if project_id and project_name:
                    project_names[int(project_id)] = project_name
        
        return project_names
    
    def perform_comprehensive_matching(self) -> Dict:
        """包括的な事業名マッチングを実行"""
        print("Performing comprehensive project name matching...")
        
        # 各データソースから事業名を抽出
        improved_names = self.extract_project_names_from_improved_search()
        basic_form_names = self.extract_project_names_from_basic_form()
        
        # 全事業名のリスト（重複除去）
        all_project_names = {}
        all_project_names.update(improved_names)
        if basic_form_names:
            all_project_names.update(basic_form_names)
        
        print(f"  Total projects in our data: {len(all_project_names)}")
        
        # マッチング結果
        matching_results = {
            'exact_matches': {},
            'fuzzy_matches': {},
            'no_matches': [],
            'statistics': {}
        }
        
        exact_count = 0
        fuzzy_count = 0
        no_match_count = 0
        
        # 各公式事業名について検索
        for official_name in self.official_projects:
            best_match = None
            best_similarity = 0.0
            best_project_id = None
            
            # 完全一致チェック
            exact_found = False
            for project_id, project_name in all_project_names.items():
                if official_name == project_name:
                    matching_results['exact_matches'][official_name] = {
                        'project_id': project_id,
                        'matched_name': project_name,
                        'similarity': 1.0,
                        'in_improved_search': project_id in improved_names,
                        'in_basic_form': project_id in basic_form_names
                    }
                    exact_found = True
                    exact_count += 1
                    break
            
            if exact_found:
                continue
            
            # ファジーマッチング
            for project_id, project_name in all_project_names.items():
                similarity = self.fuzzy_match_project_name(official_name, project_name)
                if similarity > best_similarity:
                    best_similarity = similarity
                    best_match = project_name
                    best_project_id = project_id
            
            # 閾値以上のマッチがあるか
            if best_similarity >= 0.7:  # 70%以上の類似度
                matching_results['fuzzy_matches'][official_name] = {
                    'project_id': best_project_id,
                    'matched_name': best_match,
                    'similarity': best_similarity,
                    'in_improved_search': best_project_id in improved_names,
                    'in_basic_form': best_project_id in basic_form_names
                }
                fuzzy_count += 1
            else:
                matching_results['no_matches'].append({
                    'official_name': official_name,
                    'best_candidate': best_match,
                    'best_similarity': best_similarity
                })
                no_match_count += 1
        
        # 統計情報
        matching_results['statistics'] = {
            'total_official_projects': len(self.official_projects),
            'exact_matches': exact_count,
            'fuzzy_matches': fuzzy_count,
            'no_matches': no_match_count,
            'match_rate_exact': (exact_count / len(self.official_projects)) * 100,
            'match_rate_total': ((exact_count + fuzzy_count) / len(self.official_projects)) * 100,
            'coverage_analysis': {
                'in_improved_search': sum(1 for m in {**matching_results['exact_matches'], **matching_results['fuzzy_matches']}.values() if m['in_improved_search']),
                'in_basic_form': sum(1 for m in {**matching_results['exact_matches'], **matching_results['fuzzy_matches']}.values() if m['in_basic_form'])
            }
        }
        
        print(f"  Matching completed:")
        print(f"    Exact matches: {exact_count}")
        print(f"    Fuzzy matches: {fuzzy_count}")
        print(f"    No matches: {no_match_count}")
        print(f"    Total match rate: {matching_results['statistics']['match_rate_total']:.1f}%")
        
        return matching_results
    
    def analyze_missing_projects(self, matching_results: Dict) -> Dict:
        """見つからない事業の詳細分析"""
        print("Analyzing missing projects...")
        
        missing_projects = matching_results['no_matches']
        if not missing_projects:
            print("  No missing projects found!")
            return {'missing_analysis': [], 'recommendations': []}
        
        print(f"  Analyzing {len(missing_projects)} missing projects...")
        
        missing_analysis = []
        
        # 各欠落事業について詳細分析
        for missing in missing_projects:
            official_name = missing['official_name']
            
            # AI関連キーワードの有無をチェック
            ai_keywords = ['AI', 'ＡＩ', 'A.I.', '人工知能', '機械学習', '生成AI', '生成ＡＩ']
            found_keywords = [kw for kw in ai_keywords if kw in official_name]
            
            # 可能な原因を推定
            possible_causes = []
            if not found_keywords:
                possible_causes.append("事業名にAI関連キーワードが含まれていない")
            if len(official_name) > 50:
                possible_causes.append("事業名が長く、部分一致で検出困難")
            if '（' in official_name or '）' in official_name:
                possible_causes.append("括弧内の詳細情報により完全一致困難")
            
            missing_analysis.append({
                'official_name': official_name,
                'ai_keywords_found': found_keywords,
                'possible_causes': possible_causes,
                'name_length': len(official_name)
            })
        
        # 改善提案
        recommendations = []
        if missing_projects:
            recommendations.extend([
                "事業名以外のフィールド（事業概要、目的等）でのAI検索を強化",
                "より柔軟な部分一致検索パターンの導入",
                "括弧内情報を除外した事業名での検索",
                "同義語・類似語辞書の活用"
            ])
        
        return {
            'missing_analysis': missing_analysis,
            'recommendations': recommendations
        }
    
    def generate_verification_report(self, matching_results: Dict, missing_analysis: Dict):
        """検証レポートを生成・保存"""
        print("Generating verification report...")
        
        # 詳細結果をJSON保存
        full_report = {
            'verification_summary': {
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                'official_projects_count': len(self.official_projects),
                'our_data_projects_count': len(self.improved_search_data),
                'basic_form_projects_count': len(self.basic_form_data) if self.basic_form_data is not None else 0
            },
            'matching_results': matching_results,
            'missing_analysis': missing_analysis
        }
        
        json_path = self.output_dir / 'rs_official_verification_report.json'
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(full_report, f, ensure_ascii=False, indent=2, default=str)
        print(f"  Full report saved: {json_path}")
        
        # HTMLレポート生成
        self.generate_html_verification_report(matching_results, missing_analysis)
        
        # 簡潔なサマリーCSV
        self.generate_summary_csv(matching_results)
    
    def generate_summary_csv(self, matching_results: Dict):
        """検証結果サマリーCSVを生成"""
        summary_data = []
        
        # 完全一致
        for official_name, match_data in matching_results['exact_matches'].items():
            summary_data.append({
                '公式事業名': official_name,
                'マッチタイプ': '完全一致',
                '事業ID': match_data['project_id'],
                'マッチした事業名': match_data['matched_name'],
                '類似度': match_data['similarity'],
                '改善検索に含まれる': match_data['in_improved_search'],
                '基本形スプレッドシートに含まれる': match_data['in_basic_form']
            })
        
        # ファジーマッチ
        for official_name, match_data in matching_results['fuzzy_matches'].items():
            summary_data.append({
                '公式事業名': official_name,
                'マッチタイプ': 'ファジーマッチ',
                '事業ID': match_data['project_id'],
                'マッチした事業名': match_data['matched_name'],
                '類似度': match_data['similarity'],
                '改善検索に含まれる': match_data['in_improved_search'],
                '基本形スプレッドシートに含まれる': match_data['in_basic_form']
            })
        
        # マッチなし
        for missing in matching_results['no_matches']:
            summary_data.append({
                '公式事業名': missing['official_name'],
                'マッチタイプ': 'マッチなし',
                '事業ID': '',
                'マッチした事業名': missing.get('best_candidate', ''),
                '類似度': missing.get('best_similarity', 0),
                '改善検索に含まれる': False,
                '基本形スプレッドシートに含まれる': False
            })
        
        df_summary = pd.DataFrame(summary_data)
        csv_path = self.output_dir / 'verification_summary.csv'
        df_summary.to_csv(csv_path, index=False, encoding='utf-8-sig')
        print(f"  Summary CSV saved: {csv_path}")
    
    def generate_html_verification_report(self, matching_results: Dict, missing_analysis: Dict):
        """HTML検証レポートを生成"""
        stats = matching_results['statistics']
        
        html_content = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <title>RSシステム公式AI検索結果 照合検証レポート</title>
    <style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 20px; line-height: 1.6; }}
        h1 {{ color: #2c5aa0; text-align: center; border-bottom: 3px solid #2c5aa0; padding-bottom: 10px; }}
        .summary {{ background-color: #e8f4f8; padding: 20px; border-radius: 8px; margin: 20px 0; }}
        .metric {{ font-size: 1.5em; font-weight: bold; text-align: center; margin: 10px 0; }}
        .success {{ color: #28a745; }}
        .warning {{ color: #ffc107; }}
        .danger {{ color: #dc3545; }}
        table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
        .match-exact {{ background-color: #d4edda; }}
        .match-fuzzy {{ background-color: #fff3cd; }}
        .match-none {{ background-color: #f8d7da; }}
        ul {{ padding-left: 20px; }}
        li {{ margin: 5px 0; }}
    </style>
</head>
<body>
    <h1>🔍 RSシステム公式AI検索結果 照合検証レポート</h1>
    
    <div class="summary">
        <h2>📊 検証結果サマリー</h2>
        <div class="metric success">完全一致: {stats['exact_matches']}件</div>
        <div class="metric warning">ファジーマッチ: {stats['fuzzy_matches']}件</div>
        <div class="metric danger">マッチなし: {stats['no_matches']}件</div>
        <div class="metric">総マッチ率: {stats['match_rate_total']:.1f}%</div>
    </div>
    
    <h2>📈 詳細統計</h2>
    <table>
        <tr>
            <th>項目</th>
            <th>件数</th>
            <th>割合</th>
        </tr>
        <tr class="match-exact">
            <td><strong>完全一致</strong></td>
            <td>{stats['exact_matches']}</td>
            <td>{stats['match_rate_exact']:.1f}%</td>
        </tr>
        <tr class="match-fuzzy">
            <td><strong>ファジーマッチ</strong></td>
            <td>{stats['fuzzy_matches']}</td>
            <td>{(stats['fuzzy_matches']/stats['total_official_projects']*100):.1f}%</td>
        </tr>
        <tr class="match-none">
            <td><strong>マッチなし</strong></td>
            <td>{stats['no_matches']}</td>
            <td>{(stats['no_matches']/stats['total_official_projects']*100):.1f}%</td>
        </tr>
        <tr>
            <td><strong>公式事業総数</strong></td>
            <td>{stats['total_official_projects']}</td>
            <td>100.0%</td>
        </tr>
    </table>
    
    <h2>🎯 データカバレッジ分析</h2>
    <table>
        <tr>
            <th>データソース</th>
            <th>マッチした事業数</th>
            <th>カバレッジ</th>
        </tr>
        <tr>
            <td>改善されたAI検索結果</td>
            <td>{stats['coverage_analysis']['in_improved_search']}</td>
            <td>{(stats['coverage_analysis']['in_improved_search']/stats['total_official_projects']*100):.1f}%</td>
        </tr>
        <tr>
            <td>基本形AIスプレッドシート</td>
            <td>{stats['coverage_analysis']['in_basic_form']}</td>
            <td>{(stats['coverage_analysis']['in_basic_form']/stats['total_official_projects']*100):.1f}%</td>
        </tr>
    </table>
"""
        
        # 見つからない事業の分析
        if missing_analysis['missing_analysis']:
            html_content += f"""
    <h2>⚠️ 見つからない事業の分析</h2>
    <table>
        <tr>
            <th>公式事業名</th>
            <th>事業名の長さ</th>
            <th>AI関連キーワード</th>
            <th>推定原因</th>
        </tr>"""
            
            for missing in missing_analysis['missing_analysis'][:20]:  # 最初の20件
                keywords = ', '.join(missing['ai_keywords_found']) if missing['ai_keywords_found'] else 'なし'
                causes = ', '.join(missing['possible_causes']) if missing['possible_causes'] else '不明'
                html_content += f"""
        <tr class="match-none">
            <td>{missing['official_name']}</td>
            <td>{missing['name_length']}</td>
            <td>{keywords}</td>
            <td>{causes}</td>
        </tr>"""
            
            html_content += """
    </table>"""
        
        # 改善提案
        if missing_analysis['recommendations']:
            html_content += """
    <h2>💡 改善提案</h2>
    <ul>"""
            for rec in missing_analysis['recommendations']:
                html_content += f"        <li>{rec}</li>\n"
            html_content += """
    </ul>"""
        
        html_content += """
    <div style="margin-top: 40px; text-align: center; color: #666;">
        Generated by RS Official Verification Engine
    </div>
</body>
</html>"""
        
        html_path = self.output_dir / 'rs_verification_report.html'
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"  HTML report saved: {html_path}")
    
    def run(self):
        """検証パイプライン実行"""
        print("=" * 60)
        print("🔍 RS Official AI Search Verification")
        print("=" * 60)
        
        start_time = time.time()
        
        # 1. データ読み込み
        self.load_official_ai_list()
        self.load_improved_search_data()
        self.load_basic_form_data()
        self.load_feather_tables()
        
        if not self.official_projects:
            print("No official projects loaded. Exiting.")
            return None
        
        # 2. 包括的マッチング
        matching_results = self.perform_comprehensive_matching()
        
        # 3. 欠落事業分析
        missing_analysis = self.analyze_missing_projects(matching_results)
        
        # 4. 検証レポート生成
        self.generate_verification_report(matching_results, missing_analysis)
        
        elapsed = time.time() - start_time
        
        # 最終結果表示
        stats = matching_results['statistics']
        print(f"\n{'='*60}")
        print("🎉 検証完了!")
        print(f"{'='*60}")
        print(f"📊 公式事業数: {stats['total_official_projects']}件")
        print(f"✅ 完全一致: {stats['exact_matches']}件 ({stats['match_rate_exact']:.1f}%)")
        print(f"🔄 ファジーマッチ: {stats['fuzzy_matches']}件")
        print(f"❌ マッチなし: {stats['no_matches']}件")
        print(f"🎯 総マッチ率: {stats['match_rate_total']:.1f}%")
        print(f"⏱️  実行時間: {elapsed:.1f}秒")
        print(f"📁 出力先: {self.output_dir}")
        print(f"{'='*60}")
        
        return matching_results, missing_analysis


if __name__ == "__main__":
    verifier = RSOfficalVerificationEngine()
    verifier.run()