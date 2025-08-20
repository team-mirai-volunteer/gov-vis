#!/usr/bin/env python3
"""
2024年度予算分析と上位1パーセンタイル事業リスト作成
- 実際の予算額を詳細分析
- 上位1%事業リストの生成
- 拡張版HTMLレポート作成
"""

import pandas as pd
import json
import numpy as np
from pathlib import Path
from typing import Dict, List, Any, Tuple
import warnings
warnings.filterwarnings('ignore')

class BudgetAnalyzer:
    def __init__(self, data_path: str):
        self.data_path = Path(data_path)
        self.df = None
        self.budget_data = []
        self.output_dir = Path("data/budget_analysis_2024")
        self.output_dir.mkdir(exist_ok=True)
        
    def load_data(self):
        """データ読み込み"""
        print("📊 データ読み込み開始...")
        try:
            self.df = pd.read_feather(self.data_path)
            print(f"✓ データ読み込み完了: {len(self.df):,}行 × {len(self.df.columns)}列")
            return True
        except Exception as e:
            print(f"❌ データ読み込みエラー: {e}")
            return False
    
    def extract_budget_data(self) -> List[Dict[str, Any]]:
        """予算データの詳細抽出"""
        print("\n================================================================================")
        print("1. 2024年度予算データ抽出・分析")
        print("================================================================================")
        
        budget_records = []
        projects_with_budget = 0
        extraction_errors = 0
        
        print("予算データ抽出中...")
        
        for idx, row in self.df.iterrows():
            try:
                # 基本情報
                project_info = {
                    'project_id': row['予算事業ID'],
                    'project_name': row['事業名'],
                    'ministry': row['府省庁'],
                    'agency': row['局・庁'] if pd.notna(row['局・庁']) else '',
                    'fiscal_year': row['事業年度'],
                    'project_category': row['事業区分'] if pd.notna(row['事業区分']) else '',
                }
                
                # 予算JSON解析
                if pd.notna(row['budget_summary_json']) and row['budget_summary_json'] != '[]':
                    budget_json = json.loads(row['budget_summary_json'])
                    
                    if isinstance(budget_json, list) and len(budget_json) > 0:
                        # 複数年度のデータがある場合は2024年度を探す
                        budget_2024 = None
                        total_initial_budget = 0
                        total_current_budget = 0
                        total_execution = 0
                        total_next_year_request = 0
                        
                        for budget_record in budget_json:
                            if isinstance(budget_record, dict):
                                year = budget_record.get('予算年度', 0)
                                if year == 2024:
                                    budget_2024 = budget_record
                                
                                # 累積予算額計算
                                initial = budget_record.get('当初予算（合計）', 0) or 0
                                current = budget_record.get('計（歳出予算現額合計）', 0) or 0  
                                execution = budget_record.get('執行額（合計）', 0) or 0
                                next_request = budget_record.get('翌年度要求額（合計）', 0) or 0
                                
                                if isinstance(initial, (int, float)) and initial > 0:
                                    total_initial_budget += initial
                                if isinstance(current, (int, float)) and current > 0:
                                    total_current_budget += current
                                if isinstance(execution, (int, float)) and execution > 0:
                                    total_execution += execution
                                if isinstance(next_request, (int, float)) and next_request > 0:
                                    total_next_year_request += next_request
                        
                        # 2024年度データまたは累積データを使用
                        if budget_2024:
                            budget_info = budget_2024
                        elif total_current_budget > 0:
                            budget_info = {
                                '当初予算（合計）': total_initial_budget,
                                '計（歳出予算現額合計）': total_current_budget,
                                '執行額（合計）': total_execution,
                                '翌年度要求額（合計）': total_next_year_request
                            }
                        else:
                            budget_info = budget_json[0]  # 最初のレコードを使用
                        
                        # 予算額データ整理
                        initial_budget = budget_info.get('当初予算（合計）', 0) or 0
                        current_budget = budget_info.get('計（歳出予算現額合計）', 0) or 0
                        execution_amount = budget_info.get('執行額（合計）', 0) or 0
                        next_year_request = budget_info.get('翌年度要求額（合計）', 0) or 0
                        
                        # 数値型確保
                        initial_budget = float(initial_budget) if isinstance(initial_budget, (int, float)) else 0
                        current_budget = float(current_budget) if isinstance(current_budget, (int, float)) else 0
                        execution_amount = float(execution_amount) if isinstance(execution_amount, (int, float)) else 0
                        next_year_request = float(next_year_request) if isinstance(next_year_request, (int, float)) else 0
                        
                        # 執行率計算
                        execution_rate = (execution_amount / current_budget * 100) if current_budget > 0 else 0
                        
                        # 予算情報をproject_infoに追加
                        project_info.update({
                            'initial_budget': initial_budget,
                            'current_budget': current_budget,  # これをメイン指標として使用
                            'execution_amount': execution_amount,
                            'execution_rate': execution_rate,
                            'next_year_request': next_year_request,
                            'has_valid_budget': current_budget > 0
                        })
                        
                        if current_budget > 0:
                            projects_with_budget += 1
                            
                        budget_records.append(project_info)
                
            except Exception as e:
                extraction_errors += 1
                # エラーでも基本情報は追加（予算は0）
                project_info.update({
                    'initial_budget': 0,
                    'current_budget': 0,
                    'execution_amount': 0,
                    'execution_rate': 0,
                    'next_year_request': 0,
                    'has_valid_budget': False
                })
                budget_records.append(project_info)
                continue
        
        self.budget_data = budget_records
        
        print(f"✓ 予算データ抽出完了")
        print(f"  - 総事業数: {len(budget_records):,}")
        print(f"  - 予算データ有効事業: {projects_with_budget:,}")
        print(f"  - 抽出エラー: {extraction_errors:,}")
        print(f"  - 予算データ有効率: {(projects_with_budget/len(budget_records)*100):.1f}%")
        
        return budget_records
    
    def calculate_budget_statistics(self) -> Dict[str, Any]:
        """予算統計計算"""
        print("\n================================================================================")
        print("2. 予算統計分析")
        print("================================================================================")
        
        # 有効な予算データのみフィルタ
        valid_budgets = [p for p in self.budget_data if p['has_valid_budget']]
        
        if not valid_budgets:
            print("❌ 有効な予算データが見つかりません")
            return {}
        
        # 予算額リスト作成
        current_budgets = [p['current_budget'] for p in valid_budgets]
        initial_budgets = [p['initial_budget'] for p in valid_budgets]
        execution_amounts = [p['execution_amount'] for p in valid_budgets]
        execution_rates = [p['execution_rate'] for p in valid_budgets if p['execution_rate'] > 0]
        
        # 基本統計
        current_budgets_sorted = sorted(current_budgets, reverse=True)
        
        # パーセンタイル計算
        percentile_99 = np.percentile(current_budgets, 99) if current_budgets else 0
        percentile_95 = np.percentile(current_budgets, 95) if current_budgets else 0
        percentile_90 = np.percentile(current_budgets, 90) if current_budgets else 0
        percentile_75 = np.percentile(current_budgets, 75) if current_budgets else 0
        percentile_50 = np.percentile(current_budgets, 50) if current_budgets else 0
        
        # 上位1%事業数
        top_1_percent_count = len([b for b in current_budgets if b >= percentile_99])
        
        stats = {
            'total_projects': len(self.budget_data),
            'projects_with_budget': len(valid_budgets),
            'budget_coverage_rate': (len(valid_budgets) / len(self.budget_data)) * 100,
            
            # 現行予算統計
            'current_budget_stats': {
                'mean': np.mean(current_budgets) if current_budgets else 0,
                'median': np.median(current_budgets) if current_budgets else 0,
                'std': np.std(current_budgets) if current_budgets else 0,
                'min': min(current_budgets) if current_budgets else 0,
                'max': max(current_budgets) if current_budgets else 0,
                'total': sum(current_budgets) if current_budgets else 0
            },
            
            # パーセンタイル
            'percentiles': {
                'p99': percentile_99,
                'p95': percentile_95,
                'p90': percentile_90,
                'p75': percentile_75,
                'p50': percentile_50
            },
            
            # 上位1%情報
            'top_1_percent': {
                'threshold': percentile_99,
                'count': top_1_percent_count,
                'percentage': (top_1_percent_count / len(valid_budgets)) * 100 if valid_budgets else 0
            },
            
            # 執行率統計
            'execution_stats': {
                'mean_rate': np.mean(execution_rates) if execution_rates else 0,
                'median_rate': np.median(execution_rates) if execution_rates else 0,
                'projects_with_execution': len(execution_rates)
            }
        }
        
        print("2024年度予算統計:")
        print(f"  - 総事業数: {stats['total_projects']:,}")
        print(f"  - 予算データ有効事業: {stats['projects_with_budget']:,}")
        print(f"  - 予算データカバー率: {stats['budget_coverage_rate']:.1f}%")
        print()
        print("現行予算額統計:")
        print(f"  - 平均: {stats['current_budget_stats']['mean']:,.0f}円")
        print(f"  - 中央値: {stats['current_budget_stats']['median']:,.0f}円")  
        print(f"  - 最大: {stats['current_budget_stats']['max']:,.0f}円")
        print(f"  - 総額: {stats['current_budget_stats']['total']:,.0f}円")
        print()
        print("パーセンタイル:")
        print(f"  - 99%tile (上位1%閾値): {stats['percentiles']['p99']:,.0f}円")
        print(f"  - 95%tile: {stats['percentiles']['p95']:,.0f}円")
        print(f"  - 90%tile: {stats['percentiles']['p90']:,.0f}円")
        print(f"  - 75%tile: {stats['percentiles']['p75']:,.0f}円")
        print(f"  - 50%tile: {stats['percentiles']['p50']:,.0f}円")
        print()
        print("上位1%事業:")
        print(f"  - 閾値: {stats['top_1_percent']['threshold']:,.0f}円")
        print(f"  - 事業数: {stats['top_1_percent']['count']:,}")
        print(f"  - 全体に占める割合: {stats['top_1_percent']['percentage']:.1f}%")
        
        return stats
    
    def identify_top_1_percent_projects(self, stats: Dict[str, Any]) -> List[Dict[str, Any]]:
        """上位1%事業の特定"""
        print("\n================================================================================")
        print("3. 上位1%事業特定・リスト作成")
        print("================================================================================")
        
        threshold = stats['top_1_percent']['threshold']
        
        # 上位1%事業フィルタリング
        top_projects = []
        for project in self.budget_data:
            if project['has_valid_budget'] and project['current_budget'] >= threshold:
                top_projects.append(project)
        
        # 予算額でソート
        top_projects.sort(key=lambda x: x['current_budget'], reverse=True)
        
        # ランキング追加
        for i, project in enumerate(top_projects, 1):
            project['budget_rank'] = i
        
        print(f"✓ 上位1%事業特定完了: {len(top_projects)}事業")
        print()
        print("上位10事業:")
        for i, project in enumerate(top_projects[:10], 1):
            print(f"  {i:2}. {project['project_name'][:50]}...")
            print(f"      府省庁: {project['ministry']}")
            print(f"      予算額: {project['current_budget']:,.0f}円")
            print(f"      執行率: {project['execution_rate']:.1f}%")
            print()
        
        return top_projects
    
    def analyze_ministry_budget_distribution(self, top_projects: List[Dict[str, Any]]) -> Dict[str, Any]:
        """府省庁別予算分析"""
        print("\n================================================================================")
        print("4. 府省庁別予算分析")  
        print("================================================================================")
        
        # 全体の府省庁別予算
        ministry_budgets = {}
        for project in self.budget_data:
            if project['has_valid_budget']:
                ministry = project['ministry']
                if ministry not in ministry_budgets:
                    ministry_budgets[ministry] = {
                        'total_budget': 0,
                        'project_count': 0,
                        'projects': []
                    }
                ministry_budgets[ministry]['total_budget'] += project['current_budget']
                ministry_budgets[ministry]['project_count'] += 1
                ministry_budgets[ministry]['projects'].append(project)
        
        # 上位1%の府省庁別分布
        top_ministry_distribution = {}
        for project in top_projects:
            ministry = project['ministry']
            if ministry not in top_ministry_distribution:
                top_ministry_distribution[ministry] = {
                    'project_count': 0,
                    'total_budget': 0,
                    'projects': []
                }
            top_ministry_distribution[ministry]['project_count'] += 1
            top_ministry_distribution[ministry]['total_budget'] += project['current_budget']
            top_ministry_distribution[ministry]['projects'].append(project)
        
        # 府省庁を予算額でソート
        ministry_ranking = sorted(ministry_budgets.items(), 
                                key=lambda x: x[1]['total_budget'], reverse=True)
        
        top_ministry_ranking = sorted(top_ministry_distribution.items(),
                                    key=lambda x: x[1]['project_count'], reverse=True)
        
        analysis = {
            'all_ministries': ministry_budgets,
            'top_1_percent_ministries': top_ministry_distribution,
            'ministry_budget_ranking': ministry_ranking[:10],
            'top_projects_ministry_ranking': top_ministry_ranking
        }
        
        print("全体予算上位10府省庁:")
        for i, (ministry, data) in enumerate(ministry_ranking[:10], 1):
            avg_budget = data['total_budget'] / data['project_count'] if data['project_count'] > 0 else 0
            print(f"  {i:2}. {ministry}")
            print(f"      総予算: {data['total_budget']:,.0f}円")
            print(f"      事業数: {data['project_count']:,}")
            print(f"      平均予算: {avg_budget:,.0f}円")
        print()
        
        print("上位1%事業の府省庁分布:")
        for i, (ministry, data) in enumerate(top_ministry_ranking, 1):
            percentage = (data['project_count'] / len(top_projects)) * 100
            avg_budget = data['total_budget'] / data['project_count'] if data['project_count'] > 0 else 0
            print(f"  {i:2}. {ministry}")
            print(f"      上位1%事業数: {data['project_count']:,} ({percentage:.1f}%)")
            print(f"      総予算: {data['total_budget']:,.0f}円")
            print(f"      平均予算: {avg_budget:,.0f}円")
        
        return analysis
    
    def create_top_projects_list(self, top_projects: List[Dict[str, Any]]):
        """上位1%事業リスト作成（CSV/Excel）"""
        print("\n================================================================================")
        print("5. 上位1%事業リスト出力")
        print("================================================================================")
        
        # DataFrame作成
        df_top = pd.DataFrame([
            {
                'ランキング': p['budget_rank'],
                '事業名': p['project_name'],
                '府省庁': p['ministry'],
                '局・庁': p['agency'],
                '事業区分': p['project_category'],
                '当初予算額': int(p['initial_budget']),
                '現行予算額': int(p['current_budget']),
                '執行額': int(p['execution_amount']),
                '執行率(%)': round(p['execution_rate'], 1),
                '次年度要求額': int(p['next_year_request']),
                '事業ID': p['project_id'],
                '年度': p['fiscal_year']
            }
            for p in top_projects
        ])
        
        # ファイル出力
        csv_path = self.output_dir / "top_1_percent_projects.csv"
        excel_path = self.output_dir / "top_1_percent_projects.xlsx"
        
        df_top.to_csv(csv_path, index=False, encoding='utf-8-sig')
        df_top.to_excel(excel_path, index=False, sheet_name='上位1%事業リスト')
        
        print(f"✓ CSV出力完了: {csv_path}")
        print(f"✓ Excel出力完了: {excel_path}")
        print(f"  - 出力事業数: {len(df_top):,}")
        
        return df_top
    
    def create_enhanced_html_report(self, stats: Dict[str, Any], 
                                  top_projects: List[Dict[str, Any]],
                                  ministry_analysis: Dict[str, Any]):
        """拡張HTMLレポート生成"""
        print("\n================================================================================")
        print("6. 拡張HTMLレポート生成")
        print("================================================================================")
        
        # 基本統計
        total_projects = stats['total_projects']
        projects_with_budget = stats['projects_with_budget'] 
        budget_coverage = stats['budget_coverage_rate']
        
        # 予算統計
        budget_stats = stats['current_budget_stats']
        percentiles = stats['percentiles']
        top_5_info = stats['top_5_percent']
        
        html_content = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <title>2024年度予算分析レポート - 上位1%事業リスト付き</title>
    <style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 20px; background: #f9fafb; color: #111827; }}
        .container {{ max-width: 1400px; margin: 0 auto; background: #ffffff; padding: 30px; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
        h1 {{ color: #1f2937; text-align: center; border-bottom: 3px solid #059669; padding-bottom: 15px; }}
        h2 {{ color: #1f2937; margin-top: 30px; border-left: 5px solid #059669; padding-left: 10px; }}
        .summary {{ background: #059669; color: white; padding: 20px; border-radius: 8px; margin: 20px 0; }}
        .metric {{ display: inline-block; margin: 10px 20px; }}
        .metric-value {{ font-size: 2em; font-weight: bold; }}
        .metric-label {{ font-size: 0.9em; opacity: 0.9; }}
        table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        th {{ background: #f3f4f6; color: #1f2937; padding: 12px; text-align: left; border-bottom: 2px solid #e5e7eb; }}
        td {{ padding: 10px; border-bottom: 1px solid #e5e7eb; color: #374151; }}
        tr:hover {{ background: #f9fafb; }}
        .insight {{ background: #d1fae5; border-left: 4px solid #059669; padding: 15px; margin: 10px 0; color: #064e3b; }}
        .top-badge {{ background: #dc2626; color: white; padding: 2px 8px; border-radius: 4px; font-size: 0.8em; margin-left: 10px; }}
        .budget-tier {{ padding: 5px 10px; border-radius: 20px; font-size: 0.9em; font-weight: bold; }}
        .tier-mega {{ background: #fecaca; color: #991b1b; }}
        .tier-large {{ background: #fed7aa; color: #9a3412; }}
        .tier-medium {{ background: #fef3c7; color: #92400e; }}
        .number {{ text-align: right; }}
        .project-name {{ max-width: 400px; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>💰 2024年度予算分析レポート <span class="top-badge">上位1%事業リスト付き</span></h1>
        
        <div class="summary">
            <h2 style="color: white; margin-top: 0;">分析概要</h2>
            <div class="metric">
                <div class="metric-value">{total_projects:,}</div>
                <div class="metric-label">総事業数</div>
            </div>
            <div class="metric">
                <div class="metric-value">{projects_with_budget:,}</div>
                <div class="metric-label">予算データ有効事業数</div>
            </div>
            <div class="metric">
                <div class="metric-value">{budget_coverage:.1f}%</div>
                <div class="metric-label">予算データカバー率</div>
            </div>
            <div class="metric">
                <div class="metric-value">{stats['top_1_percent']['count']:,}</div>
                <div class="metric-label">上位1%事業数</div>
            </div>
        </div>
        
        <h2>🎯 主要予算Insight</h2>
        <div class="insight">1. 政府事業総予算額は{budget_stats['total']:,.0f}円（約{budget_stats['total']/1e12:.1f}兆円）</div>
        <div class="insight">2. 上位1%事業の予算閾値は{percentiles['p99']:,.0f}円（約{percentiles['p99']/1e8:.1f}億円）</div>
        <div class="insight">3. 平均事業予算は{budget_stats['mean']:,.0f}円、中央値は{budget_stats['median']:,.0f}円</div>
        <div class="insight">4. 最大事業予算は{budget_stats['max']:,.0f}円（約{budget_stats['max']/1e12:.2f}兆円）</div>
        <div class="insight">5. 上位1%事業が全体予算の{sum(p['current_budget'] for p in top_projects)/budget_stats['total']*100:.1f}%を占める</div>
        
        <h2>📊 予算規模統計</h2>
        <table>
            <tr>
                <th>統計項目</th>
                <th>金額（円）</th>
                <th>金額（億円）</th>
            </tr>
            <tr>
                <td>平均予算額</td>
                <td class="number">{budget_stats['mean']:,.0f}</td>
                <td class="number">{budget_stats['mean']/1e8:.1f}</td>
            </tr>
            <tr>
                <td>中央値</td>
                <td class="number">{budget_stats['median']:,.0f}</td>
                <td class="number">{budget_stats['median']/1e8:.1f}</td>
            </tr>
            <tr>
                <td>99パーセンタイル（上位1%閾値）</td>
                <td class="number">{percentiles['p99']:,.0f}</td>
                <td class="number">{percentiles['p99']/1e8:.1f}</td>
            </tr>
            <tr>
                <td>95パーセンタイル</td>
                <td class="number">{percentiles['p95']:,.0f}</td>
                <td class="number">{percentiles['p95']/1e8:.1f}</td>
            </tr>
            <tr>
                <td>90パーセンタイル</td>
                <td class="number">{percentiles['p90']:,.0f}</td>
                <td class="number">{percentiles['p90']/1e8:.1f}</td>
            </tr>
            <tr>
                <td>75パーセンタイル</td>
                <td class="number">{percentiles['p75']:,.0f}</td>
                <td class="number">{percentiles['p75']/1e8:.1f}</td>
            </tr>
            <tr>
                <td>最大予算額</td>
                <td class="number">{budget_stats['max']:,.0f}</td>
                <td class="number">{budget_stats['max']/1e8:.1f}</td>
            </tr>
            <tr style="background: #f0f9ff; font-weight: bold;">
                <td>総予算額</td>
                <td class="number">{budget_stats['total']:,.0f}</td>
                <td class="number">{budget_stats['total']/1e8:.1f}</td>
            </tr>
        </table>
        
        <h2>🏆 上位1%事業リスト（全事業）</h2>
        <table>
            <tr>
                <th>順位</th>
                <th>事業名</th>
                <th>府省庁</th>
                <th>予算額（円）</th>
                <th>予算額（億円）</th>
                <th>執行率</th>
                <th>規模</th>
            </tr>"""
        
        # 全上位1%事業のテーブル
        for project in top_projects:
            # 予算規模による分類
            budget_amount = project['current_budget']
            if budget_amount >= 1e12:  # 1兆円以上
                tier_class = "tier-mega"
                tier_label = "超大規模"
            elif budget_amount >= 1e11:  # 1000億円以上
                tier_class = "tier-large" 
                tier_label = "大規模"
            else:
                tier_class = "tier-medium"
                tier_label = "中規模"
            
            execution_rate = f"{project['execution_rate']:.1f}%" if project['execution_rate'] > 0 else "未執行"
            
            html_content += f"""
            <tr>
                <td>{project['budget_rank']}</td>
                <td class="project-name">{project['project_name'][:80]}{'...' if len(project['project_name']) > 80 else ''}</td>
                <td>{project['ministry']}</td>
                <td class="number">{budget_amount:,.0f}</td>
                <td class="number">{budget_amount/1e8:.1f}</td>
                <td class="number">{execution_rate}</td>
                <td><span class="budget-tier {tier_class}">{tier_label}</span></td>
            </tr>"""
        
        html_content += """
        </table>
        
        <h2>🏛️ 府省庁別上位1%事業分布</h2>
        <table>
            <tr>
                <th>府省庁</th>
                <th>上位1%事業数</th>
                <th>割合</th>
                <th>総予算額（億円）</th>
                <th>平均予算額（億円）</th>
            </tr>"""
        
        # 府省庁別上位1%分布
        for ministry, data in ministry_analysis['top_projects_ministry_ranking']:
            percentage = (data['project_count'] / len(top_projects)) * 100
            avg_budget = data['total_budget'] / data['project_count'] if data['project_count'] > 0 else 0
            
            html_content += f"""
            <tr>
                <td>{ministry}</td>
                <td class="number">{data['project_count']:,}</td>
                <td class="number">{percentage:.1f}%</td>
                <td class="number">{data['total_budget']/1e8:.1f}</td>
                <td class="number">{avg_budget/1e8:.1f}</td>
            </tr>"""
        
        html_content += f"""
        </table>
        
        <div style="text-align: center; margin-top: 40px; color: #7f8c8d; font-size: 0.9em;">
            2024年度予算分析レポート（上位1%事業リスト付き） - RS Visualization System<br>
            分析日時: {pd.Timestamp.now().strftime('%Y年%m月%d日')} | 
            分析事業数: {total_projects:,} | 上位1%事業数: {len(top_projects):,}
        </div>
    </div>
</body>
</html>"""
        
        # HTMLファイル出力
        html_path = self.output_dir / "budget_analysis_with_top1_report.html"
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"✓ 拡張HTMLレポート生成完了: {html_path}")
        return html_path
    
    def save_analysis_results(self, stats: Dict[str, Any], 
                            top_projects: List[Dict[str, Any]],
                            ministry_analysis: Dict[str, Any]):
        """分析結果をJSON形式で保存"""
        
        results = {
            'analysis_date': pd.Timestamp.now().isoformat(),
            'budget_statistics': stats,
            'top_5_percent_projects': top_projects,
            'ministry_analysis': ministry_analysis,
            'summary': {
                'total_projects': stats['total_projects'],
                'top_5_percent_count': len(top_projects),
                'total_budget': stats['current_budget_stats']['total'],
                'top_5_percent_budget': sum(p['current_budget'] for p in top_projects),
                'top_5_percent_share': (sum(p['current_budget'] for p in top_projects) / 
                                      stats['current_budget_stats']['total']) * 100
            }
        }
        
        json_path = self.output_dir / "budget_analysis_results.json"
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        print(f"✓ JSON結果保存完了: {json_path}")
        return json_path
    
    def run_analysis(self):
        """メイン分析実行"""
        print("================================================================================")
        print("🚀 2024年度予算分析と上位5%事業リスト作成開始")
        print("================================================================================")
        print("目標: 実際の予算額分析、上位5パーセンタイル事業特定、詳細レポート生成")
        print()
        
        if not self.load_data():
            return False
        
        try:
            # 1. 予算データ抽出
            self.extract_budget_data()
            
            # 2. 予算統計計算
            stats = self.calculate_budget_statistics()
            if not stats:
                return False
            
            # 3. 上位5%事業特定
            top_projects = self.identify_top_5_percent_projects(stats)
            
            # 4. 府省庁別分析
            ministry_analysis = self.analyze_ministry_budget_distribution(top_projects)
            
            # 5. 上位5%事業リスト作成
            df_top = self.create_top_projects_list(top_projects)
            
            # 6. 拡張HTMLレポート生成
            html_path = self.create_enhanced_html_report(stats, top_projects, ministry_analysis)
            
            # 7. 結果保存
            json_path = self.save_analysis_results(stats, top_projects, ministry_analysis)
            
            # サマリーテキスト作成
            summary_path = self.output_dir / "executive_summary.txt"
            with open(summary_path, 'w', encoding='utf-8') as f:
                f.write("2024年度予算分析 - エグゼクティブサマリー\n")
                f.write("=" * 50 + "\n\n")
                f.write(f"分析日時: {pd.Timestamp.now().strftime('%Y年%m月%d日 %H:%M')}\n\n")
                f.write("🎯 主要な発見:\n")
                f.write(f"  • 総事業数: {stats['total_projects']:,}\n")
                f.write(f"  • 予算データ有効事業: {stats['projects_with_budget']:,} ({stats['budget_coverage_rate']:.1f}%)\n")
                f.write(f"  • 政府総予算額: {stats['current_budget_stats']['total']:,.0f}円 (約{stats['current_budget_stats']['total']/1e12:.1f}兆円)\n")
                f.write(f"  • 上位5%事業数: {len(top_projects):,}\n")
                f.write(f"  • 上位5%予算閾値: {stats['percentiles']['p95']:,.0f}円 (約{stats['percentiles']['p95']/1e8:.0f}億円)\n")
                f.write(f"  • 上位5%事業の予算集中度: {sum(p['current_budget'] for p in top_projects)/stats['current_budget_stats']['total']*100:.1f}%\n\n")
                
                f.write("📊 予算規模分布:\n")
                f.write(f"  • 平均事業予算: {stats['current_budget_stats']['mean']:,.0f}円\n")
                f.write(f"  • 中央値: {stats['current_budget_stats']['median']:,.0f}円\n")
                f.write(f"  • 最大事業予算: {stats['current_budget_stats']['max']:,.0f}円\n\n")
                
                f.write("🏛️ 上位5%事業の府省庁分布 (TOP5):\n")
                for i, (ministry, data) in enumerate(ministry_analysis['top_projects_ministry_ranking'][:5], 1):
                    percentage = (data['project_count'] / len(top_projects)) * 100
                    f.write(f"  {i}. {ministry}: {data['project_count']}事業 ({percentage:.1f}%)\n")
            
            print("\n================================================================================")
            print("✅ 2024年度予算分析完了")
            print("================================================================================")
            print("生成ファイル:")
            print(f"  📄 拡張HTMLレポート: {html_path}")
            print(f"  📊 上位5%事業リスト(CSV): {self.output_dir}/top_5_percent_projects.csv") 
            print(f"  📈 上位5%事業リスト(Excel): {self.output_dir}/top_5_percent_projects.xlsx")
            print(f"  📋 分析結果JSON: {json_path}")
            print(f"  📝 エグゼクティブサマリー: {summary_path}")
            print()
            print("🎯 主要結果:")
            print(f"  • 上位5%事業数: {len(top_projects):,} ({(len(top_projects)/stats['projects_with_budget']*100):.1f}%)")
            print(f"  • 上位5%予算閾値: {stats['percentiles']['p95']:,.0f}円 (約{stats['percentiles']['p95']/1e8:.0f}億円)")
            print(f"  • 最大事業予算: {stats['current_budget_stats']['max']:,.0f}円 (約{stats['current_budget_stats']['max']/1e12:.2f}兆円)")
            print(f"  • 上位5%予算集中度: {sum(p['current_budget'] for p in top_projects)/stats['current_budget_stats']['total']*100:.1f}%")
            
            return True
            
        except Exception as e:
            print(f"\n❌ 分析中にエラーが発生: {e}")
            import traceback
            traceback.print_exc()
            return False

def main():
    data_path = "data/project_master/rs_project_master_with_details.feather"
    
    analyzer = BudgetAnalyzer(data_path)
    success = analyzer.run_analysis()
    
    if success:
        print("\n🎉 2024年度予算分析が正常に完了しました！")
    else:
        print("\n❌ 分析中にエラーが発生しました")

if __name__ == "__main__":
    main()