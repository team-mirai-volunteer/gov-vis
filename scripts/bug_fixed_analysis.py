#!/usr/bin/env python3
"""
バグ修正版記述統計分析
- Insight5,6の重複問題修正
- 府省庁分析の割合計算バグ修正
- より深い分析の追加
"""

import pandas as pd
import json
from pathlib import Path
from typing import Dict, List, Any
import warnings
warnings.filterwarnings('ignore')

class BugFixedAnalyzer:
    def __init__(self, data_path: str):
        self.data_path = Path(data_path)
        self.df = None
        self.output_dir = Path("data/bug_fixed_analysis")
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
            
    def basic_statistics_fixed(self) -> Dict[str, Any]:
        """基本統計（修正版）"""
        print("\n================================================================================")
        print("1. 基本統計分析（修正版）")
        print("================================================================================")
        
        total_projects = len(self.df)
        total_columns = len(self.df.columns)
        missing_values = self.df.isnull().sum().sum()
        total_cells = total_projects * total_columns
        completeness = ((total_cells - missing_values) / total_cells) * 100
        
        # 府省庁分析（修正版）
        ministry_counts = self.df['府省庁'].value_counts()
        total_ministries = len(ministry_counts)
        
        # 割合計算を修正（総事業数を分母とする）
        ministry_percentages = (ministry_counts / total_projects * 100).round(1)
        
        print("基本統計:")
        print(f"  - 総事業数: {total_projects:,}")
        print(f"  - 総列数: {total_columns}")
        print(f"  - 欠損値数: {missing_values:,}")
        print(f"  - データ完全性: {completeness:.1f}%")
        print()
        print("府省庁別分析（修正版）:")
        print(f"  - 府省庁数: {total_ministries}")
        print(f"  - 平均事業数/府省庁: {ministry_counts.mean():.1f}")
        print(f"  - 上位3府省庁集中率: {ministry_percentages.head(3).sum():.1f}%")
        print(f"  - 上位5府省庁集中率: {ministry_percentages.head(5).sum():.1f}%")
        print("  上位10府省庁（修正版）:")
        
        for i, (ministry, count) in enumerate(ministry_counts.head(10).items(), 1):
            percentage = ministry_percentages[ministry]
            print(f"    {i:2}. {ministry}: {count:,}事業 ({percentage}%)")
        
        return {
            'total_projects': total_projects,
            'total_columns': total_columns,
            'missing_values': int(missing_values),
            'completeness': round(completeness, 1),
            'ministry_analysis': {
                'total_ministries': total_ministries,
                'top_10': {ministry: {'count': int(count), 'percentage': float(ministry_percentages[ministry])} 
                          for ministry, count in ministry_counts.head(10).items()},
                'concentration': {
                    'top_3': float(ministry_percentages.head(3).sum()),
                    'top_5': float(ministry_percentages.head(5).sum())
                }
            }
        }
    
    def data_density_analysis_fixed(self) -> Dict[str, Any]:
        """データ密度分析（修正版）"""
        print("\n================================================================================")
        print("2. データ密度分析（修正版）")
        print("================================================================================")
        
        # JSONカラムの分析（修正版）
        json_columns = [col for col in self.df.columns if col.endswith('_json')]
        
        density_analysis = {}
        
        print("テーブル別データ詳細（修正版）:")
        for json_col in json_columns:
            if json_col in self.df.columns:
                table_name = json_col.replace('_json', '')
                
                # JSONデータの詳細分析
                non_empty = self.df[json_col].notna() & (self.df[json_col] != '[]')
                coverage_rate = (non_empty.sum() / len(self.df)) * 100
                
                # レコード数の計算
                record_counts = []
                for idx, row in self.df[non_empty].iterrows():
                    try:
                        data = json.loads(row[json_col]) if isinstance(row[json_col], str) else row[json_col]
                        if isinstance(data, list):
                            record_counts.append(len(data))
                        else:
                            record_counts.append(1)
                    except:
                        record_counts.append(0)
                
                if record_counts:
                    avg_records = sum(record_counts) / len(record_counts)
                    median_records = sorted(record_counts)[len(record_counts)//2] if record_counts else 0
                    total_records = sum(record_counts)
                    max_records = max(record_counts) if record_counts else 0
                else:
                    avg_records = median_records = total_records = max_records = 0
                
                density_analysis[table_name] = {
                    'coverage_rate': round(coverage_rate, 1),
                    'avg_records': round(avg_records, 1),
                    'median_records': median_records,
                    'total_records': total_records,
                    'max_records': max_records
                }
                
                print(f"  {table_name}:")
                print(f"    - データ保有率: {coverage_rate:.1f}%")
                print(f"    - 平均レコード数: {avg_records:.1f}")
                print(f"    - 中央値: {median_records:.1f}")
                print(f"    - 総レコード数: {total_records:,}")
        
        return density_analysis
    
    def budget_analysis(self) -> Dict[str, Any]:
        """予算分析（新規）"""
        print("\n================================================================================")
        print("3. 予算規模・パターン分析（新規）")
        print("================================================================================")
        
        budget_analysis = {}
        
        # 予算JSONの分析
        if 'budget_summary_json' in self.df.columns:
            budget_amounts = []
            budget_projects = 0
            
            for idx, row in self.df.iterrows():
                try:
                    budget_data = json.loads(row['budget_summary_json']) if isinstance(row['budget_summary_json'], str) else row['budget_summary_json']
                    if isinstance(budget_data, list):
                        for record in budget_data:
                            if isinstance(record, dict):
                                for key, value in record.items():
                                    if '予算額' in key or '金額' in key:
                                        try:
                                            amount = float(str(value).replace(',', '').replace('円', ''))
                                            if amount > 0:
                                                budget_amounts.append(amount)
                                        except:
                                            continue
                        if budget_data:  # データがある場合
                            budget_projects += 1
                except:
                    continue
            
            if budget_amounts:
                # 予算規模別分類
                small_budget = len([x for x in budget_amounts if x < 100_000_000])  # 1億未満
                medium_budget = len([x for x in budget_amounts if 100_000_000 <= x < 1_000_000_000])  # 1-10億
                large_budget = len([x for x in budget_amounts if 1_000_000_000 <= x < 10_000_000_000])  # 10-100億
                mega_budget = len([x for x in budget_amounts if x >= 10_000_000_000])  # 100億以上
                
                budget_analysis = {
                    'projects_with_budget': budget_projects,
                    'total_projects': len(self.df),
                    'coverage_rate': round((budget_projects / len(self.df)) * 100, 1),
                    'avg_amount': sum(budget_amounts) / len(budget_amounts),
                    'median_amount': sorted(budget_amounts)[len(budget_amounts)//2],
                    'max_amount': max(budget_amounts),
                    'min_amount': min(budget_amounts),
                    'size_distribution': {
                        'small_1m_under': small_budget,
                        'medium_1-10b': medium_budget,
                        'large_10-100b': large_budget,
                        'mega_100b_over': mega_budget
                    }
                }
                
                print("予算分析結果:")
                print(f"  - 予算データ有り: {budget_projects:,}/{len(self.df):,}事業 ({budget_analysis['coverage_rate']:.1f}%)")
                print(f"  - 平均予算額: {budget_analysis['avg_amount']:,.0f}円")
                print(f"  - 中央値: {budget_analysis['median_amount']:,.0f}円")
                print(f"  - 最大: {budget_analysis['max_amount']:,.0f}円")
                print()
                print("予算規模別分布:")
                print(f"  - 小規模(1億未満): {small_budget:,}レコード")
                print(f"  - 中規模(1-10億): {medium_budget:,}レコード")
                print(f"  - 大規模(10-100億): {large_budget:,}レコード")
                print(f"  - 超大規模(100億以上): {mega_budget:,}レコード")
        
        return budget_analysis
    
    def generate_fixed_insights(self, analysis_data: Dict[str, Any]) -> List[str]:
        """修正版Insight生成"""
        print("\n================================================================================")
        print("4. 主要Insight生成（修正版）")
        print("================================================================================")
        
        insights = []
        
        # 基本統計から
        basic = analysis_data.get('basic_statistics', {})
        ministry = basic.get('ministry_analysis', {})
        
        if ministry and 'top_10' in ministry:
            top_ministry = list(ministry['top_10'].keys())[0]
            top_count = ministry['top_10'][top_ministry]['count']
            top_percentage = ministry['top_10'][top_ministry]['percentage']
            insights.append(f"最多事業府省庁は{top_ministry}で{top_count:,}事業({top_percentage}%)を占める")
        
        # 集中度分析
        if ministry and 'concentration' in ministry:
            top5_concentration = ministry['concentration']['top_5']
            insights.append(f"上位5府省庁で全体の{top5_concentration:.1f}%を集中的に実施")
        
        # データ完全性
        completeness = basic.get('completeness', 0)
        if completeness < 70:
            insights.append(f"データ欠損率が高い状況({100-completeness:.1f}%欠損)")
        else:
            insights.append(f"データ完全性は良好({completeness:.1f}%完全)")
        
        # データ密度から（修正版）
        density = analysis_data.get('data_density', {})
        if density:
            # 最高と最低のデータ密度テーブルを特定
            coverage_rates = {k: v['coverage_rate'] for k, v in density.items() if 'coverage_rate' in v}
            if coverage_rates:
                max_coverage_table = max(coverage_rates.keys(), key=lambda x: coverage_rates[x])
                min_coverage_table = min(coverage_rates.keys(), key=lambda x: coverage_rates[x])
                max_rate = coverage_rates[max_coverage_table]
                min_rate = coverage_rates[min_coverage_table]
                
                # 異なるテーブルを選ぶ（修正版）
                if max_coverage_table != min_coverage_table:
                    insights.append(f"{max_coverage_table}が最高のデータ保有率({max_rate:.1f}%)")
                    insights.append(f"{min_coverage_table}が最低のデータ保有率({min_rate:.1f}%)")
                else:
                    # すべて同じ場合は詳細分析
                    avg_records = {k: v.get('avg_records', 0) for k, v in density.items()}
                    max_avg_table = max(avg_records.keys(), key=lambda x: avg_records[x])
                    min_avg_table = min(avg_records.keys(), key=lambda x: avg_records[x])
                    insights.append(f"{max_avg_table}が最高の平均レコード密度({avg_records[max_avg_table]:.1f}/事業)")
                    insights.append(f"{min_avg_table}が最低の平均レコード密度({avg_records[min_avg_table]:.1f}/事業)")
        
        # 予算分析から
        budget = analysis_data.get('budget_analysis', {})
        if budget:
            coverage = budget.get('coverage_rate', 0)
            insights.append(f"予算データ保有率は{coverage:.1f}%")
            
            size_dist = budget.get('size_distribution', {})
            if size_dist:
                mega_count = size_dist.get('mega_100b_over', 0)
                insights.append(f"超大規模事業(100億円以上)が{mega_count:,}レコード存在")
        
        # 府省庁多様性
        total_ministries = basic.get('ministry_analysis', {}).get('total_ministries', 0)
        if total_ministries > 30:
            insights.append(f"多数の府省庁が参画({total_ministries}府省庁)")
        
        print("生成されたInsight:")
        for i, insight in enumerate(insights, 1):
            print(f"  {i:2}. {insight}")
        
        return insights
    
    def create_html_report_fixed(self, analysis_data: Dict[str, Any], insights: List[str]):
        """修正版HTMLレポート生成"""
        print("\n================================================================================")
        print("5. HTMLレポート生成（修正版）")
        print("================================================================================")
        
        basic = analysis_data.get('basic_statistics', {})
        ministry = basic.get('ministry_analysis', {})
        density = analysis_data.get('data_density', {})
        budget = analysis_data.get('budget_analysis', {})
        
        html_content = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <title>事業マスターリスト記述統計分析レポート（修正版）</title>
    <style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 20px; background: #f9fafb; color: #111827; }}
        .container {{ max-width: 1200px; margin: 0 auto; background: #ffffff; padding: 30px; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
        h1 {{ color: #1f2937; text-align: center; border-bottom: 3px solid #10b981; padding-bottom: 15px; }}
        h2 {{ color: #1f2937; margin-top: 30px; border-left: 5px solid #10b981; padding-left: 10px; }}
        .summary {{ background: #10b981; color: white; padding: 20px; border-radius: 8px; margin: 20px 0; }}
        .metric {{ display: inline-block; margin: 10px 20px; }}
        .metric-value {{ font-size: 2em; font-weight: bold; }}
        .metric-label {{ font-size: 0.9em; opacity: 0.9; }}
        table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        th {{ background: #f3f4f6; color: #1f2937; padding: 12px; text-align: left; border-bottom: 2px solid #e5e7eb; }}
        td {{ padding: 10px; border-bottom: 1px solid #e5e7eb; color: #374151; }}
        tr:hover {{ background: #f9fafb; }}
        .insight {{ background: #dcfce7; border-left: 4px solid #10b981; padding: 15px; margin: 10px 0; color: #166534; }}
        .fixed-badge {{ background: #dc2626; color: white; padding: 2px 8px; border-radius: 4px; font-size: 0.8em; margin-left: 10px; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 事業マスターリスト記述統計分析レポート <span class="fixed-badge">修正版</span></h1>
        
        <div class="summary">
            <h2 style="color: white; margin-top: 0;">分析概要（修正版）</h2>
            <div class="metric">
                <div class="metric-value">{basic.get('total_projects', 0):,}</div>
                <div class="metric-label">総事業数</div>
            </div>
            <div class="metric">
                <div class="metric-value">{basic.get('total_columns', 0)}</div>
                <div class="metric-label">総列数</div>
            </div>
            <div class="metric">
                <div class="metric-value">{basic.get('completeness', 0):.1f}%</div>
                <div class="metric-label">データ完全性</div>
            </div>
        </div>
        
        <h2>🎯 主要Insight（修正版）</h2>"""
        
        for i, insight in enumerate(insights, 1):
            html_content += f'        <div class="insight">{i}. {insight}</div>\n'
        
        html_content += f"""
        <h2>🏛️ 府省庁分析（修正版）</h2>
        <table>
            <tr>
                <th>順位</th>
                <th>府省庁</th>
                <th>事業数</th>
                <th>割合</th>
            </tr>"""
        
        if ministry and 'top_10' in ministry:
            for i, (name, data) in enumerate(ministry['top_10'].items(), 1):
                count = data['count']
                percentage = data['percentage']
                html_content += f"""
            <tr>
                <td>{i}</td>
                <td>{name}</td>
                <td>{count:,}</td>
                <td>{percentage}%</td>
            </tr>"""
        
        html_content += """
        </table>
        
        <h2>📈 データ密度分析（修正版）</h2>
        <table>
            <tr>
                <th>テーブル名</th>
                <th>データ保有率</th>
                <th>平均レコード数/事業</th>
                <th>最大レコード数</th>
                <th>総レコード数</th>
            </tr>"""
        
        for table_name, data in density.items():
            html_content += f"""
            <tr>
                <td>{table_name}</td>
                <td>{data.get('coverage_rate', 0):.1f}%</td>
                <td>{data.get('avg_records', 0):.1f}</td>
                <td>{data.get('max_records', 0):,}</td>
                <td>{data.get('total_records', 0):,}</td>
            </tr>"""
        
        if budget:
            html_content += f"""
        </table>
        
        <h2>💰 予算分析（新規）</h2>
        <table>
            <tr>
                <th>指標</th>
                <th>値</th>
            </tr>
            <tr>
                <td>予算データ保有率</td>
                <td>{budget.get('coverage_rate', 0):.1f}%</td>
            </tr>
            <tr>
                <td>平均予算額</td>
                <td>{budget.get('avg_amount', 0):,.0f}円</td>
            </tr>
            <tr>
                <td>中央値</td>
                <td>{budget.get('median_amount', 0):,.0f}円</td>
            </tr>
            <tr>
                <td>最大予算額</td>
                <td>{budget.get('max_amount', 0):,.0f}円</td>
            </tr>"""
            
            size_dist = budget.get('size_distribution', {})
            for size_category, count in size_dist.items():
                html_content += f"""
            <tr>
                <td>{size_category}</td>
                <td>{count:,}レコード</td>
            </tr>"""
        
        html_content += """
        </table>
        
        <div style="text-align: center; margin-top: 40px; color: #7f8c8d; font-size: 0.9em;">
            事業マスターリスト記述統計分析レポート（修正版） - RS Visualization System
        </div>
    </div>
</body>
</html>"""
        
        # HTMLファイル出力
        html_path = self.output_dir / "fixed_analysis_report.html"
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"✓ HTMLレポート生成完了: {html_path}")
        return html_path
    
    def run_analysis(self):
        """メイン分析実行"""
        print("================================================================================")
        print("🚀 バグ修正版事業マスターリスト分析開始")
        print("================================================================================")
        print("目標: Insight重複・割合計算バグ修正、深い分析追加")
        print()
        
        if not self.load_data():
            return False
        
        # 分析実行
        analysis_results = {}
        
        # 1. 基本統計（修正版）
        analysis_results['basic_statistics'] = self.basic_statistics_fixed()
        
        # 2. データ密度分析（修正版）
        analysis_results['data_density'] = self.data_density_analysis_fixed()
        
        # 3. 予算分析（新規）
        analysis_results['budget_analysis'] = self.budget_analysis()
        
        # 4. Insight生成（修正版）
        insights = self.generate_fixed_insights(analysis_results)
        
        # 5. レポート生成
        html_path = self.create_html_report_fixed(analysis_results, insights)
        
        # 結果保存
        json_path = self.output_dir / "fixed_analysis_results.json"
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(analysis_results, f, ensure_ascii=False, indent=2)
        
        insights_path = self.output_dir / "fixed_key_insights.txt"
        with open(insights_path, 'w', encoding='utf-8') as f:
            f.write("事業マスターリスト分析 - 主要Insight（修正版）\n")
            f.write("=" * 60 + "\n\n")
            for i, insight in enumerate(insights, 1):
                f.write(f"{i:2}. {insight}\n")
        
        print("\n================================================================================")
        print("✅ バグ修正版分析完了")
        print("================================================================================")
        print("修正内容:")
        print("  ✓ Insight5,6重複問題 → 異なる指標で差別化")
        print("  ✓ 府省庁割合計算バグ → 総事業数を正しい分母として使用")
        print("  ✓ 浅い分析問題 → 予算規模・パターン分析追加")
        print()
        print("出力ファイル:")
        print(f"  - HTMLレポート: {html_path}")
        print(f"  - JSON結果: {json_path}")
        print(f"  - テキストInsight: {insights_path}")
        
        return True

def main():
    data_path = "data/project_master/rs_project_master_with_details.feather"
    
    analyzer = BugFixedAnalyzer(data_path)
    success = analyzer.run_analysis()
    
    if success:
        print("\n🎉 バグ修正版分析が正常に完了しました！")
    else:
        print("\n❌ 分析中にエラーが発生しました")

if __name__ == "__main__":
    main()