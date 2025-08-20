#!/usr/bin/env python3
"""
2024年度予算分析と上位1%事業リスト作成（簡略版）
- NumPy互換性問題を回避
- 基本的な統計計算のみ使用
- 上位1%事業リスト生成
"""

import json
import csv
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# パンダスなしで基本操作
def load_feather_data(file_path):
    """featherファイル読み込み（pandas使用）"""
    try:
        import pandas as pd
        df = pd.read_feather(file_path)
        return df.to_dict('records')
    except Exception as e:
        print(f"データ読み込みエラー: {e}")
        return []

def extract_budget_amounts():
    """予算額抽出と上位1%特定"""
    print("================================================================================")
    print("🚀 2024年度予算分析と上位1%事業特定")
    print("================================================================================")
    
    # データ読み込み
    data_path = "data/project_master/rs_project_master_with_details.feather"
    print(f"📊 データ読み込み: {data_path}")
    
    records = load_feather_data(data_path)
    if not records:
        return False
    
    print(f"✓ データ読み込み完了: {len(records):,}行")
    
    # 予算データ抽出
    print("\n予算データ抽出中...")
    budget_projects = []
    valid_count = 0
    error_count = 0
    
    for i, record in enumerate(records):
        try:
            project_info = {
                'project_id': record.get('予算事業ID', ''),
                'project_name': record.get('事業名', ''),
                'ministry': record.get('府省庁', ''),
                'agency': record.get('局・庁', ''),
                'fiscal_year': record.get('事業年度', 2024),
                'current_budget': 0,
                'initial_budget': 0,
                'execution_amount': 0,
                'next_year_request': 0
            }
            
            # 予算JSON処理
            budget_json_str = record.get('budget_summary_json', '')
            if budget_json_str and budget_json_str != '[]':
                try:
                    budget_data = json.loads(budget_json_str)
                    if isinstance(budget_data, list) and len(budget_data) > 0:
                        # 2024年データまたは最初のレコードを使用
                        budget_record = None
                        for item in budget_data:
                            if isinstance(item, dict):
                                year = item.get('予算年度', 0)
                                if year == 2024:
                                    budget_record = item
                                    break
                        
                        if not budget_record and budget_data:
                            budget_record = budget_data[0]
                        
                        if budget_record and isinstance(budget_record, dict):
                            # 予算額取得
                            current_budget = budget_record.get('計（歳出予算現額合計）', 0)
                            initial_budget = budget_record.get('当初予算（合計）', 0)
                            execution_amount = budget_record.get('執行額（合計）', 0)
                            next_year_request = budget_record.get('翌年度要求額（合計）', 0)
                            
                            # 数値変換
                            if isinstance(current_budget, (int, float)) and current_budget > 0:
                                project_info['current_budget'] = float(current_budget)
                                project_info['initial_budget'] = float(initial_budget) if isinstance(initial_budget, (int, float)) else 0
                                project_info['execution_amount'] = float(execution_amount) if isinstance(execution_amount, (int, float)) else 0
                                project_info['next_year_request'] = float(next_year_request) if isinstance(next_year_request, (int, float)) else 0
                                
                                # 執行率計算
                                if current_budget > 0:
                                    project_info['execution_rate'] = (project_info['execution_amount'] / current_budget) * 100
                                else:
                                    project_info['execution_rate'] = 0
                                
                                valid_count += 1
                
                except json.JSONDecodeError:
                    error_count += 1
                    continue
                except Exception:
                    error_count += 1
                    continue
            
            budget_projects.append(project_info)
            
        except Exception:
            error_count += 1
            continue
    
    print(f"✓ 予算データ抽出完了")
    print(f"  - 総事業数: {len(budget_projects):,}")
    print(f"  - 有効な予算データ: {valid_count:,}")
    print(f"  - 抽出エラー: {error_count:,}")
    print(f"  - 有効率: {(valid_count/len(budget_projects)*100):.1f}%")
    
    if valid_count == 0:
        print("❌ 有効な予算データが見つかりません")
        return False
    
    # 予算額でフィルタとソート
    valid_projects = [p for p in budget_projects if p['current_budget'] > 0]
    valid_projects.sort(key=lambda x: x['current_budget'], reverse=True)
    
    # 統計計算
    budgets = [p['current_budget'] for p in valid_projects]
    total_budget = sum(budgets)
    avg_budget = total_budget / len(budgets)
    max_budget = max(budgets)
    min_budget = min(budgets)
    
    # 中央値計算（簡易版）
    sorted_budgets = sorted(budgets)
    n = len(sorted_budgets)
    median_budget = sorted_budgets[n//2] if n % 2 == 1 else (sorted_budgets[n//2-1] + sorted_budgets[n//2]) / 2
    
    # 99パーセンタイル計算（上位1%閾値）
    percentile_99_index = int(n * 0.99)
    percentile_99 = sorted_budgets[percentile_99_index] if percentile_99_index < n else sorted_budgets[-1]
    
    # 上位1%事業フィルタ
    top_1_percent = [p for p in valid_projects if p['current_budget'] >= percentile_99]
    
    print(f"\n================================================================================")
    print("📊 2024年度予算統計")
    print("================================================================================")
    print(f"総予算額: {total_budget:,.0f}円 (約{total_budget/1e12:.1f}兆円)")
    print(f"平均予算額: {avg_budget:,.0f}円")
    print(f"中央値: {median_budget:,.0f}円") 
    print(f"最大予算額: {max_budget:,.0f}円 (約{max_budget/1e12:.2f}兆円)")
    print(f"最小予算額: {min_budget:,.0f}円")
    print()
    print(f"上位1%閾値: {percentile_99:,.0f}円 (約{percentile_99/1e8:.0f}億円)")
    print(f"上位1%事業数: {len(top_1_percent):,} ({len(top_1_percent)/len(valid_projects)*100:.1f}%)")
    print(f"上位1%予算集中度: {sum(p['current_budget'] for p in top_1_percent)/total_budget*100:.1f}%")
    
    # 上位1%事業リスト表示
    print(f"\n================================================================================")
    print("🏆 上位1%事業リスト")
    print("================================================================================")
    
    for i, project in enumerate(top_1_percent, 1):
        project['rank'] = i
        print(f"{i:2}. {project['project_name'][:60]}...")
        print(f"    府省庁: {project['ministry']}")
        print(f"    予算額: {project['current_budget']:,.0f}円 (約{project['current_budget']/1e8:.1f}億円)")
        print(f"    執行率: {project['execution_rate']:.1f}%")
        print()
    
    # CSV出力
    output_dir = Path("data/budget_analysis_2024")
    output_dir.mkdir(exist_ok=True)
    
    csv_path = output_dir / "top_1_percent_projects_simple.csv"
    
    print(f"================================================================================")
    print("💾 CSV出力")
    print("================================================================================")
    
    with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
        fieldnames = [
            'ランキング', '事業名', '府省庁', '局・庁', 
            '当初予算額', '現行予算額', '執行額', '執行率(%)', 
            '次年度要求額', '事業ID', '年度'
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for project in top_1_percent:
            writer.writerow({
                'ランキング': project['rank'],
                '事業名': project['project_name'],
                '府省庁': project['ministry'],
                '局・庁': project['agency'],
                '当初予算額': int(project['initial_budget']),
                '現行予算額': int(project['current_budget']),
                '執行額': int(project['execution_amount']),
                '執行率(%)': round(project['execution_rate'], 1),
                '次年度要求額': int(project['next_year_request']),
                '事業ID': project['project_id'],
                '年度': project['fiscal_year']
            })
    
    print(f"✓ CSV出力完了: {csv_path}")
    print(f"  出力事業数: {len(top_1_percent):,}")
    
    # 府省庁別分析
    ministry_stats = {}
    for project in top_1_percent:
        ministry = project['ministry']
        if ministry not in ministry_stats:
            ministry_stats[ministry] = {
                'count': 0,
                'total_budget': 0
            }
        ministry_stats[ministry]['count'] += 1
        ministry_stats[ministry]['total_budget'] += project['current_budget']
    
    # 府省庁ランキング
    ministry_ranking = sorted(ministry_stats.items(), key=lambda x: x[1]['count'], reverse=True)
    
    print(f"\n================================================================================")
    print("🏛️ 府省庁別上位1%事業分布")
    print("================================================================================")
    
    for ministry, stats in ministry_ranking:
        percentage = (stats['count'] / len(top_1_percent)) * 100
        avg_budget = stats['total_budget'] / stats['count']
        print(f"{ministry}: {stats['count']}事業 ({percentage:.1f}%) - 平均{avg_budget/1e8:.1f}億円")
    
    # 簡易HTMLレポート作成
    html_path = output_dir / "top_1_percent_report_simple.html"
    
    html_content = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <title>2024年度予算分析 - 上位1%事業リスト</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        h1 {{ color: #2c3e50; text-align: center; }}
        .stats {{ background: #ecf0f1; padding: 20px; margin: 20px 0; border-radius: 5px; }}
        table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        th, td {{ padding: 10px; border: 1px solid #ddd; text-align: left; }}
        th {{ background: #3498db; color: white; }}
        .number {{ text-align: right; }}
    </style>
</head>
<body>
    <h1>💰 2024年度予算分析 - 上位1%事業リスト</h1>
    
    <div class="stats">
        <h2>📊 統計サマリー</h2>
        <p><strong>総事業数:</strong> {len(valid_projects):,}</p>
        <p><strong>総予算額:</strong> {total_budget:,.0f}円 (約{total_budget/1e12:.1f}兆円)</p>
        <p><strong>上位1%閾値:</strong> {percentile_99:,.0f}円 (約{percentile_99/1e8:.0f}億円)</p>
        <p><strong>上位1%事業数:</strong> {len(top_1_percent):,}事業</p>
        <p><strong>上位1%予算集中度:</strong> {sum(p['current_budget'] for p in top_1_percent)/total_budget*100:.1f}%</p>
    </div>
    
    <h2>🏆 上位1%事業リスト</h2>
    <table>
        <tr>
            <th>順位</th>
            <th>事業名</th>
            <th>府省庁</th>
            <th>予算額（億円）</th>
            <th>執行率</th>
        </tr>"""
    
    for project in top_1_percent:
        html_content += f"""
        <tr>
            <td>{project['rank']}</td>
            <td>{project['project_name'][:80]}{'...' if len(project['project_name']) > 80 else ''}</td>
            <td>{project['ministry']}</td>
            <td class="number">{project['current_budget']/1e8:.1f}</td>
            <td class="number">{project['execution_rate']:.1f}%</td>
        </tr>"""
    
    html_content += f"""
    </table>
    
    <h2>🏛️ 府省庁別分布</h2>
    <table>
        <tr>
            <th>府省庁</th>
            <th>事業数</th>
            <th>割合</th>
            <th>平均予算額（億円）</th>
        </tr>"""
    
    for ministry, stats in ministry_ranking:
        percentage = (stats['count'] / len(top_1_percent)) * 100
        avg_budget = stats['total_budget'] / stats['count']
        html_content += f"""
        <tr>
            <td>{ministry}</td>
            <td class="number">{stats['count']}</td>
            <td class="number">{percentage:.1f}%</td>
            <td class="number">{avg_budget/1e8:.1f}</td>
        </tr>"""
    
    html_content += f"""
    </table>
    
    <div style="text-align: center; margin-top: 40px; color: #7f8c8d;">
        <p>2024年度予算分析レポート - 生成日時: {Path().absolute().name}</p>
    </div>
    
</body>
</html>"""
    
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"✓ HTMLレポート出力完了: {html_path}")
    
    print(f"\n🎉 分析完了! 上位1%事業（{len(top_1_percent)}事業）を特定しました。")
    print(f"予算規模: {percentile_99/1e8:.0f}億円以上")
    print(f"最大事業: {max_budget/1e12:.2f}兆円")
    
    return True

if __name__ == "__main__":
    extract_budget_amounts()