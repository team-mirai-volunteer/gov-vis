#!/usr/bin/env python3
"""
強化版事業マスターリスト記述統計分析
深い洞察とバグ修正を含む包括的な分析
"""
import pandas as pd
import numpy as np
import json
from pathlib import Path
from typing import Dict, List, Any, Tuple
import warnings
import re
from collections import Counter
warnings.filterwarnings('ignore')

# 可視化ライブラリの条件付きインポート
try:
    import matplotlib.pyplot as plt
    import seaborn as sns
    plt.rcParams['font.family'] = 'DejaVu Sans'
    sns.set_style("whitegrid")
    sns.set_palette("husl")
    VISUALIZATION_AVAILABLE = True
except ImportError:
    VISUALIZATION_AVAILABLE = False
    print("⚠️ 可視化ライブラリが利用できません。テキストベースの分析のみ実行します。")


class EnhancedProjectAnalyzer:
    """強化版事業マスターリスト分析クラス"""
    
    def __init__(self):
        self.data_dir = Path("data")
        self.project_master_path = self.data_dir / "project_master" / "rs_project_master_with_details.feather"
        self.output_dir = self.data_dir / "enhanced_project_analysis"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.df = None
        self.analysis_results = {}
        self.insights = []
        
        # 分析対象の列定義
        self.count_cols = [
            'budget_summary_count', 'budget_items_count', 'goals_performance_count',
            'goal_connections_count', 'evaluations_count', 'expenditure_info_count',
            'expenditure_connections_count', 'expenditure_details_count', 'contracts_count'
        ]
        
        self.json_cols = [
            'budget_summary_json', 'budget_items_json', 'goals_performance_json',
            'goal_connections_json', 'evaluations_json', 'expenditure_info_json',
            'expenditure_connections_json', 'expenditure_details_json', 'contracts_json'
        ]
    
    def load_data(self):
        """データを読み込み"""
        print("\n" + "="*80)
        print("強化版データ分析開始")
        print("="*80)
        
        if not self.project_master_path.exists():
            raise FileNotFoundError(f"事業マスターリストが見つかりません: {self.project_master_path}")
        
        try:
            self.df = pd.read_feather(self.project_master_path)
            print(f"✓ データ読み込み完了: {len(self.df):,}行 × {len(self.df.columns)}列")
            print(f"  - 事業数: {len(self.df):,}")
            print(f"  - 列数: {len(self.df.columns)}")
            print(f"  - データサイズ: {self.df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
            
            return True
            
        except Exception as e:
            print(f"✗ データ読み込みエラー: {e}")
            return False
    
    def basic_statistics(self):
        """修正された基本統計分析"""
        print("\n" + "="*80)
        print("1. 基本統計分析（修正版）")
        print("="*80)
        
        basic_stats = {
            'total_projects': len(self.df),
            'total_columns': len(self.df.columns),
            'missing_values': self.df.isnull().sum().sum(),
            'data_completeness': (1 - self.df.isnull().sum().sum() / (len(self.df) * len(self.df.columns))) * 100
        }
        
        print(f"基本統計:")
        print(f"  - 総事業数: {basic_stats['total_projects']:,}")
        print(f"  - 総列数: {basic_stats['total_columns']}")
        print(f"  - 欠損値数: {basic_stats['missing_values']:,}")
        print(f"  - データ完全性: {basic_stats['data_completeness']:.1f}%")
        
        self.analysis_results['basic_statistics'] = basic_stats
        return basic_stats
    
    def ministry_analysis_fixed(self):
        """修正された府省庁別分析"""
        print("\n府省庁別分析（修正版）:")
        
        ministry_counts = self.df['府省庁'].value_counts()
        
        analysis = {
            'total_ministries': len(ministry_counts),
            'total_projects': len(self.df),  # 修正: 正確な事業総数
            'ministry_distribution': ministry_counts.to_dict(),
            'top_10_ministries': ministry_counts.head(10).to_dict(),
            'ministry_stats': {
                'mean': ministry_counts.mean(),
                'median': ministry_counts.median(),
                'std': ministry_counts.std(),
                'max': ministry_counts.max(),
                'min': ministry_counts.min(),
                'concentration_ratio_top3': (ministry_counts.head(3).sum() / len(self.df)) * 100,
                'concentration_ratio_top5': (ministry_counts.head(5).sum() / len(self.df)) * 100
            }
        }
        
        print(f"  - 府省庁数: {analysis['total_ministries']}")
        print(f"  - 平均事業数/府省庁: {analysis['ministry_stats']['mean']:.1f}")
        print(f"  - 上位3府省庁集中率: {analysis['ministry_stats']['concentration_ratio_top3']:.1f}%")
        print(f"  - 上位5府省庁集中率: {analysis['ministry_stats']['concentration_ratio_top5']:.1f}%")
        
        print("  上位10府省庁（修正版）:")
        for i, (ministry, count) in enumerate(ministry_counts.head(10).items(), 1):
            percentage = (count / len(self.df)) * 100  # 修正: 正しい分母
            print(f"    {i:2d}. {ministry}: {count:,}事業 ({percentage:.1f}%)")
        
        self.analysis_results['ministry_analysis'] = analysis
        
        # 修正されたInsight
        top_ministry = ministry_counts.index[0]
        top_count = ministry_counts.iloc[0]
        top_percentage = (top_count / len(self.df)) * 100
        
        self.insights.append(f"最多事業府省庁は{top_ministry}で{top_count:,}事業({top_percentage:.1f}%)を占める")
        self.insights.append(f"上位3府省庁で全事業の{analysis['ministry_stats']['concentration_ratio_top3']:.1f}%を占有（集中度高）")
        
        return analysis
    
    def data_density_analysis_fixed(self):
        """修正されたデータ密度分析"""
        print("\n" + "="*80)
        print("2. データ密度分析（修正版）")
        print("="*80)
        
        data_availability = {}
        
        for col in self.count_cols:
            if col in self.df.columns:
                table_name = col.replace('_count', '')
                record_counts = self.df[col]
                has_data = record_counts > 0
                
                data_availability[table_name] = {
                    'projects_with_data': has_data.sum(),
                    'coverage_rate': (has_data.sum() / len(self.df)) * 100,
                    'avg_records_per_project': record_counts.mean(),
                    'median_records_per_project': record_counts.median(),
                    'max_records': record_counts.max(),
                    'std_records': record_counts.std(),
                    'total_records': record_counts.sum()
                }
        
        # Total related recordsの分析
        total_records_stats = {}
        if 'total_related_records' in self.df.columns:
            total_records = self.df['total_related_records']
            total_records_stats = {
                'mean': total_records.mean(),
                'median': total_records.median(),
                'std': total_records.std(),
                'min': total_records.min(),
                'max': total_records.max(),
                'percentiles': {
                    '25th': total_records.quantile(0.25),
                    '75th': total_records.quantile(0.75),
                    '90th': total_records.quantile(0.90),
                    '95th': total_records.quantile(0.95)
                }
            }
        
        analysis = {
            'data_availability': data_availability,
            'total_records_stats': total_records_stats
        }
        
        print("テーブル別データ詳細（修正版）:")
        for table_name, stats in data_availability.items():
            print(f"  {table_name}:")
            print(f"    - データ保有率: {stats['coverage_rate']:.1f}%")
            print(f"    - 平均レコード数: {stats['avg_records_per_project']:.1f}")
            print(f"    - 中央値: {stats['median_records_per_project']:.1f}")
            print(f"    - 総レコード数: {stats['total_records']:,}")
        
        self.analysis_results['data_density_analysis'] = analysis
        
        # 修正されたInsight：より意味のある比較
        highest_avg = max(data_availability.items(), key=lambda x: x[1]['avg_records_per_project'])
        lowest_avg = min(data_availability.items(), key=lambda x: x[1]['avg_records_per_project'])
        highest_total = max(data_availability.items(), key=lambda x: x[1]['total_records'])
        
        self.insights.append(f"{highest_avg[0]}が最高の平均レコード数/事業({highest_avg[1]['avg_records_per_project']:.1f})")
        self.insights.append(f"{lowest_avg[0]}が最低の平均レコード数/事業({lowest_avg[1]['avg_records_per_project']:.1f})")
        self.insights.append(f"{highest_total[0]}が最大の総レコード数({highest_total[1]['total_records']:,})")
        
        return analysis
    
    def budget_pattern_analysis(self):
        """予算規模・パターン分析"""
        print("\n" + "="*80)
        print("3. 予算規模・パターン分析（新規）")
        print("="*80)
        
        budget_data = []
        budget_projects = []
        
        for idx, json_str in enumerate(self.df['budget_summary_json'].dropna()):
            try:
                if json_str and json_str != 'null':
                    records = json.loads(json_str)
                    if isinstance(records, list):
                        project_budgets = []
                        for record in records:
                            if isinstance(record, dict):
                                # より包括的な予算額抽出
                                for key, value in record.items():
                                    if any(budget_key in key for budget_key in 
                                          ['予算', '金額', '額', '執行', '要求', '当初', '補正']):
                                        if isinstance(value, (int, float)) and value > 0:
                                            project_budgets.append(value)
                                        elif isinstance(value, str):
                                            # 数値抽出の改善
                                            numbers = re.findall(r'[\d,]+', str(value).replace(',', ''))
                                            for num_str in numbers:
                                                try:
                                                    num = float(num_str)
                                                    if 1000 <= num <= 1e12:  # 現実的な範囲
                                                        project_budgets.append(num)
                                                except:
                                                    pass
                        
                        if project_budgets:
                            project_total = sum(project_budgets)
                            budget_data.append(project_total)
                            budget_projects.append({
                                'project_id': self.df.iloc[idx]['予算事業ID'],
                                'project_name': self.df.iloc[idx]['事業名'],
                                'ministry': self.df.iloc[idx]['府省庁'],
                                'total_budget': project_total,
                                'budget_records': len(project_budgets)
                            })
            except:
                continue
        
        if budget_data:
            budget_array = np.array(budget_data)
            # 異常値除去（上位・下位1%）
            budget_clean = budget_array[
                (budget_array >= np.percentile(budget_array, 1)) &
                (budget_array <= np.percentile(budget_array, 99))
            ]
            
            analysis = {
                'total_projects_with_budget': len(budget_data),
                'budget_coverage': (len(budget_data) / len(self.df)) * 100,
                'budget_statistics': {
                    'mean': np.mean(budget_clean),
                    'median': np.median(budget_clean),
                    'std': np.std(budget_clean),
                    'min': np.min(budget_clean),
                    'max': np.max(budget_clean),
                    'q25': np.percentile(budget_clean, 25),
                    'q75': np.percentile(budget_clean, 75),
                    'q90': np.percentile(budget_clean, 90),
                    'q95': np.percentile(budget_clean, 95)
                },
                'budget_distribution': {
                    'small_projects': len([b for b in budget_clean if b < 1e8]),  # 1億未満
                    'medium_projects': len([b for b in budget_clean if 1e8 <= b < 1e9]),  # 1-10億
                    'large_projects': len([b for b in budget_clean if 1e9 <= b < 1e10]),  # 10-100億
                    'mega_projects': len([b for b in budget_clean if b >= 1e10])  # 100億以上
                },
                'top_budget_projects': sorted(budget_projects, key=lambda x: x['total_budget'], reverse=True)[:10]
            }
            
            # 府省庁別予算分析
            ministry_budgets = {}
            for project in budget_projects:
                ministry = project['ministry']
                if ministry not in ministry_budgets:
                    ministry_budgets[ministry] = []
                ministry_budgets[ministry].append(project['total_budget'])
            
            ministry_budget_stats = {}
            for ministry, budgets in ministry_budgets.items():
                if len(budgets) >= 3:  # 3事業以上
                    ministry_budget_stats[ministry] = {
                        'count': len(budgets),
                        'total': sum(budgets),
                        'mean': np.mean(budgets),
                        'median': np.median(budgets),
                        'max': max(budgets)
                    }
            
            analysis['ministry_budget_analysis'] = dict(sorted(
                ministry_budget_stats.items(), 
                key=lambda x: x[1]['total'], reverse=True
            )[:10])
            
            print(f"予算分析結果:")
            print(f"  - 予算データ有り: {analysis['total_projects_with_budget']:,}/{len(self.df):,}事業 ({analysis['budget_coverage']:.1f}%)")
            print(f"  - 平均予算額: {analysis['budget_statistics']['mean']:,.0f}円")
            print(f"  - 中央値: {analysis['budget_statistics']['median']:,.0f}円")
            print(f"  - 最大: {analysis['budget_statistics']['max']:,.0f}円")
            
            print(f"\n予算規模別分布:")
            print(f"  - 小規模(1億未満): {analysis['budget_distribution']['small_projects']}事業")
            print(f"  - 中規模(1-10億): {analysis['budget_distribution']['medium_projects']}事業")
            print(f"  - 大規模(10-100億): {analysis['budget_distribution']['large_projects']}事業")
            print(f"  - 超大規模(100億以上): {analysis['budget_distribution']['mega_projects']}事業")
            
            print(f"\n府省庁別予算規模（上位5）:")
            for ministry, stats in list(analysis['ministry_budget_analysis'].items())[:5]:
                print(f"  {ministry}: 総額{stats['total']:,.0f}円 (平均{stats['mean']:,.0f}円, {stats['count']}事業)")
            
            # Insight追加
            mega_ratio = (analysis['budget_distribution']['mega_projects'] / len(budget_clean)) * 100
            top_ministry_budget = list(analysis['ministry_budget_analysis'].items())[0]
            
            self.insights.append(f"予算データ取得率{analysis['budget_coverage']:.1f}%、平均{analysis['budget_statistics']['mean']/1e8:.1f}億円/事業")
            if mega_ratio > 1:
                self.insights.append(f"超大規模事業(100億円以上)が{mega_ratio:.1f}%存在")
            self.insights.append(f"予算総額最大は{top_ministry_budget[0]}({top_ministry_budget[1]['total']/1e12:.1f}兆円)")
        
        else:
            analysis = {'error': '予算データの抽出に失敗'}
        
        self.analysis_results['budget_pattern_analysis'] = analysis
        return analysis
    
    def expenditure_diversity_analysis(self):
        """支出多様性・複雑性分析"""
        print("\n" + "="*80)
        print("4. 支出多様性・複雑性分析（新規）")
        print("="*80)
        
        expenditure_diversity = []
        contract_complexity = []
        
        for idx, json_str in enumerate(self.df['expenditure_info_json'].dropna()):
            try:
                if json_str and json_str != 'null':
                    records = json.loads(json_str)
                    if isinstance(records, list):
                        entities = set()
                        amounts = []
                        contract_types = set()
                        
                        for record in records:
                            if isinstance(record, dict):
                                # 支出先の多様性
                                if '支出先名' in record and record['支出先名']:
                                    entities.add(str(record['支出先名']).strip())
                                
                                # 契約方式の多様性
                                if '契約方式等' in record and record['契約方式等']:
                                    contract_types.add(str(record['契約方式等']).strip())
                                
                                # 金額データ
                                for key, value in record.items():
                                    if '金額' in key:
                                        if isinstance(value, (int, float)) and value > 0:
                                            amounts.append(value)
                        
                        if entities:
                            # ハーフィンダール指数（集中度）の計算
                            total_amount = sum(amounts) if amounts else len(entities)
                            if total_amount > 0:
                                entity_weights = [1/len(entities)] * len(entities)  # 簡易版
                                hhi = sum(w**2 for w in entity_weights)
                            else:
                                hhi = 1.0
                            
                            expenditure_diversity.append({
                                'project_id': self.df.iloc[idx]['予算事業ID'],
                                'project_name': self.df.iloc[idx]['事業名'],
                                'ministry': self.df.iloc[idx]['府省庁'],
                                'entity_count': len(entities),
                                'contract_type_count': len(contract_types),
                                'hhi': hhi,
                                'total_amount': sum(amounts) if amounts else 0
                            })
            except:
                continue
        
        if expenditure_diversity:
            entity_counts = [item['entity_count'] for item in expenditure_diversity]
            contract_counts = [item['contract_type_count'] for item in expenditure_diversity]
            hhi_values = [item['hhi'] for item in expenditure_diversity]
            
            analysis = {
                'total_analyzed_projects': len(expenditure_diversity),
                'entity_diversity_stats': {
                    'mean': np.mean(entity_counts),
                    'median': np.median(entity_counts),
                    'max': max(entity_counts),
                    'min': min(entity_counts),
                    'std': np.std(entity_counts)
                },
                'contract_diversity_stats': {
                    'mean': np.mean(contract_counts),
                    'median': np.median(contract_counts),
                    'max': max(contract_counts),
                    'std': np.std(contract_counts)
                },
                'concentration_stats': {
                    'mean_hhi': np.mean(hhi_values),
                    'median_hhi': np.median(hhi_values)
                },
                'complexity_categories': {
                    'simple': len([item for item in expenditure_diversity if item['entity_count'] <= 5]),
                    'moderate': len([item for item in expenditure_diversity if 5 < item['entity_count'] <= 20]),
                    'complex': len([item for item in expenditure_diversity if 20 < item['entity_count'] <= 50]),
                    'very_complex': len([item for item in expenditure_diversity if item['entity_count'] > 50])
                },
                'most_diverse_projects': sorted(expenditure_diversity, key=lambda x: x['entity_count'], reverse=True)[:5],
                'most_complex_contracts': sorted(expenditure_diversity, key=lambda x: x['contract_type_count'], reverse=True)[:5]
            }
            
            print(f"支出多様性分析結果:")
            print(f"  - 分析対象事業: {analysis['total_analyzed_projects']:,}")
            print(f"  - 平均支出先数: {analysis['entity_diversity_stats']['mean']:.1f}")
            print(f"  - 最大支出先数: {analysis['entity_diversity_stats']['max']}")
            print(f"  - 平均契約方式数: {analysis['contract_diversity_stats']['mean']:.1f}")
            
            print(f"\n事業複雑性分類:")
            total = len(expenditure_diversity)
            for category, count in analysis['complexity_categories'].items():
                percentage = (count / total) * 100
                print(f"  - {category}: {count}事業 ({percentage:.1f}%)")
            
            print(f"\n最多支出先事業トップ3:")
            for i, project in enumerate(analysis['most_diverse_projects'][:3], 1):
                print(f"  {i}. {project['project_name'][:50]}... ({project['ministry']}) - {project['entity_count']}先")
            
            # Insight追加
            complex_ratio = (analysis['complexity_categories']['complex'] + analysis['complexity_categories']['very_complex']) / total * 100
            most_diverse = analysis['most_diverse_projects'][0]
            
            self.insights.append(f"支出先数の平均{analysis['entity_diversity_stats']['mean']:.1f}、最大{analysis['entity_diversity_stats']['max']}先")
            if complex_ratio > 10:
                self.insights.append(f"複雑な支出構造(20先以上)の事業が{complex_ratio:.1f}%存在")
            self.insights.append(f"最多支出先事業「{most_diverse['project_name'][:30]}...」({most_diverse['entity_count']}先)")
        
        else:
            analysis = {'error': '支出データの抽出に失敗'}
        
        self.analysis_results['expenditure_diversity_analysis'] = analysis
        return analysis
    
    def temporal_trend_analysis(self):
        """時系列トレンド分析の強化"""
        print("\n" + "="*80)
        print("5. 時系列トレンド分析（強化版）")
        print("="*80)
        
        if '事業開始年度' not in self.df.columns:
            print("事業開始年度データが見つかりません")
            return {}
        
        start_years = pd.to_numeric(self.df['事業開始年度'], errors='coerce').dropna()
        
        # 府省庁別の時系列トレンド
        ministry_temporal = {}
        for ministry in self.df['府省庁'].value_counts().head(10).index:
            ministry_data = self.df[self.df['府省庁'] == ministry]
            ministry_years = pd.to_numeric(ministry_data['事業開始年度'], errors='coerce').dropna()
            
            if len(ministry_years) > 0:
                recent_ratio = len(ministry_years[ministry_years >= 2020]) / len(ministry_years) * 100
                ministry_temporal[ministry] = {
                    'total_projects': len(ministry_years),
                    'recent_projects_ratio': recent_ratio,
                    'mean_start_year': ministry_years.mean(),
                    'oldest_project': ministry_years.min(),
                    'newest_project': ministry_years.max()
                }
        
        # データ密度の年代変化
        density_by_decade = {}
        for decade in range(1990, 2030, 10):
            decade_projects = self.df[
                (pd.to_numeric(self.df['事業開始年度'], errors='coerce') >= decade) &
                (pd.to_numeric(self.df['事業開始年度'], errors='coerce') < decade + 10)
            ]
            
            if len(decade_projects) > 0:
                avg_density = decade_projects['total_related_records'].mean()
                density_by_decade[f"{decade}年代"] = {
                    'project_count': len(decade_projects),
                    'avg_data_density': avg_density,
                    'max_data_density': decade_projects['total_related_records'].max()
                }
        
        # 事業区分の時系列変化
        category_temporal = {}
        if '事業区分' in self.df.columns:
            for category in self.df['事業区分'].unique():
                if pd.notna(category):
                    category_data = self.df[self.df['事業区分'] == category]
                    category_years = pd.to_numeric(category_data['事業開始年度'], errors='coerce').dropna()
                    
                    if len(category_years) > 0:
                        category_temporal[category] = {
                            'count': len(category_years),
                            'mean_start_year': category_years.mean(),
                            'recent_ratio': len(category_years[category_years >= 2020]) / len(category_years) * 100
                        }
        
        analysis = {
            'overall_temporal_stats': {
                'min_year': int(start_years.min()),
                'max_year': int(start_years.max()),
                'mean_year': start_years.mean(),
                'recent_projects_ratio': len(start_years[start_years >= 2020]) / len(start_years) * 100,
                'decade_2020s_ratio': len(start_years[start_years >= 2020]) / len(start_years) * 100,
                'decade_2010s_ratio': len(start_years[(start_years >= 2010) & (start_years < 2020)]) / len(start_years) * 100,
                'decade_2000s_ratio': len(start_years[(start_years >= 2000) & (start_years < 2010)]) / len(start_years) * 100
            },
            'ministry_temporal_analysis': ministry_temporal,
            'density_by_decade': density_by_decade,
            'category_temporal_analysis': category_temporal
        }
        
        print(f"時系列トレンド分析:")
        print(f"  - 事業開始年度範囲: {analysis['overall_temporal_stats']['min_year']}-{analysis['overall_temporal_stats']['max_year']}")
        print(f"  - 2020年代開始事業: {analysis['overall_temporal_stats']['decade_2020s_ratio']:.1f}%")
        print(f"  - 2010年代開始事業: {analysis['overall_temporal_stats']['decade_2010s_ratio']:.1f}%")
        
        print(f"\n府省庁別最新事業比率（2020年以降）:")
        sorted_ministries = sorted(ministry_temporal.items(), key=lambda x: x[1]['recent_projects_ratio'], reverse=True)
        for ministry, stats in sorted_ministries[:5]:
            print(f"  {ministry}: {stats['recent_projects_ratio']:.1f}% ({stats['total_projects']}事業中)")
        
        print(f"\n年代別データ密度変化:")
        for decade, stats in density_by_decade.items():
            print(f"  {decade}: 平均{stats['avg_data_density']:.1f}レコード/事業 ({stats['project_count']}事業)")
        
        # Insight追加
        most_recent_ministry = max(ministry_temporal.items(), key=lambda x: x[1]['recent_projects_ratio'])
        most_historic_ministry = min(ministry_temporal.items(), key=lambda x: x[1]['mean_start_year'])
        
        self.insights.append(f"2020年代事業が{analysis['overall_temporal_stats']['decade_2020s_ratio']:.1f}%（新規事業中心の傾向）")
        self.insights.append(f"{most_recent_ministry[0]}が最も新しい事業構成({most_recent_ministry[1]['recent_projects_ratio']:.1f}%が2020年以降)")
        self.insights.append(f"{most_historic_ministry[0]}が最も歴史の長い事業構成(平均{most_historic_ministry[1]['mean_start_year']:.0f}年開始)")
        
        self.analysis_results['temporal_trend_analysis'] = analysis
        return analysis
    
    def anomaly_deep_dive(self):
        """特異事業の深掘り分析"""
        print("\n" + "="*80)
        print("6. 特異事業深掘り分析（新規）")
        print("="*80)
        
        anomalies = {}
        
        # 高データ密度事業の特徴分析
        if 'total_related_records' in self.df.columns:
            high_density_threshold = self.df['total_related_records'].quantile(0.95)
            high_density_projects = self.df[self.df['total_related_records'] >= high_density_threshold]
            
            # 高密度事業の共通特徴
            ministry_distribution = high_density_projects['府省庁'].value_counts()
            category_distribution = high_density_projects.get('事業区分', pd.Series()).value_counts()
            
            # 実施方法の特徴
            implementation_cols = [col for col in self.df.columns if '実施方法' in col]
            implementation_features = {}
            for col in implementation_cols:
                if col in high_density_projects.columns:
                    ratio = (pd.to_numeric(high_density_projects[col], errors='coerce').fillna(0) > 0).mean()
                    implementation_features[col.replace('実施方法ー', '')] = ratio * 100
            
            anomalies['high_density_projects'] = {
                'threshold': high_density_threshold,
                'count': len(high_density_projects),
                'percentage': len(high_density_projects) / len(self.df) * 100,
                'ministry_concentration': ministry_distribution.to_dict(),
                'category_distribution': category_distribution.to_dict(),
                'implementation_methods': implementation_features,
                'top_projects': high_density_projects.nlargest(5, 'total_related_records')[
                    ['事業名', '府省庁', 'total_related_records', '事業区分']].to_dict('records')
            }
        
        # 長期事業の分析
        if '事業開始年度' in self.df.columns and '事業終了（予定）年度' in self.df.columns:
            start_years = pd.to_numeric(self.df['事業開始年度'], errors='coerce')
            end_years = pd.to_numeric(self.df['事業終了（予定）年度'], errors='coerce')
            
            project_duration = end_years - start_years
            long_projects = self.df[project_duration >= 20]  # 20年以上
            
            if len(long_projects) > 0:
                anomalies['long_duration_projects'] = {
                    'count': len(long_projects),
                    'percentage': len(long_projects) / len(self.df) * 100,
                    'avg_duration': project_duration[project_duration >= 20].mean(),
                    'max_duration': project_duration.max(),
                    'ministry_distribution': long_projects['府省庁'].value_counts().to_dict(),
                    'examples': long_projects.nlargest(3, project_duration.reindex(long_projects.index))[
                        ['事業名', '府省庁', '事業開始年度', '事業終了（予定）年度']].to_dict('records')
                }
        
        # 多目標設定事業
        if 'goals_performance_count' in self.df.columns:
            high_goals_threshold = self.df['goals_performance_count'].quantile(0.9)
            high_goals_projects = self.df[self.df['goals_performance_count'] >= high_goals_threshold]
            
            anomalies['high_goals_projects'] = {
                'threshold': high_goals_threshold,
                'count': len(high_goals_projects),
                'avg_goals': high_goals_projects['goals_performance_count'].mean(),
                'max_goals': high_goals_projects['goals_performance_count'].max(),
                'ministry_distribution': high_goals_projects['府省庁'].value_counts().to_dict(),
                'examples': high_goals_projects.nlargest(3, 'goals_performance_count')[
                    ['事業名', '府省庁', 'goals_performance_count']].to_dict('records')
            }
        
        print(f"特異事業分析:")
        
        if 'high_density_projects' in anomalies:
            hdp = anomalies['high_density_projects']
            print(f"  高データ密度事業（95%ile以上）:")
            print(f"    - 該当事業: {hdp['count']}事業 ({hdp['percentage']:.1f}%)")
            print(f"    - 閾値: {hdp['threshold']:.0f}レコード")
            print(f"    - 主要府省庁: {list(hdp['ministry_concentration'].keys())[:3]}")
        
        if 'long_duration_projects' in anomalies:
            ldp = anomalies['long_duration_projects']
            print(f"  長期事業（20年以上）:")
            print(f"    - 該当事業: {ldp['count']}事業 ({ldp['percentage']:.1f}%)")
            print(f"    - 平均期間: {ldp['avg_duration']:.1f}年")
            print(f"    - 最長: {ldp['max_duration']:.0f}年")
        
        if 'high_goals_projects' in anomalies:
            hgp = anomalies['high_goals_projects']
            print(f"  多目標設定事業（90%ile以上）:")
            print(f"    - 該当事業: {hgp['count']}事業")
            print(f"    - 平均目標数: {hgp['avg_goals']:.1f}")
            print(f"    - 最大目標数: {hgp['max_goals']:.0f}")
        
        # Insight追加
        if 'high_density_projects' in anomalies:
            top_ministry = list(anomalies['high_density_projects']['ministry_concentration'].keys())[0]
            self.insights.append(f"高データ密度事業の{anomalies['high_density_projects']['ministry_concentration'][top_ministry]}事業は{top_ministry}が占有")
        
        if 'long_duration_projects' in anomalies:
            self.insights.append(f"20年以上の長期事業が{anomalies['long_duration_projects']['percentage']:.1f}%存在")
        
        self.analysis_results['anomaly_deep_dive'] = anomalies
        return anomalies
    
    def project_clustering_analysis(self):
        """事業の類型化・クラスタリング分析"""
        print("\n" + "="*80)
        print("7. 事業類型化・複雑性指標分析（新規）")
        print("="*80)
        
        # 複雑性指標の計算
        complexity_features = []
        
        # 数値特徴量の抽出
        numeric_features = ['total_related_records'] + [col for col in self.count_cols if col in self.df.columns]
        
        # 各事業の複雑性スコア計算
        for idx, row in self.df.iterrows():
            features = {}
            
            # データ密度
            features['data_density'] = row.get('total_related_records', 0)
            
            # 支出の複雑性（支出先数）
            features['expenditure_complexity'] = row.get('expenditure_info_count', 0)
            
            # 目標の複雑性
            features['goals_complexity'] = row.get('goals_performance_count', 0)
            
            # 予算の複雑性
            features['budget_complexity'] = row.get('budget_items_count', 0)
            
            # 総合複雑性スコア
            features['total_complexity'] = sum(features.values())
            
            # その他の特徴
            features['ministry'] = row.get('府省庁', '')
            features['category'] = row.get('事業区分', '')
            features['project_id'] = row.get('予算事業ID', '')
            features['project_name'] = row.get('事業名', '')
            
            complexity_features.append(features)
        
        # 複雑性による分類
        complexity_scores = [item['total_complexity'] for item in complexity_features]
        
        if complexity_scores:
            q25 = np.percentile(complexity_scores, 25)
            q75 = np.percentile(complexity_scores, 75)
            q90 = np.percentile(complexity_scores, 90)
            
            # 事業タイプ分類
            simple_projects = [item for item in complexity_features if item['total_complexity'] <= q25]
            moderate_projects = [item for item in complexity_features if q25 < item['total_complexity'] <= q75]
            complex_projects = [item for item in complexity_features if q75 < item['total_complexity'] <= q90]
            very_complex_projects = [item for item in complexity_features if item['total_complexity'] > q90]
            
            # 各タイプの特徴分析
            def analyze_group(group, name):
                if not group:
                    return {}
                
                ministry_dist = Counter([item['ministry'] for item in group])
                category_dist = Counter([item['category'] for item in group if item['category']])
                
                return {
                    'count': len(group),
                    'percentage': len(group) / len(complexity_features) * 100,
                    'avg_complexity': np.mean([item['total_complexity'] for item in group]),
                    'avg_data_density': np.mean([item['data_density'] for item in group]),
                    'top_ministries': dict(ministry_dist.most_common(3)),
                    'top_categories': dict(category_dist.most_common(3)),
                    'examples': group[:3]
                }
            
            analysis = {
                'complexity_distribution': {
                    'simple': analyze_group(simple_projects, '単純'),
                    'moderate': analyze_group(moderate_projects, '中程度'),
                    'complex': analyze_group(complex_projects, '複雑'),
                    'very_complex': analyze_group(very_complex_projects, '極複雑')
                },
                'complexity_thresholds': {
                    'q25': q25,
                    'q75': q75,
                    'q90': q90
                },
                'top_complex_projects': sorted(complexity_features, key=lambda x: x['total_complexity'], reverse=True)[:10]
            }
            
            # 府省庁別の複雑性傾向
            ministry_complexity = {}
            for ministry in self.df['府省庁'].value_counts().head(10).index:
                ministry_items = [item for item in complexity_features if item['ministry'] == ministry]
                if ministry_items:
                    avg_complexity = np.mean([item['total_complexity'] for item in ministry_items])
                    ministry_complexity[ministry] = {
                        'avg_complexity': avg_complexity,
                        'project_count': len(ministry_items),
                        'complexity_rank': 0  # 後で設定
                    }
            
            # 複雑性ランキング
            sorted_ministries = sorted(ministry_complexity.items(), key=lambda x: x[1]['avg_complexity'], reverse=True)
            for i, (ministry, stats) in enumerate(sorted_ministries):
                ministry_complexity[ministry]['complexity_rank'] = i + 1
            
            analysis['ministry_complexity_ranking'] = dict(sorted_ministries[:10])
            
            print(f"事業複雑性分析:")
            print(f"  複雑性分類:")
            for type_name, stats in analysis['complexity_distribution'].items():
                print(f"    - {type_name}: {stats['count']}事業 ({stats['percentage']:.1f}%), 平均複雑性{stats['avg_complexity']:.1f}")
            
            print(f"\n府省庁別複雑性ランキング（上位5）:")
            for i, (ministry, stats) in enumerate(sorted_ministries[:5], 1):
                print(f"    {i}. {ministry}: 平均複雑性{stats['avg_complexity']:.1f} ({stats['project_count']}事業)")
            
            print(f"\n最複雑事業トップ3:")
            for i, project in enumerate(analysis['top_complex_projects'][:3], 1):
                print(f"    {i}. {project['project_name'][:50]}... ({project['ministry']}) - 複雑性{project['total_complexity']:.0f}")
            
            # Insight追加
            very_complex_ratio = analysis['complexity_distribution']['very_complex']['percentage']
            most_complex_ministry = sorted_ministries[0]
            simplest_ministry = sorted_ministries[-1]
            
            self.insights.append(f"極複雑事業(上位10%)が{very_complex_ratio:.1f}%存在")
            self.insights.append(f"{most_complex_ministry[0]}が最高複雑性(平均{most_complex_ministry[1]['avg_complexity']:.1f})")
            self.insights.append(f"{simplest_ministry[0]}が最低複雑性(平均{simplest_ministry[1]['avg_complexity']:.1f})")
        
        else:
            analysis = {'error': '複雑性分析に失敗'}
        
        self.analysis_results['project_clustering_analysis'] = analysis
        return analysis
    
    def correlation_analysis_enhanced(self):
        """強化された相関・パターン分析"""
        print("\n" + "="*80)
        print("8. 相関・パターン分析（強化版）")
        print("="*80)
        
        # 数値列の抽出
        numeric_cols = []
        for col in self.count_cols + ['total_related_records']:
            if col in self.df.columns:
                numeric_cols.append(col)
        
        correlations = {}
        
        if len(numeric_cols) >= 2:
            # 相関行列の計算
            corr_matrix = self.df[numeric_cols].corr()
            
            # 強相関ペアの抽出（閾値を0.3に下げてより多くの関係を発見）
            strong_correlations = []
            moderate_correlations = []
            
            for i in range(len(corr_matrix.columns)):
                for j in range(i+1, len(corr_matrix.columns)):
                    corr_value = corr_matrix.iloc[i, j]
                    pair = {
                        'var1': corr_matrix.columns[i],
                        'var2': corr_matrix.columns[j],
                        'correlation': corr_value
                    }
                    
                    if abs(corr_value) > 0.7:
                        strong_correlations.append(pair)
                    elif abs(corr_value) > 0.3:
                        moderate_correlations.append(pair)
            
            correlations['correlation_matrix'] = corr_matrix.to_dict()
            correlations['strong_correlations'] = strong_correlations
            correlations['moderate_correlations'] = moderate_correlations
            
            print(f"強相関ペア (|r| > 0.7):")
            for corr in sorted(strong_correlations, key=lambda x: abs(x['correlation']), reverse=True):
                var1_name = corr['var1'].replace('_count', '').replace('_', ' ')
                var2_name = corr['var2'].replace('_count', '').replace('_', ' ')
                print(f"    {var1_name} ⟷ {var2_name}: r = {corr['correlation']:.3f}")
            
            print(f"\n中相関ペア (0.3 < |r| ≤ 0.7):")
            for corr in sorted(moderate_correlations, key=lambda x: abs(x['correlation']), reverse=True)[:5]:
                var1_name = corr['var1'].replace('_count', '').replace('_', ' ')
                var2_name = corr['var2'].replace('_count', '').replace('_', ' ')
                print(f"    {var1_name} ⟷ {var2_name}: r = {corr['correlation']:.3f}")
        
        # 府省庁とデータ特性の詳細分析
        if '府省庁' in self.df.columns:
            ministry_profiles = {}
            
            for ministry in self.df['府省庁'].value_counts().head(15).index:
                ministry_data = self.df[self.df['府省庁'] == ministry]
                
                if len(ministry_data) >= 5:  # 最小5事業
                    profile = {
                        'project_count': len(ministry_data),
                        'avg_data_density': ministry_data['total_related_records'].mean(),
                        'data_density_std': ministry_data['total_related_records'].std(),
                        'specialization_scores': {}
                    }
                    
                    # 各データタイプの特化度計算
                    for col in self.count_cols:
                        if col in ministry_data.columns:
                            ministry_avg = ministry_data[col].mean()
                            overall_avg = self.df[col].mean()
                            specialization = ministry_avg / overall_avg if overall_avg > 0 else 0
                            profile['specialization_scores'][col.replace('_count', '')] = specialization
                    
                    ministry_profiles[ministry] = profile
            
            correlations['ministry_profiles'] = ministry_profiles
            
            # 府省庁の特化分析
            print(f"\n府省庁別特化分析（特化度1.5以上）:")
            for ministry, profile in ministry_profiles.items():
                specializations = [(k, v) for k, v in profile['specialization_scores'].items() if v >= 1.5]
                if specializations:
                    top_spec = sorted(specializations, key=lambda x: x[1], reverse=True)[:2]
                    spec_text = ", ".join([f"{spec[0]}({spec[1]:.1f}倍)" for spec in top_spec])
                    print(f"    {ministry}: {spec_text}")
        
        self.analysis_results['correlation_analysis_enhanced'] = correlations
        
        # 新しいInsight
        if strong_correlations:
            strongest = max(strong_correlations, key=lambda x: abs(x['correlation']))
            self.insights.append(f"最強相関: {strongest['var1']}と{strongest['var2']} (r={strongest['correlation']:.3f})")
        
        return correlations
    
    def generate_comprehensive_insights(self):
        """包括的Insight生成"""
        print("\n" + "="*80)
        print("🔍 包括的Insight総括")
        print("="*80)
        
        # カテゴリ別にInsightを整理
        categorized_insights = {
            '組織・規模': [],
            'データ特性': [],
            '予算・支出': [],
            '時系列・トレンド': [],
            '複雑性・特異性': [],
            '相関・パターン': []
        }
        
        # 既存のInsightを分類
        for insight in self.insights:
            if any(word in insight for word in ['府省庁', '組織', '集中', '事業数']):
                categorized_insights['組織・規模'].append(insight)
            elif any(word in insight for word in ['データ', 'レコード', '密度', '保有']):
                categorized_insights['データ特性'].append(insight)
            elif any(word in insight for word in ['予算', '支出', '億円', '兆円']):
                categorized_insights['予算・支出'].append(insight)
            elif any(word in insight for word in ['年', '年代', 'トレンド', '新規']):
                categorized_insights['時系列・トレンド'].append(insight)
            elif any(word in insight for word in ['複雑', '特異', '異常', '最大', '最小']):
                categorized_insights['複雑性・特異性'].append(insight)
            elif any(word in insight for word in ['相関', 'パターン', '関係']):
                categorized_insights['相関・パターン'].append(insight)
            else:
                categorized_insights['データ特性'].append(insight)
        
        # 追加の深いInsight生成
        additional_insights = self.generate_deep_insights()
        
        # 全Insightを表示
        total_insights = 0
        for category, insights in categorized_insights.items():
            if insights:
                print(f"\n【{category}】")
                for i, insight in enumerate(insights, 1):
                    print(f"  {total_insights + i}. {insight}")
                total_insights += len(insights)
        
        if additional_insights:
            print(f"\n【追加発見】")
            for i, insight in enumerate(additional_insights, total_insights + 1):
                print(f"  {i}. {insight}")
        
        self.insights.extend(additional_insights)
        return categorized_insights
    
    def generate_deep_insights(self):
        """深いInsightの生成"""
        deep_insights = []
        
        # 分析結果から深いInsightを抽出
        if 'budget_pattern_analysis' in self.analysis_results:
            budget_analysis = self.analysis_results['budget_pattern_analysis']
            if 'budget_statistics' in budget_analysis:
                median = budget_analysis['budget_statistics']['median']
                mean = budget_analysis['budget_statistics']['mean']
                if mean > median * 2:
                    deep_insights.append("予算分布が右に歪んでおり少数の大規模事業が平均を押し上げている")
        
        if 'expenditure_diversity_analysis' in self.analysis_results:
            exp_analysis = self.analysis_results['expenditure_diversity_analysis']
            if 'complexity_categories' in exp_analysis:
                total = sum(exp_analysis['complexity_categories'].values())
                simple_ratio = exp_analysis['complexity_categories']['simple'] / total * 100
                if simple_ratio > 60:
                    deep_insights.append(f"支出構造が単純な事業が{simple_ratio:.1f}%を占め効率的な執行体制")
        
        if 'temporal_trend_analysis' in self.analysis_results:
            temporal_analysis = self.analysis_results['temporal_trend_analysis']
            if 'density_by_decade' in temporal_analysis:
                decades = list(temporal_analysis['density_by_decade'].keys())
                if len(decades) >= 2:
                    latest = temporal_analysis['density_by_decade'][decades[-1]]['avg_data_density']
                    previous = temporal_analysis['density_by_decade'][decades[-2]]['avg_data_density']
                    if latest > previous * 1.2:
                        deep_insights.append("最新年代の事業でデータ管理の高度化が進んでいる")
        
        if 'project_clustering_analysis' in self.analysis_results:
            cluster_analysis = self.analysis_results['project_clustering_analysis']
            if 'ministry_complexity_ranking' in cluster_analysis:
                ranking = list(cluster_analysis['ministry_complexity_ranking'].items())
                if len(ranking) >= 3:
                    top3_avg = np.mean([stats['avg_complexity'] for _, stats in ranking[:3]])
                    bottom3_avg = np.mean([stats['avg_complexity'] for _, stats in ranking[-3:]])
                    if top3_avg > bottom3_avg * 2:
                        deep_insights.append("府省庁間で事業複雑性に大きな格差があり専門性の違いを示唆")
        
        if 'correlation_analysis_enhanced' in self.analysis_results:
            corr_analysis = self.analysis_results['correlation_analysis_enhanced']
            if 'ministry_profiles' in corr_analysis:
                profiles = corr_analysis['ministry_profiles']
                high_specialization = []
                for ministry, profile in profiles.items():
                    max_spec = max(profile['specialization_scores'].values()) if profile['specialization_scores'] else 0
                    if max_spec > 2.0:
                        high_specialization.append(ministry)
                
                if len(high_specialization) >= 3:
                    deep_insights.append(f"{len(high_specialization)}府省庁が特定分野で高い特化を示し役割分担が明確")
        
        return deep_insights
    
    def save_enhanced_results(self):
        """強化された結果保存"""
        print("\n" + "="*80)
        print("結果保存（強化版）")
        print("="*80)
        
        # JSON結果保存
        results_path = self.output_dir / "enhanced_analysis_results.json"
        with open(results_path, 'w', encoding='utf-8') as f:
            json.dump(self.analysis_results, f, ensure_ascii=False, indent=2, default=str)
        print(f"✓ 強化分析結果保存: {results_path}")
        
        # Insight保存
        insights_path = self.output_dir / "comprehensive_insights.txt"
        with open(insights_path, 'w', encoding='utf-8') as f:
            f.write("事業マスターリスト強化分析 - 包括的Insight\n")
            f.write("="*60 + "\n\n")
            for i, insight in enumerate(self.insights, 1):
                f.write(f"{i:2d}. {insight}\n")
        print(f"✓ 包括的Insight保存: {insights_path}")
        
        # 強化HTMLレポート生成
        self.generate_enhanced_html_report()
        
        return True
    
    def generate_enhanced_html_report(self):
        """強化HTMLレポート生成"""
        html_content = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <title>事業マスターリスト強化分析レポート</title>
    <style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 20px; background: #f8fafc; color: #1a202c; }}
        .container {{ max-width: 1400px; margin: 0 auto; background: #ffffff; padding: 40px; border-radius: 12px; box-shadow: 0 4px 20px rgba(0,0,0,0.1); }}
        h1 {{ color: #2d3748; text-align: center; border-bottom: 4px solid #4299e1; padding-bottom: 20px; margin-bottom: 30px; }}
        h2 {{ color: #2d3748; margin-top: 40px; border-left: 6px solid #4299e1; padding-left: 15px; }}
        h3 {{ color: #4a5568; margin-top: 30px; }}
        .summary {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 10px; margin: 25px 0; }}
        .metric {{ display: inline-block; margin: 15px 25px; text-align: center; }}
        .metric-value {{ font-size: 2.5em; font-weight: bold; display: block; }}
        .metric-label {{ font-size: 1em; opacity: 0.9; }}
        table {{ width: 100%; border-collapse: collapse; margin: 25px 0; }}
        th {{ background: #edf2f7; color: #2d3748; padding: 15px; text-align: left; border-bottom: 2px solid #cbd5e0; }}
        td {{ padding: 12px 15px; border-bottom: 1px solid #e2e8f0; color: #4a5568; }}
        tr:hover {{ background: #f7fafc; }}
        .insight {{ background: #e6fffa; border-left: 5px solid #38b2ac; padding: 20px; margin: 15px 0; color: #234e52; border-radius: 5px; }}
        .section {{ margin: 40px 0; padding: 30px; background: #f7fafc; border-radius: 8px; }}
        .highlight {{ background: #fed7d7; color: #742a2a; padding: 3px 6px; border-radius: 3px; }}
        .success {{ background: #c6f6d5; color: #22543d; padding: 3px 6px; border-radius: 3px; }}
        .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin: 20px 0; }}
        .card {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.05); }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 事業マスターリスト強化分析レポート</h1>
        
        <div class="summary">
            <h2 style="color: white; margin-top: 0;">分析概要</h2>
"""
        
        # 基本統計の表示
        if 'basic_statistics' in self.analysis_results:
            stats = self.analysis_results['basic_statistics']
            html_content += f"""
            <div class="metric">
                <span class="metric-value">{stats['total_projects']:,}</span>
                <span class="metric-label">総事業数</span>
            </div>
            <div class="metric">
                <span class="metric-value">{stats['total_columns']}</span>
                <span class="metric-label">総列数</span>
            </div>
            <div class="metric">
                <span class="metric-value">{stats['data_completeness']:.1f}%</span>
                <span class="metric-label">データ完全性</span>
            </div>
            <div class="metric">
                <span class="metric-value">{len(self.insights)}</span>
                <span class="metric-label">発見Insight数</span>
            </div>
"""
        
        html_content += """
        </div>
        
        <h2>🎯 主要Insight</h2>
        <div class="grid">
"""
        
        # Insightをカード形式で表示
        for i, insight in enumerate(self.insights[:12], 1):  # 最初の12個を表示
            html_content += f'            <div class="card"><div class="insight">{i}. {insight}</div></div>\n'
        
        html_content += """
        </div>
"""
        
        # 府省庁分析（修正版）
        if 'ministry_analysis' in self.analysis_results:
            ministry_data = self.analysis_results['ministry_analysis']
            html_content += """
        <div class="section">
            <h2>🏛️ 府省庁分析（修正版）</h2>
            <table>
                <tr>
                    <th>順位</th>
                    <th>府省庁</th>
                    <th>事業数</th>
                    <th>割合</th>
                    <th>集中度</th>
                </tr>
"""
            total_projects = ministry_data['total_projects']
            for i, (ministry, count) in enumerate(ministry_data['top_10_ministries'].items(), 1):
                percentage = (count / total_projects) * 100  # 修正された計算
                concentration = "高" if percentage > 10 else "中" if percentage > 5 else "低"
                html_content += f"""
                <tr>
                    <td>{i}</td>
                    <td><strong>{ministry}</strong></td>
                    <td>{count:,}</td>
                    <td><span class="{'highlight' if percentage > 15 else 'success' if percentage > 10 else ''}">{percentage:.1f}%</span></td>
                    <td>{concentration}</td>
                </tr>
"""
            html_content += """
            </table>
            <p><strong>集中度分析:</strong> 
            上位3府省庁で{:.1f}%、上位5府省庁で{:.1f}%を占有</p>
        </div>
""".format(
            ministry_data['ministry_stats']['concentration_ratio_top3'],
            ministry_data['ministry_stats']['concentration_ratio_top5']
        )
        
        # 予算分析
        if 'budget_pattern_analysis' in self.analysis_results:
            budget_data = self.analysis_results['budget_pattern_analysis']
            if 'budget_statistics' in budget_data:
                html_content += f"""
        <div class="section">
            <h2>💰 予算規模分析</h2>
            <div class="grid">
                <div class="card">
                    <h3>基本統計</h3>
                    <p>平均予算: <strong>{budget_data['budget_statistics']['mean']/1e8:.1f}億円</strong></p>
                    <p>中央値: <strong>{budget_data['budget_statistics']['median']/1e8:.1f}億円</strong></p>
                    <p>最大予算: <strong>{budget_data['budget_statistics']['max']/1e8:.1f}億円</strong></p>
                </div>
                <div class="card">
                    <h3>規模別分布</h3>
                    <p>小規模(1億未満): {budget_data['budget_distribution']['small_projects']}事業</p>
                    <p>中規模(1-10億): {budget_data['budget_distribution']['medium_projects']}事業</p>
                    <p>大規模(10-100億): {budget_data['budget_distribution']['large_projects']}事業</p>
                    <p>超大規模(100億以上): <span class="highlight">{budget_data['budget_distribution']['mega_projects']}事業</span></p>
                </div>
            </div>
        </div>
"""
        
        # 複雑性分析
        if 'project_clustering_analysis' in self.analysis_results:
            cluster_data = self.analysis_results['project_clustering_analysis']
            if 'complexity_distribution' in cluster_data:
                html_content += """
        <div class="section">
            <h2>🔬 事業複雑性分析</h2>
            <table>
                <tr>
                    <th>複雑性レベル</th>
                    <th>事業数</th>
                    <th>割合</th>
                    <th>平均複雑性スコア</th>
                </tr>
"""
                for level, stats in cluster_data['complexity_distribution'].items():
                    level_name = {'simple': '単純', 'moderate': '中程度', 'complex': '複雑', 'very_complex': '極複雑'}[level]
                    html_content += f"""
                <tr>
                    <td>{level_name}</td>
                    <td>{stats['count']:,}</td>
                    <td>{stats['percentage']:.1f}%</td>
                    <td>{stats['avg_complexity']:.1f}</td>
                </tr>
"""
                html_content += """
            </table>
        </div>
"""
        
        # 時系列トレンド
        if 'temporal_trend_analysis' in self.analysis_results:
            temporal_data = self.analysis_results['temporal_trend_analysis']
            html_content += f"""
        <div class="section">
            <h2>📈 時系列トレンド分析</h2>
            <div class="grid">
                <div class="card">
                    <h3>年代別分布</h3>
                    <p>2020年代開始: <span class="highlight">{temporal_data['overall_temporal_stats']['decade_2020s_ratio']:.1f}%</span></p>
                    <p>2010年代開始: {temporal_data['overall_temporal_stats']['decade_2010s_ratio']:.1f}%</p>
                    <p>2000年代開始: {temporal_data['overall_temporal_stats']['decade_2000s_ratio']:.1f}%</p>
                </div>
                <div class="card">
                    <h3>最新事業比率（2020年以降開始）</h3>
"""
            
            if 'ministry_temporal_analysis' in temporal_data:
                sorted_recent = sorted(temporal_data['ministry_temporal_analysis'].items(), 
                                     key=lambda x: x[1]['recent_projects_ratio'], reverse=True)
                for ministry, stats in sorted_recent[:5]:
                    html_content += f"                    <p>{ministry}: {stats['recent_projects_ratio']:.1f}%</p>\n"
            
            html_content += """
                </div>
            </div>
        </div>
"""
        
        html_content += """
        <div style="text-align: center; margin-top: 50px; color: #718096; font-size: 0.9em;">
            事業マスターリスト強化分析レポート - RS Visualization System<br>
            深い洞察による政府事業の包括的理解
        </div>
    </div>
</body>
</html>
"""
        
        report_path = self.output_dir / "enhanced_analysis_report.html"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"✓ 強化HTMLレポート保存: {report_path}")
    
    def run_enhanced_analysis(self):
        """強化分析実行メイン"""
        print("\n" + "="*80)
        print("🚀 事業マスターリスト強化分析開始")
        print("="*80)
        print("目標: 深い洞察とバグ修正による包括的分析")
        
        try:
            # 1. データ読み込み
            if not self.load_data():
                return False
            
            # 2. 修正された基本分析
            self.basic_statistics()
            self.ministry_analysis_fixed()
            self.data_density_analysis_fixed()
            
            # 3. 新規深い分析
            self.budget_pattern_analysis()
            self.expenditure_diversity_analysis()
            self.temporal_trend_analysis()
            self.anomaly_deep_dive()
            self.project_clustering_analysis()
            self.correlation_analysis_enhanced()
            
            # 4. 包括的Insight生成
            self.generate_comprehensive_insights()
            
            # 5. 結果保存
            self.save_enhanced_results()
            
            print("\n" + "="*80)
            print("✅ 強化分析完了！")
            print("="*80)
            print(f"📊 {len(self.insights)}個の深いInsightを発見")
            print(f"📁 結果保存先: {self.output_dir}/")
            print("  - enhanced_analysis_results.json")
            print("  - enhanced_analysis_report.html")
            print("  - comprehensive_insights.txt")
            print("\n🎯 主要改善点:")
            print("  ✓ Insight重複問題を修正")
            print("  ✓ 府省庁分析の割合計算バグを修正")
            print("  ✓ 予算・支出パターンの深い分析を追加")
            print("  ✓ 事業複雑性・類型化分析を追加")
            print("  ✓ 時系列トレンド分析を強化")
            print("  ✓ 特異事業の深掘り分析を追加")
            
            return True
            
        except Exception as e:
            print(f"\n❌ 強化分析エラー: {e}")
            import traceback
            traceback.print_exc()
            return False


def main():
    analyzer = EnhancedProjectAnalyzer()
    analyzer.run_enhanced_analysis()


if __name__ == "__main__":
    main()