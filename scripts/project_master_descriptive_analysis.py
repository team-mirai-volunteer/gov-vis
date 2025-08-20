#!/usr/bin/env python3
"""
事業マスターリスト記述統計分析
5,664事業×95列のデータからinsightを抽出する包括的な分析
"""
import pandas as pd
import numpy as np
import json
from pathlib import Path
from typing import Dict, List, Any, Tuple
import warnings
warnings.filterwarnings('ignore')

# 可視化ライブラリの条件付きインポート
try:
    import matplotlib.pyplot as plt
    import seaborn as sns
    # 日本語フォントの設定
    plt.rcParams['font.family'] = 'DejaVu Sans'
    sns.set_style("whitegrid")
    sns.set_palette("husl")
    VISUALIZATION_AVAILABLE = True
except ImportError:
    VISUALIZATION_AVAILABLE = False
    print("⚠️ 可視化ライブラリが利用できません。テキストベースの分析のみ実行します。")


class ProjectMasterAnalyzer:
    """事業マスターリスト記述統計分析クラス"""
    
    def __init__(self):
        self.data_dir = Path("data")
        self.project_master_path = self.data_dir / "project_master" / "rs_project_master_with_details.feather"
        self.output_dir = self.data_dir / "project_descriptive_analysis"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.df = None
        self.analysis_results = {}
        self.insights = []
        
        # 分析対象の列定義
        self.basic_info_cols = [
            '府省庁', '事業区分', '事業開始年度', '事業終了（予定）年度', 
            '主要経費', '政策', '施策', '実施方法ー直接実施', '実施方法ー補助', 
            '実施方法ー負担', '実施方法ー交付', '実施方法ー分担金・拠出金'
        ]
        
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
        print("データ読み込み開始")
        print("="*80)
        
        if not self.project_master_path.exists():
            raise FileNotFoundError(f"事業マスターリストが見つかりません: {self.project_master_path}")
        
        try:
            self.df = pd.read_feather(self.project_master_path)
            print(f"✓ データ読み込み完了: {len(self.df):,}行 × {len(self.df.columns)}列")
            
            # 基本情報を表示
            print(f"  - 事業数: {len(self.df):,}")
            print(f"  - 列数: {len(self.df.columns)}")
            print(f"  - データサイズ: {self.df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
            
            return True
            
        except Exception as e:
            print(f"✗ データ読み込みエラー: {e}")
            return False
    
    def basic_statistics(self):
        """基本統計分析"""
        print("\n" + "="*80)
        print("基本統計分析")
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
    
    def ministry_analysis(self):
        """府省庁別分析"""
        print("\n府省庁別分析:")
        
        # 府省庁別事業数
        ministry_counts = self.df['府省庁'].value_counts()
        
        analysis = {
            'total_ministries': len(ministry_counts),
            'ministry_distribution': ministry_counts.to_dict(),
            'top_10_ministries': ministry_counts.head(10).to_dict(),
            'ministry_stats': {
                'mean': ministry_counts.mean(),
                'median': ministry_counts.median(),
                'std': ministry_counts.std(),
                'max': ministry_counts.max(),
                'min': ministry_counts.min()
            }
        }
        
        print(f"  - 府省庁数: {analysis['total_ministries']}")
        print(f"  - 平均事業数/府省庁: {analysis['ministry_stats']['mean']:.1f}")
        print(f"  - 最大事業数: {analysis['ministry_stats']['max']}")
        print(f"  - 最小事業数: {analysis['ministry_stats']['min']}")
        
        print("  上位10府省庁:")
        for i, (ministry, count) in enumerate(ministry_counts.head(10).items(), 1):
            percentage = (count / len(self.df)) * 100
            print(f"    {i:2d}. {ministry}: {count:,}事業 ({percentage:.1f}%)")
        
        self.analysis_results['ministry_analysis'] = analysis
        
        # Insight抽出
        top_ministry = ministry_counts.index[0]
        top_count = ministry_counts.iloc[0]
        top_percentage = (top_count / len(self.df)) * 100
        
        self.insights.append(f"最多事業府省庁は{top_ministry}で{top_count:,}事業({top_percentage:.1f}%)を占める")
        
        if analysis['ministry_stats']['std'] > analysis['ministry_stats']['mean']:
            self.insights.append("府省庁間の事業数格差が大きい（標準偏差 > 平均値）")
        
        return analysis
    
    def project_category_analysis(self):
        """事業区分分析"""
        print("\n事業区分分析:")
        
        if '事業区分' in self.df.columns:
            category_counts = self.df['事業区分'].value_counts()
            
            analysis = {
                'total_categories': len(category_counts),
                'category_distribution': category_counts.to_dict(),
                'category_stats': {
                    'mean': category_counts.mean(),
                    'median': category_counts.median()
                }
            }
            
            print(f"  - 事業区分数: {analysis['total_categories']}")
            print("  事業区分別分布:")
            for category, count in category_counts.head(10).items():
                percentage = (count / len(self.df)) * 100
                print(f"    {category}: {count:,}事業 ({percentage:.1f}%)")
            
            self.analysis_results['project_category_analysis'] = analysis
        else:
            print("  事業区分列が見つかりません")
            self.analysis_results['project_category_analysis'] = {}
    
    def temporal_analysis(self):
        """時系列分析"""
        print("\n時系列分析:")
        
        # 事業開始年度の分析
        if '事業開始年度' in self.df.columns:
            start_years = pd.to_numeric(self.df['事業開始年度'], errors='coerce').dropna()
            
            analysis = {
                'start_year_stats': {
                    'min': int(start_years.min()) if not start_years.empty else None,
                    'max': int(start_years.max()) if not start_years.empty else None,
                    'mean': start_years.mean() if not start_years.empty else None,
                    'median': start_years.median() if not start_years.empty else None,
                    'mode': start_years.mode().iloc[0] if not start_years.empty and not start_years.mode().empty else None
                },
                'decade_distribution': {},
                'recent_trend': {}
            }
            
            # 年代別分布
            if not start_years.empty:
                decades = (start_years // 10) * 10
                decade_counts = decades.value_counts().sort_index()
                analysis['decade_distribution'] = decade_counts.to_dict()
                
                # 最近の傾向（2000年以降）
                recent_years = start_years[start_years >= 2000]
                if not recent_years.empty:
                    recent_counts = recent_years.value_counts().sort_index()
                    analysis['recent_trend'] = recent_counts.tail(10).to_dict()
            
            print(f"  - 事業開始年度範囲: {analysis['start_year_stats']['min']} - {analysis['start_year_stats']['max']}")
            print(f"  - 平均開始年度: {analysis['start_year_stats']['mean']:.1f}")
            print(f"  - 最頻開始年度: {analysis['start_year_stats']['mode']}")
            
            # 年代別分布表示
            print("  年代別事業数:")
            for decade, count in sorted(analysis['decade_distribution'].items()):
                percentage = (count / len(start_years)) * 100
                print(f"    {int(decade)}年代: {count}事業 ({percentage:.1f}%)")
            
            self.analysis_results['temporal_analysis'] = analysis
            
            # Insight抽出
            if analysis['start_year_stats']['mode'] and analysis['start_year_stats']['mode'] >= 2000:
                self.insights.append(f"{int(analysis['start_year_stats']['mode'])}年が事業開始の最頻年度")
        else:
            print("  事業開始年度列が見つかりません")
    
    def data_density_analysis(self):
        """データ密度分析"""
        print("\n" + "="*80)
        print("データ密度分析")
        print("="*80)
        
        # 各テーブルのデータ保有率
        data_availability = {}
        total_records_stats = {}
        
        for col in self.count_cols:
            if col in self.df.columns:
                table_name = col.replace('_count', '')
                has_data = self.df[col] > 0
                
                data_availability[table_name] = {
                    'projects_with_data': has_data.sum(),
                    'coverage_rate': (has_data.sum() / len(self.df)) * 100,
                    'avg_records_per_project': self.df[col].mean(),
                    'max_records': self.df[col].max(),
                    'std_records': self.df[col].std()
                }
        
        # Total related recordsの分析
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
        
        print("テーブル別データ保有状況:")
        for table_name, stats in data_availability.items():
            print(f"  {table_name}:")
            print(f"    - データ有り事業: {stats['projects_with_data']:,}/{len(self.df):,} ({stats['coverage_rate']:.1f}%)")
            print(f"    - 平均レコード数/事業: {stats['avg_records_per_project']:.1f}")
            print(f"    - 最大レコード数: {stats['max_records']}")
        
        if total_records_stats:
            print(f"\n関連レコード総数統計:")
            print(f"  - 平均: {total_records_stats['mean']:.1f}レコード/事業")
            print(f"  - 中央値: {total_records_stats['median']:.1f}")
            print(f"  - 90パーセンタイル: {total_records_stats['percentiles']['90th']:.1f}")
            print(f"  - 95パーセンタイル: {total_records_stats['percentiles']['95th']:.1f}")
            print(f"  - 最大値: {total_records_stats['max']}")
        
        self.analysis_results['data_density_analysis'] = analysis
        
        # Insight抽出
        highest_coverage = max(data_availability.items(), key=lambda x: x[1]['coverage_rate'])
        lowest_coverage = min(data_availability.items(), key=lambda x: x[1]['coverage_rate'])
        
        self.insights.append(f"{highest_coverage[0]}が最高のデータ保有率({highest_coverage[1]['coverage_rate']:.1f}%)")
        self.insights.append(f"{lowest_coverage[0]}が最低のデータ保有率({lowest_coverage[1]['coverage_rate']:.1f}%)")
        
        if total_records_stats and total_records_stats['max'] > total_records_stats['mean'] * 3:
            self.insights.append(f"データ密度に大きなばらつきあり（最大{total_records_stats['max']}vs平均{total_records_stats['mean']:.1f}）")
        
        return analysis
    
    def implementation_method_analysis(self):
        """実施方法分析"""
        print("\n実施方法分析:")
        
        implementation_cols = [
            '実施方法ー直接実施', '実施方法ー補助', '実施方法ー負担', 
            '実施方法ー交付', '実施方法ー分担金・拠出金', '実施方法ーその他'
        ]
        
        implementation_stats = {}
        
        for col in implementation_cols:
            if col in self.df.columns:
                # ブール値として解釈（1/0 または True/False）
                values = pd.to_numeric(self.df[col], errors='coerce').fillna(0)
                count = (values > 0).sum()
                percentage = (count / len(self.df)) * 100
                
                method_name = col.replace('実施方法ー', '')
                implementation_stats[method_name] = {
                    'count': count,
                    'percentage': percentage
                }
                
                print(f"  {method_name}: {count:,}事業 ({percentage:.1f}%)")
        
        self.analysis_results['implementation_method_analysis'] = implementation_stats
        
        # Insight抽出
        if implementation_stats:
            most_common = max(implementation_stats.items(), key=lambda x: x[1]['count'])
            self.insights.append(f"{most_common[0]}が最も多い実施方法({most_common[1]['count']:,}事業)")
        
        return implementation_stats
    
    def json_data_analysis(self):
        """JSON詳細データ分析"""
        print("\n" + "="*80)
        print("JSON詳細データ分析")
        print("="*80)
        
        json_insights = {}
        
        # 予算情報の分析（budget_summary_json）
        if 'budget_summary_json' in self.df.columns:
            budget_analysis = self.analyze_budget_json()
            json_insights['budget'] = budget_analysis
        
        # 支出情報の分析（expenditure_info_json）
        if 'expenditure_info_json' in self.df.columns:
            expenditure_analysis = self.analyze_expenditure_json()
            json_insights['expenditure'] = expenditure_analysis
        
        # 目標情報の分析（goals_performance_json）
        if 'goals_performance_json' in self.df.columns:
            goals_analysis = self.analyze_goals_json()
            json_insights['goals'] = goals_analysis
        
        self.analysis_results['json_data_analysis'] = json_insights
        return json_insights
    
    def analyze_budget_json(self):
        """予算JSONデータの分析"""
        print("\n予算情報分析:")
        
        budget_data = []
        budget_amounts = []
        
        for idx, json_str in enumerate(self.df['budget_summary_json'].dropna()):
            try:
                if json_str and json_str != 'null':
                    records = json.loads(json_str)
                    if isinstance(records, list):
                        for record in records:
                            if isinstance(record, dict):
                                # 予算額の抽出を試行
                                for key, value in record.items():
                                    if any(budget_key in key for budget_key in ['予算', '金額', '額']):
                                        if isinstance(value, (int, float)):
                                            budget_amounts.append(value)
                                        elif isinstance(value, str):
                                            # 数値の抽出を試行
                                            import re
                                            numbers = re.findall(r'[\d,]+', str(value).replace(',', ''))
                                            for num_str in numbers:
                                                try:
                                                    budget_amounts.append(float(num_str))
                                                except:
                                                    pass
                        budget_data.append(len(records))
            except:
                continue
        
        analysis = {
            'projects_with_budget_data': len(budget_data),
            'avg_budget_records_per_project': np.mean(budget_data) if budget_data else 0,
            'budget_amounts_found': len(budget_amounts),
            'budget_stats': {}
        }
        
        if budget_amounts:
            # 異常に大きな値を除外（上位1%を除去）
            budget_amounts = np.array(budget_amounts)
            budget_amounts = budget_amounts[budget_amounts <= np.percentile(budget_amounts, 99)]
            
            if len(budget_amounts) > 0:
                analysis['budget_stats'] = {
                    'mean': np.mean(budget_amounts),
                    'median': np.median(budget_amounts),
                    'std': np.std(budget_amounts),
                    'min': np.min(budget_amounts),
                    'max': np.max(budget_amounts),
                    'count': len(budget_amounts)
                }
        
        print(f"  - 予算データ有り事業: {analysis['projects_with_budget_data']:,}")
        print(f"  - 平均予算レコード数: {analysis['avg_budget_records_per_project']:.1f}")
        print(f"  - 抽出された予算額数: {analysis['budget_amounts_found']:,}")
        
        if analysis['budget_stats']:
            stats = analysis['budget_stats']
            print(f"  - 予算額統計 (n={stats['count']:,}):")
            print(f"    平均: {stats['mean']:,.0f}")
            print(f"    中央値: {stats['median']:,.0f}")
            print(f"    最大: {stats['max']:,.0f}")
            print(f"    最小: {stats['min']:,.0f}")
        
        return analysis
    
    def analyze_expenditure_json(self):
        """支出JSONデータの分析"""
        print("\n支出情報分析:")
        
        expenditure_entities = []
        expenditure_amounts = []
        
        for idx, json_str in enumerate(self.df['expenditure_info_json'].dropna()):
            try:
                if json_str and json_str != 'null':
                    records = json.loads(json_str)
                    if isinstance(records, list):
                        unique_entities = set()
                        for record in records:
                            if isinstance(record, dict):
                                # 支出先名の抽出
                                if '支出先名' in record and record['支出先名']:
                                    unique_entities.add(record['支出先名'])
                                
                                # 金額の抽出
                                for key, value in record.items():
                                    if '金額' in key or '額' in key:
                                        if isinstance(value, (int, float)):
                                            expenditure_amounts.append(value)
                        
                        expenditure_entities.append(len(unique_entities))
            except:
                continue
        
        analysis = {
            'projects_with_expenditure_data': len(expenditure_entities),
            'avg_expenditure_entities_per_project': np.mean(expenditure_entities) if expenditure_entities else 0,
            'expenditure_amounts_found': len(expenditure_amounts),
            'expenditure_diversity_stats': {}
        }
        
        if expenditure_entities:
            analysis['expenditure_diversity_stats'] = {
                'mean': np.mean(expenditure_entities),
                'median': np.median(expenditure_entities),
                'max': np.max(expenditure_entities),
                'min': np.min(expenditure_entities),
                'std': np.std(expenditure_entities)
            }
        
        print(f"  - 支出データ有り事業: {analysis['projects_with_expenditure_data']:,}")
        print(f"  - 平均支出先数/事業: {analysis['avg_expenditure_entities_per_project']:.1f}")
        
        if analysis['expenditure_diversity_stats']:
            stats = analysis['expenditure_diversity_stats']
            print(f"  - 支出先多様性統計:")
            print(f"    平均支出先数: {stats['mean']:.1f}")
            print(f"    最大支出先数: {stats['max']}")
            print(f"    標準偏差: {stats['std']:.1f}")
        
        return analysis
    
    def analyze_goals_json(self):
        """目標JSONデータの分析"""
        print("\n目標・実績分析:")
        
        goals_counts = []
        performance_data = []
        
        for idx, json_str in enumerate(self.df['goals_performance_json'].dropna()):
            try:
                if json_str and json_str != 'null':
                    records = json.loads(json_str)
                    if isinstance(records, list):
                        goals_counts.append(len(records))
                        
                        for record in records:
                            if isinstance(record, dict):
                                # 目標・実績データの抽出
                                for key, value in record.items():
                                    if any(goal_key in key for goal_key in ['目標', '実績', '達成率']):
                                        performance_data.append(key)
            except:
                continue
        
        analysis = {
            'projects_with_goals_data': len(goals_counts),
            'avg_goals_per_project': np.mean(goals_counts) if goals_counts else 0,
            'goals_stats': {},
            'performance_fields_found': len(performance_data)
        }
        
        if goals_counts:
            analysis['goals_stats'] = {
                'mean': np.mean(goals_counts),
                'median': np.median(goals_counts),
                'max': np.max(goals_counts),
                'min': np.min(goals_counts),
                'std': np.std(goals_counts)
            }
        
        print(f"  - 目標データ有り事業: {analysis['projects_with_goals_data']:,}")
        print(f"  - 平均目標数/事業: {analysis['avg_goals_per_project']:.1f}")
        print(f"  - 実績フィールド発見数: {analysis['performance_fields_found']:,}")
        
        if analysis['goals_stats']:
            stats = analysis['goals_stats']
            print(f"  - 目標設定統計:")
            print(f"    最大目標数: {stats['max']}")
            print(f"    平均目標数: {stats['mean']:.1f}")
        
        return analysis
    
    def outlier_detection(self):
        """異常値検出・特殊事業の抽出"""
        print("\n" + "="*80)
        print("異常値検出・特殊事業分析")
        print("="*80)
        
        outliers = {}
        
        # データ密度の異常値
        if 'total_related_records' in self.df.columns:
            total_records = self.df['total_related_records']
            q95 = total_records.quantile(0.95)
            q99 = total_records.quantile(0.99)
            
            high_density_projects = self.df[total_records >= q95]
            ultra_high_density = self.df[total_records >= q99]
            
            outliers['high_data_density'] = {
                'q95_threshold': q95,
                'q99_threshold': q99,
                'high_density_count': len(high_density_projects),
                'ultra_high_density_count': len(ultra_high_density),
                'top_projects': high_density_projects.nlargest(5, 'total_related_records')[
                    ['事業名', '府省庁', 'total_related_records']].to_dict('records')
            }
            
            print(f"高データ密度事業:")
            print(f"  - 95パーセンタイル以上: {len(high_density_projects)}事業 (閾値: {q95:.0f})")
            print(f"  - 99パーセンタイル以上: {len(ultra_high_density)}事業 (閾値: {q99:.0f})")
            print(f"  データ密度トップ5:")
            for i, project in enumerate(outliers['high_data_density']['top_projects'], 1):
                print(f"    {i}. {project['事業名'][:50]}... ({project['府省庁']}) - {project['total_related_records']}レコード")
        
        # 各テーブル別異常値
        for col in self.count_cols:
            if col in self.df.columns:
                table_name = col.replace('_count', '')
                values = self.df[col]
                q95 = values.quantile(0.95)
                
                if q95 > 0:
                    high_count_projects = self.df[values >= q95]
                    outliers[f'{table_name}_high_count'] = {
                        'threshold': q95,
                        'count': len(high_count_projects),
                        'top_projects': high_count_projects.nlargest(3, col)[
                            ['事業名', '府省庁', col]].to_dict('records')
                    }
        
        self.analysis_results['outlier_detection'] = outliers
        
        # Insight抽出
        if 'high_data_density' in outliers:
            top_project = outliers['high_data_density']['top_projects'][0]
            self.insights.append(f"最高データ密度事業: 「{top_project['事業名'][:30]}...」({top_project['total_related_records']}レコード)")
        
        return outliers
    
    def correlation_analysis(self):
        """相関・パターン分析"""
        print("\n" + "="*80)
        print("相関・パターン分析")
        print("="*80)
        
        correlations = {}
        
        # 数値列の抽出
        numeric_cols = []
        for col in self.count_cols + ['total_related_records']:
            if col in self.df.columns:
                numeric_cols.append(col)
        
        if len(numeric_cols) >= 2:
            # 相関行列の計算
            corr_matrix = self.df[numeric_cols].corr()
            
            # 高相関ペアの抽出
            high_correlations = []
            for i in range(len(corr_matrix.columns)):
                for j in range(i+1, len(corr_matrix.columns)):
                    corr_value = corr_matrix.iloc[i, j]
                    if abs(corr_value) > 0.5:  # 相関係数の閾値
                        high_correlations.append({
                            'var1': corr_matrix.columns[i],
                            'var2': corr_matrix.columns[j],
                            'correlation': corr_value
                        })
            
            correlations['correlation_matrix'] = corr_matrix.to_dict()
            correlations['high_correlations'] = high_correlations
            
            print(f"高相関ペア (|r| > 0.5):")
            for corr in sorted(high_correlations, key=lambda x: abs(x['correlation']), reverse=True):
                var1_name = corr['var1'].replace('_count', '').replace('_', ' ')
                var2_name = corr['var2'].replace('_count', '').replace('_', ' ')
                print(f"  {var1_name} ⟷ {var2_name}: r = {corr['correlation']:.3f}")
        
        # 府省庁とデータ密度の関係
        if '府省庁' in self.df.columns and 'total_related_records' in self.df.columns:
            ministry_density = self.df.groupby('府省庁')['total_related_records'].agg(['mean', 'std', 'count']).round(2)
            ministry_density = ministry_density[ministry_density['count'] >= 10].sort_values('mean', ascending=False)
            
            correlations['ministry_data_density'] = ministry_density.head(10).to_dict()
            
            print(f"\n府省庁別データ密度 (10事業以上):")
            for ministry, stats in ministry_density.head(10).iterrows():
                print(f"  {ministry}: 平均{stats['mean']:.1f}レコード (σ={stats['std']:.1f}, n={stats['count']})")
        
        self.analysis_results['correlation_analysis'] = correlations
        
        # Insight抽出
        if high_correlations:
            strongest = max(high_correlations, key=lambda x: abs(x['correlation']))
            self.insights.append(f"最強の相関: {strongest['var1']} と {strongest['var2']} (r={strongest['correlation']:.3f})")
        
        return correlations
    
    def generate_insights_summary(self):
        """Insight総括"""
        print("\n" + "="*80)
        print("🔍 発見されたInsight")
        print("="*80)
        
        for i, insight in enumerate(self.insights, 1):
            print(f"{i:2d}. {insight}")
        
        # 追加的なInsight生成
        additional_insights = []
        
        # データ完全性に関するInsight
        if 'basic_statistics' in self.analysis_results:
            completeness = self.analysis_results['basic_statistics']['data_completeness']
            if completeness > 90:
                additional_insights.append(f"高いデータ完全性を実現({completeness:.1f}%)")
            elif completeness < 70:
                additional_insights.append(f"データ欠損が多い状況({completeness:.1f}%)")
        
        # 規模に関するInsight
        if 'ministry_analysis' in self.analysis_results:
            total_ministries = self.analysis_results['ministry_analysis']['total_ministries']
            if total_ministries > 20:
                additional_insights.append(f"多数の府省庁が参画({total_ministries}府省庁)")
        
        # JSON活用に関するInsight
        if 'json_data_analysis' in self.analysis_results:
            json_analysis = self.analysis_results['json_data_analysis']
            if 'budget' in json_analysis and json_analysis['budget']['projects_with_budget_data'] > 1000:
                additional_insights.append("予算詳細データの充実度が高い")
        
        # 追加Insightの表示
        if additional_insights:
            print("\n追加発見:")
            for i, insight in enumerate(additional_insights, len(self.insights) + 1):
                print(f"{i:2d}. {insight}")
        
        self.insights.extend(additional_insights)
        
        return self.insights
    
    def save_results(self):
        """結果保存"""
        print("\n" + "="*80)
        print("結果保存")
        print("="*80)
        
        # JSON結果保存
        results_path = self.output_dir / "descriptive_analysis_results.json"
        with open(results_path, 'w', encoding='utf-8') as f:
            json.dump(self.analysis_results, f, ensure_ascii=False, indent=2, default=str)
        print(f"✓ 分析結果保存: {results_path}")
        
        # Insight保存
        insights_path = self.output_dir / "key_insights.txt"
        with open(insights_path, 'w', encoding='utf-8') as f:
            f.write("事業マスターリスト分析 - 主要Insight\n")
            f.write("="*50 + "\n\n")
            for i, insight in enumerate(self.insights, 1):
                f.write(f"{i:2d}. {insight}\n")
        print(f"✓ Insight保存: {insights_path}")
        
        # HTMLレポート生成
        self.generate_html_report()
        
        return True
    
    def generate_html_report(self):
        """HTMLレポート生成"""
        html_content = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <title>事業マスターリスト記述統計分析レポート</title>
    <style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 20px; background: #f9fafb; color: #111827; }}
        .container {{ max-width: 1200px; margin: 0 auto; background: #ffffff; padding: 30px; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
        h1 {{ color: #1f2937; text-align: center; border-bottom: 3px solid #3b82f6; padding-bottom: 15px; }}
        h2 {{ color: #1f2937; margin-top: 30px; border-left: 5px solid #3b82f6; padding-left: 10px; }}
        .summary {{ background: #3b82f6; color: white; padding: 20px; border-radius: 8px; margin: 20px 0; }}
        .metric {{ display: inline-block; margin: 10px 20px; }}
        .metric-value {{ font-size: 2em; font-weight: bold; }}
        .metric-label {{ font-size: 0.9em; opacity: 0.9; }}
        table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        th {{ background: #f3f4f6; color: #1f2937; padding: 12px; text-align: left; border-bottom: 2px solid #e5e7eb; }}
        td {{ padding: 10px; border-bottom: 1px solid #e5e7eb; color: #374151; }}
        tr:hover {{ background: #f9fafb; }}
        .insight {{ background: #eff6ff; border-left: 4px solid #3b82f6; padding: 15px; margin: 10px 0; color: #1e40af; }}
        .code {{ background: #f3f4f6; padding: 10px; border-radius: 5px; font-family: monospace; color: #111827; }}
        .chart-placeholder {{ background: #f3f4f6; padding: 40px; text-align: center; border: 2px dashed #d1d5db; margin: 20px 0; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 事業マスターリスト記述統計分析レポート</h1>
        
        <div class="summary">
            <h2 style="color: white; margin-top: 0;">分析概要</h2>
"""
        
        # 基本統計の表示
        if 'basic_statistics' in self.analysis_results:
            stats = self.analysis_results['basic_statistics']
            html_content += f"""
            <div class="metric">
                <div class="metric-value">{stats['total_projects']:,}</div>
                <div class="metric-label">総事業数</div>
            </div>
            <div class="metric">
                <div class="metric-value">{stats['total_columns']}</div>
                <div class="metric-label">総列数</div>
            </div>
            <div class="metric">
                <div class="metric-value">{stats['data_completeness']:.1f}%</div>
                <div class="metric-label">データ完全性</div>
            </div>
"""
        
        html_content += """
        </div>
        
        <h2>🎯 主要Insight</h2>
"""
        
        # Insight表示
        for i, insight in enumerate(self.insights, 1):
            html_content += f'        <div class="insight">{i}. {insight}</div>\n'
        
        # 府省庁分析
        if 'ministry_analysis' in self.analysis_results:
            ministry_data = self.analysis_results['ministry_analysis']
            html_content += """
        <h2>🏛️ 府省庁分析</h2>
        <table>
            <tr>
                <th>順位</th>
                <th>府省庁</th>
                <th>事業数</th>
                <th>割合</th>
            </tr>
"""
            for i, (ministry, count) in enumerate(ministry_data['top_10_ministries'].items(), 1):
                percentage = (count / ministry_data.get('total_ministries', 1)) * 100
                html_content += f"""
            <tr>
                <td>{i}</td>
                <td>{ministry}</td>
                <td>{count:,}</td>
                <td>{percentage:.1f}%</td>
            </tr>
"""
            html_content += "        </table>"
        
        # データ密度分析
        if 'data_density_analysis' in self.analysis_results:
            density_data = self.analysis_results['data_density_analysis']
            html_content += """
        <h2>📈 データ密度分析</h2>
        <table>
            <tr>
                <th>テーブル名</th>
                <th>データ保有率</th>
                <th>平均レコード数/事業</th>
                <th>最大レコード数</th>
            </tr>
"""
            for table_name, stats in density_data['data_availability'].items():
                html_content += f"""
            <tr>
                <td>{table_name}</td>
                <td>{stats['coverage_rate']:.1f}%</td>
                <td>{stats['avg_records_per_project']:.1f}</td>
                <td>{stats['max_records']}</td>
            </tr>
"""
            html_content += "        </table>"
        
        # 異常値情報
        if 'outlier_detection' in self.analysis_results and 'high_data_density' in self.analysis_results['outlier_detection']:
            outlier_data = self.analysis_results['outlier_detection']['high_data_density']
            html_content += """
        <h2>⚡ 特殊事業（高データ密度）</h2>
        <table>
            <tr>
                <th>事業名</th>
                <th>府省庁</th>
                <th>関連レコード数</th>
            </tr>
"""
            for project in outlier_data['top_projects']:
                html_content += f"""
            <tr>
                <td>{project['事業名'][:60]}...</td>
                <td>{project['府省庁']}</td>
                <td>{project['total_related_records']:,}</td>
            </tr>
"""
            html_content += "        </table>"
        
        html_content += """
        <div style="text-align: center; margin-top: 40px; color: #7f8c8d; font-size: 0.9em;">
            事業マスターリスト記述統計分析レポート - RS Visualization System
        </div>
    </div>
</body>
</html>
"""
        
        report_path = self.output_dir / "descriptive_analysis_report.html"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"✓ HTMLレポート保存: {report_path}")
    
    def run_analysis(self):
        """分析実行メイン"""
        print("\n" + "="*80)
        print("事業マスターリスト記述統計分析開始")
        print("="*80)
        print("目標: 5,664事業×95列からのInsight抽出")
        
        try:
            # 1. データ読み込み
            if not self.load_data():
                return False
            
            # 2. 基本統計
            self.basic_statistics()
            
            # 3. 分布分析
            self.ministry_analysis()
            self.project_category_analysis()
            self.temporal_analysis()
            self.implementation_method_analysis()
            
            # 4. データ密度分析
            self.data_density_analysis()
            
            # 5. JSON詳細データ分析
            self.json_data_analysis()
            
            # 6. 異常値検出
            self.outlier_detection()
            
            # 7. 相関分析
            self.correlation_analysis()
            
            # 8. Insight総括
            self.generate_insights_summary()
            
            # 9. 結果保存
            self.save_results()
            
            print("\n" + "="*80)
            print("✅ 記述統計分析完了！")
            print("="*80)
            print(f"📊 {len(self.insights)}個のInsightを発見")
            print(f"📁 結果保存先: {self.output_dir}/")
            print("  - descriptive_analysis_results.json")
            print("  - descriptive_analysis_report.html")
            print("  - key_insights.txt")
            
            return True
            
        except Exception as e:
            print(f"\n❌ 分析エラー: {e}")
            import traceback
            traceback.print_exc()
            return False


def main():
    analyzer = ProjectMasterAnalyzer()
    analyzer.run_analysis()


if __name__ == "__main__":
    main()
