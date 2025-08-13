# RSシステム データ処理・分析プロジェクト

RSシステム（行政事業レビュー見える化サイト）のデータをダウンロード・処理・分析するためのPythonプロジェクトです。特にAI関連事業の包括的検索と正規化されたデータ構造による高速分析に特化しています。

## セットアップ

### 1. 仮想環境の作成と有効化

```bash
# 仮想環境を作成
python3 -m venv venv

# 仮想環境を有効化（Linux/Mac）
source venv/bin/activate

# 仮想環境を有効化（Windows）
venv\Scripts\activate
```

### 2. 依存パッケージのインストール

```bash
pip install -r requirements.txt
pip install polars pyarrow  # 高速データ処理用（オプション）
```

## 使い方

### 手動ダウンロード方式（推奨）

RSシステムのデータは手動でダウンロードする必要があります。

1. [RSシステム](https://rssystem.go.jp)にアクセス
2. データダウンロードページから必要なZIPファイルをダウンロード
3. `downloads`ディレクトリを作成し、ダウンロードしたZIPファイルを配置

```bash
mkdir downloads
# ダウンロードしたZIPファイルをdownloadsフォルダに配置
```

4. データ処理スクリプトを実行

```bash
# 基本データ処理（従来方式）
python scripts/process_local_data.py

# データ構造分析
python scripts/data_structure_analyzer.py

# 正規化・Feather変換（推奨）
python scripts/quick_feather_converter.py

# AI関連事業の包括的検索
python scripts/feather_ai_search.py
```

### 自動ダウンロード試行（制限あり）

```bash
python scripts/fetch_rs_data.py
```

注意: SPA構造により直接HTTP取得は制限されています。手動ダウンロードが確実です。

## プロジェクト構造

```
rs-visualization/
├── scripts/
│   ├── process_local_data.py        # 基本データ処理（従来方式）
│   ├── data_structure_analyzer.py  # データ構造詳細分析
│   ├── quick_feather_converter.py  # 正規化・Feather変換
│   ├── feather_ai_search.py        # AI関連事業検索
│   ├── performance_comparison_report.py # パフォーマンス比較
│   └── fetch_rs_data.py            # 自動ダウンロード（実験的）
├── downloads/                       # 手動ダウンロードZIPファイル配置用
├── data/
│   ├── extracted/                   # 解凍された元データ（15ファイル）
│   ├── processed/                   # 統合CSV（553,094行）
│   ├── normalized_feather/          # 正規化Featherテーブル（5テーブル）
│   ├── structure_analysis/          # データ構造分析結果
│   ├── ai_analysis_feather/         # AI検索結果（新手法）
│   ├── performance_comparison/      # パフォーマンス比較レポート
│   └── reports/                     # 基本分析レポート
├── requirements.txt                 # 基本依存パッケージ
├── CLAUDE.md                        # プロジェクト詳細ガイド
├── .gitignore                       # Git除外設定
└── README.md                        # このファイル
```

## 主要機能

### 1. データ処理・構造分析
- **ZIP解凍・CSV処理**: 15ファイルの自動処理と日本語エンコーディング検出
- **データ構造分析**: 全251フィールドの詳細分析とリレーション性調査
- **データ品質評価**: 欠損値・重複・正規化可能性の包括的評価

### 2. 正規化・最適化
- **リレーショナル正規化**: 5つのテーブルに効率的に分離
  - `projects`: 事業マスター（5,664事業）
  - `expenditure_info`: 支出情報（173,359レコード）
  - `goals_performance`: 目標・実績（117,971レコード） 
  - `expenditure_connections`: 支出先関係（20,074レコード）
  - `contracts`: 契約情報（4,149レコード）
- **Feather変換**: 93%のサイズ削減（229MB → 15MB）と高速アクセス

### 3. AI関連事業の包括的検索
- **包括的検索パターン**: 86種類のAI関連用語・技術での検索
- **マルチテーブル検索**: 5テーブル横断での関連情報検索
- **高精度検索**: 正規表現による文脈を考慮した検索

### 4. 分析・レポート生成
- **比較分析**: 従来手法との詳細パフォーマンス比較
- **統計レポート**: 府省庁別・用語別・テーブル別の詳細統計
- **可視化レポート**: HTML形式でのインタラクティブ分析結果

## 2024年度データ処理実績

### 📊 データセット概要
- **処理ファイル数**: 15 ZIPファイル（1.2GB）
- **総レコード数**: 553,094行（統合前）
- **ユニーク事業数**: 5,664事業
- **データカテゴリ**: 基本情報・予算執行・効果発現・支出先・評価（5分野）

### 🏗️ データ構造変換結果

| 処理段階 | データ形式 | サイズ | レコード数 | 特徴 |
|---------|-----------|--------|------------|------|
| **元データ** | 15 CSV | 229MB | 553,094行 | 縦結合・重複あり |
| **正規化後** | 5 Featherテーブル | 15MB | 321,217行 | リレーショナル構造 |

### 🔍 AI関連事業検索結果

| 検索手法 | AI関連事業（広範囲） | AI限定事業 | 実行時間 | 改善率 |
|---------|-------------------|-----------|---------|--------|
| **従来手法** | 52件 (0.92%) | 4件 (0.07%) | 76.3秒 | - |
| **正規化手法** | **892件 (15.7%)** | **57件 (1.0%)** | **44.6秒** | **+1,615%** |

## 🗂️ 出力ファイル・レポート

### データ構造分析
- `data/structure_analysis/detailed_structure_analysis.json`: 15ファイルの詳細構造分析
- `data/structure_analysis/structure_analysis_report.html`: データ構造分析の視覚化レポート

### 正規化データ
- `data/normalized_feather/*.feather`: 正規化された5つのFeatherテーブル
- `data/normalized_feather/ai_search_metadata.json`: AI検索用メタデータ

### AI関連事業検索結果
- `data/ai_analysis_feather/ai_related_projects_feather.json`: AI関連事業892件の詳細データ
- `data/ai_analysis_feather/ai_only_projects_feather.json`: AI限定事業57件の詳細データ
- `data/ai_analysis_feather/feather_search_report.html`: AI検索結果の可視化レポート

### パフォーマンス比較
- `data/performance_comparison/performance_comparison_report.html`: 手法比較の詳細レポート
- `data/performance_comparison/performance_comparison_report.json`: 比較データ（JSON形式）

## トラブルシューティング

### エンコーディングエラー

日本語データのエンコーディング問題が発生した場合、スクリプトは自動的に以下のエンコーディングを試行します：
- UTF-8
- Shift-JIS
- CP932
- UTF-8 with BOM
- ISO-2022-JP

### メモリ不足

大規模なデータセットを処理する際にメモリ不足が発生する場合：
1. データを分割して処理
2. `chunksize`パラメータを使用した段階的読み込み

## 🚀 技術的成果・改善

### データ処理パフォーマンス
| 指標 | 従来手法 | 正規化手法 | 改善 |
|------|----------|------------|------|
| **データサイズ** | 229MB | 15MB | **93%削減** |
| **処理時間** | 76.3秒 | 44.6秒 | **71%高速化** |
| **メモリ効率** | 単一巨大テーブル | 分散テーブル | **最適化** |
| **検索精度** | 限定フィールド | 全フィールド | **包括的** |

### AI関連事業発見の飛躍的向上
- **従来見落とし**: 840件のAI関連事業を新たに発見
- **検索範囲拡大**: 6フィールド → 全テキストフィールド（5テーブル横断）
- **パターン強化**: 19用語 → 86種類の包括的AI関連パターン
- **府省庁分布**: 経済産業省133件、国土交通省106件、デジタル庁101件が上位

### 正規化による構造改善
- **リレーショナル設計**: 事業マスターと関連テーブルの適切な分離
- **重複排除**: 同一情報の重複スキャンを排除
- **型最適化**: Feather形式による効率的データ型とカテゴリ最適化
- **拡張性**: 新しい分析要件への柔軟な対応が可能

## ライセンス

このプロジェクトはMITライセンスの下で公開されています。

## 貢献

バグ報告や機能提案は、GitHubのIssuesページでお願いします。