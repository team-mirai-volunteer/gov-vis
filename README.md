# RSシステム データ可視化プロジェクト

RSシステム（行政事業レビュー見える化サイト）のデータをダウンロード・処理・可視化するためのPythonプロジェクトです。

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
python scripts/process_local_data.py
```

### 自動ダウンロード試行（実験的）

```bash
python scripts/fetch_rs_data.py
```

注意: RSシステムのWebサイト構造により、自動ダウンロードが機能しない場合があります。

## プロジェクト構造

```
rs-visualization/
├── scripts/
│   ├── fetch_rs_data.py      # 自動ダウンロードスクリプト（実験的）
│   └── process_local_data.py # ローカルデータ処理スクリプト
├── downloads/                 # 手動でダウンロードしたZIPファイル配置用
├── data/
│   ├── raw/                  # ダウンロードした生データ
│   ├── extracted/             # 解凍されたデータ
│   ├── processed/             # 処理済みデータ
│   └── reports/               # 分析レポート
├── requirements.txt           # Python依存パッケージ
├── .gitignore                # Git除外設定
└── README.md                 # このファイル
```

## 機能

- **データダウンロード**: RSシステムからのデータ取得（手動/自動）
- **ZIP解凍**: ダウンロードしたZIPファイルの自動解凍
- **データ分析**: CSV/Excelファイルの構造分析
  - カラム情報
  - データ型
  - 欠損値の確認
  - 統計情報
- **データマージ**: 複数のCSVファイルの結合
- **レポート生成**: 
  - JSON形式の詳細レポート
  - HTML形式の可視化レポート

## 出力ファイル

処理完了後、以下のファイルが生成されます：

- `data/reports/analysis_report.json`: 詳細な分析結果（JSON形式）
- `data/reports/analysis_report.html`: 視覚的な分析レポート（HTML形式）
- `data/processed/merged_data.csv`: マージされたデータ（CSV形式）

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

## ライセンス

このプロジェクトはMITライセンスの下で公開されています。

## 貢献

バグ報告や機能提案は、GitHubのIssuesページでお願いします。