.PHONY: data clean release

# Pythonの仮想環境
VENV := venv
PYTHON := $(VENV)/bin/python

# データマニフェスト
MANIFEST := data/manifest.json

# リリース用ディレクトリ
RELEASE_DIR := release
RELEASE_FILE := $(RELEASE_DIR)/data.zip

# dataターゲット: データをダウンロード・展開する
# bootstrap_data.pyがダウンロードと展開を行う
data: $(PYTHON) $(MANIFEST)
	$(PYTHON) scripts/bootstrap_data.py

# releaseターゲット: データをzipに固める
# dataとdownloadsディレクトリ全体を圧縮対象とする
release: data
	@mkdir -p $(RELEASE_DIR)
	zip -r $(RELEASE_FILE) data downloads

# cleanターゲット: ダウンロードしたデータとリリースファイルを削除する
clean:
	rm -rf data/full_feather data/ai_analysis_feather data/ai_investigation data/ai_ultimate_spreadsheet data/extracted data/improved_ai_search data/normalized_feather data/performance_comparison data/rs_official_verification data/structure_analysis $(RELEASE_DIR)

# Python仮想環境のセットアップ
$(PYTHON): requirements.txt
	test -d $(VENV) || python3 -m venv $(VENV)
	$(PYTHON) -m pip install -r requirements.txt
	touch $(PYTHON)
