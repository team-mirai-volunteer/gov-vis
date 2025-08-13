.PHONY: help data build clean

# ====================================================================================
# VARIABLES
# ====================================================================================
PYTHON = python3
VENV_DIR = .venv
DATA_DIR = data
DOWNLOAD_DIR = downloads
MANIFEST = $(DATA_DIR)/manifest.json
BOOTSTRAP_SCRIPT = scripts/bootstrap_data.py

# ====================================================================================
# HELP
# ====================================================================================
help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  help          Show this help message."
	@echo "  data          Download and unpack data based on the manifest."
	@echo "  build         Process raw data into final datasets (e.g., feather files)."
	@echo "  clean         Remove downloaded and generated data."
	@echo ""
	@echo "Example:"
	@echo "  make data     # Download all required data"
	@echo "  make build    # Run all processing scripts"


# ====================================================================================
# DATA MANAGEMENT
# ====================================================================================
data:
	@echo "--- Downloading and setting up data..."
	$(PYTHON) $(BOOTSTRAP_SCRIPT) --manifest $(MANIFEST) --output-dir $(DOWNLOAD_DIR)

# ====================================================================================
# PROJECT BUILD
# ====================================================================================
build:
	@echo "--- Processing data..."
	# Add commands to run your data processing scripts here
	# For example:
	# $(PYTHON) scripts/process_local_data.py
	# $(PYTHON) scripts/full_feather_converter.py
	@echo "Build process needs to be defined based on script dependencies."

# ====================================================================================
# CLEANUP
# ====================================================================================
clean:
	@echo "--- Cleaning up downloaded and generated data..."
	rm -rf $(DOWNLOAD_DIR)
	rm -rf $(DATA_DIR)/extracted
	rm -rf $(DATA_DIR)/full_feather
	@echo "Cleanup complete."
