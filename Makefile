# Meridian library task entrypoints.
#
# Thin wrapper around npm and the dataset prep scripts. Targets here exist so
# common development, verification, and host-data layout workflows are stable
# repo-root commands and show up in `make help`.

SHELL := /bin/bash
.DEFAULT_GOAL := help

.PHONY: help setup install build typecheck test tests ci clean distclean \
        data data-fetch-ibge data-processed data-host data-check data-validate \
        smoke benchmark pack stats

MERIDIAN_DATA_DIR ?= $(CURDIR)/lib/meridian
NPM_CACHE ?= /private/tmp/meridian-npm-cache
NPM := NPM_CONFIG_CACHE=$(NPM_CACHE) npm

SRC_DIRS = src test scripts
LOC_PRUNE = \( -name node_modules -o -name dist -o -name coverage -o -name .tsup -o -name .vitest \) -prune
LOC_FIND_TYPES = \( -name '*.ts' -o -name '*.js' -o -name '*.json' -o -name '*.py' -o -name '*.md' \)
LOC_GIT_PATHS = \
	':(glob)src/**/*.ts' \
	':(glob)test/**/*.ts' \
	':(glob)scripts/**/*.py' \
	package.json \
	tsconfig.json \
	tsup.config.ts \
	vitest.config.ts \
	README.md

help: ## Show this help
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@awk 'BEGIN {FS = ":.*## "}; /^[a-zA-Z0-9_.-]+:.*## / {printf "  %-16s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

setup: install data-host ## Install dependencies and prepare default lib/meridian data links

install: ## Install npm dependencies
	$(NPM) install

build: ## Build ESM, CJS, and TypeScript declarations
	$(NPM) run build

typecheck: ## Run TypeScript without emitting files
	$(NPM) run typecheck

test: ## Build and run the Vitest suite
	$(NPM) test

tests: test ## Alias for test

ci: typecheck test pack ## Run the local CI flow

clean: ## Remove generated build and test artefacts
	rm -rf dist coverage .tsup .vitest

distclean: clean ## Remove generated artefacts and installed dependencies
	rm -rf node_modules

data: data-processed ## Build processed datasets used by the library

data-fetch-ibge: ## Fetch the correct IBGE income variables into datasets/raw/ibge
	python3 scripts/fetch_ibge_income.py

data-processed: ## Rebuild processed CSVs from raw datasets
	python3 scripts/build_processed_datasets.py

data-host: ## Symlink processed data into MERIDIAN_DATA_DIR (default: ./lib/meridian)
	@set -euo pipefail; \
	mkdir -p "$(MERIDIAN_DATA_DIR)/maxmind" "$(MERIDIAN_DATA_DIR)/ibge" "$(MERIDIAN_DATA_DIR)/ghsl"; \
	ln -sf "$(CURDIR)/datasets/maxmind/GeoLite2-City.mmdb" "$(MERIDIAN_DATA_DIR)/maxmind/GeoLite2-City.mmdb"; \
	ln -sf "$(CURDIR)/datasets/maxmind/GeoLite2-Country.mmdb" "$(MERIDIAN_DATA_DIR)/maxmind/GeoLite2-Country.mmdb"; \
	ln -sf "$(CURDIR)/datasets/maxmind/GeoLite2-ASN.mmdb" "$(MERIDIAN_DATA_DIR)/maxmind/GeoLite2-ASN.mmdb"; \
	ln -sf "$(CURDIR)/datasets/processed/ibge_municipality_income.csv" "$(MERIDIAN_DATA_DIR)/ibge/ibge_municipality_income.csv"; \
	ln -sf "$(CURDIR)/datasets/processed/ghsl_city_metrics.csv" "$(MERIDIAN_DATA_DIR)/ghsl/ghsl_city_metrics.csv"; \
	printf "Prepared Meridian data links at %s\n" "$(MERIDIAN_DATA_DIR)"

data-check: ## Verify required source and processed data files exist
	@set -euo pipefail; \
	files=( \
		"datasets/maxmind/GeoLite2-City.mmdb" \
		"datasets/maxmind/GeoLite2-Country.mmdb" \
		"datasets/maxmind/GeoLite2-ASN.mmdb" \
		"datasets/processed/ibge_municipality_income.csv" \
		"datasets/processed/ghsl_city_metrics.csv" \
	); \
	for file in "$${files[@]}"; do \
		if [ ! -f "$$file" ]; then \
			echo "Missing $$file"; \
			exit 1; \
		fi; \
		printf "ok  %s\n" "$$file"; \
	done

data-validate: build data-host ## Validate host data layout, CSV schemas, and sample lookups
	node scripts/validate_data.mjs --data-dir "$(MERIDIAN_DATA_DIR)"

smoke: build data-host ## Run a quick local library smoke test against MERIDIAN_DATA_DIR
	@node -e 'const { Meridian } = require("./dist/index.cjs"); (async () => { const m = await Meridian.open({ dataDir: process.env.MERIDIAN_DATA_DIR || "$(MERIDIAN_DATA_DIR)" }); console.log(m.sources()); console.log(m.ibge("São Paulo", "SP")); console.log(m.ghsl("São Paulo", "Brazil")); console.log(m.ip("8.8.8.8")); })().catch((error) => { console.error(error); process.exit(1); });'

N ?= 10000
benchmark: build data-host ## Benchmark open(), ip(), ibge(), and ghsl() lookups (N=10000 default)
	node scripts/benchmark.mjs --data-dir "$(MERIDIAN_DATA_DIR)" --n "$(N)"

pack: build ## Validate npm package contents without publishing
	$(NPM) pack --dry-run

stats: ## Show project LOC (current tree + historical churn)
	@current_loc=$$(find $(SRC_DIRS) $(LOC_PRUNE) -o -type f $(LOC_FIND_TYPES) -print0 2>/dev/null | xargs -0 cat 2>/dev/null | wc -l | tr -d ' ') && \
	printf "LOC\n  Current: %s\n" "$$current_loc"
	@historical_loc=$$(git log --numstat --format=tformat: -- $(LOC_GIT_PATHS) | \
		awk '($$1 ~ /^[0-9]+$$/ && $$2 ~ /^[0-9]+$$/) { total += $$1 + $$2 } END { print total + 0 }') && \
	printf "  Historical: %s\n" "$$historical_loc"
