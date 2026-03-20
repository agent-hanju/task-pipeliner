# CLAUDE.md — Naver News Taxonomy Converter Sample

> Follow root `CLAUDE.md` rules first. This file adds sample-specific context only.

## Overview

Sample project using task-pipeliner framework.
Reads Naver news article files (JSON array / JSONL) → text preprocessing → Taxonomy schema conversion → deduplication → JSONL output.

## WBS

Development plan and progress tracking: `sample/taxonomy-converter/WBS.md`

## Commands

```bash
# Tests (sample scope only)
.venv/Scripts/pytest sample/taxonomy-converter/tests --timeout=30 -v

# Lint & format (sample scope only)
.venv/Scripts/ruff check sample/taxonomy-converter
.venv/Scripts/ruff format sample/taxonomy-converter

# Type check (MYPYPATH needed to work around hyphenated directory name)
MYPYPATH=sample/taxonomy-converter .venv/Scripts/mypy --ignore-missing-imports --explicit-package-bases sample/taxonomy-converter/run.py sample/taxonomy-converter/steps.py
```

## Project Structure

```
sample/taxonomy-converter/
├── taxonomy.py            # Taxonomy schema (TypedDict) + TaxonomyResult
├── utils.py               # merge_title, parse_date, make_metadata
├── loader.py              # Multi-format file loader (JSON/JSONL, directory traversal)
├── text_rules.py          # Generic text preprocessing (normalize, line filter, inline transform)
├── naver_rules.py         # Naver news-specific rules (patterns, filters, transforms)
├── steps.py               # Pipeline Step classes (Preprocess, Convert, Deduplicate)
├── pipeline_config.yaml   # Pipeline configuration
├── run.py                 # Entry point
└── tests/
    ├── dummy_data.py      # Test fixtures (dummy news items)
    └── test_*.py          # Tests per module
```

## Pipeline Flow

```
Input files/directories (JSON array / JSONL)
  → loader.load_items()          # Format detection, date_prefix extraction from filename, yield items
  → PreprocessStep (PARALLEL)    # Normalize → document transform → line filter → line inline → validate
  → ConvertStep (PARALLEL)       # Build TaxonomyDict, merge title, generate ID
  → DeduplicateStep (SEQUENTIAL) # Deduplicate by id + text_hash
  → kept.jsonl / removed.jsonl   # Output
```

## Important Notes

- All classes/functions at module level (spawn-mode multiprocessing)
- Step classes must be picklable
- Tests use dummy data from `tests/dummy_data.py`, not real news articles
