# CLAUDE.md — Pretrain Data Filter Sample

> Follow root `CLAUDE.md` rules first. This file adds sample-specific context only.

## Overview

Sample project using task-pipeliner framework.
Reads Korean pretrain corpus (JSONL, Taxonomy/SimpleTaxonomy schema) → quality filtering (length, repetition, PII) → exact dedup → MinHash near-dedup → JSONL output (kept/removed).
Remaining filters (hangul ratio, quality heuristics, structural markers, footnote density) are planned as Phase 5 extensions.

Based on hanati-pretrain-data-filter's filtering logic (Gopher paper heuristics + Korean-specific filters).

## WBS

Development plan and progress tracking (completed): `docs/WBS-pretrain-data-filter.md`

## Commands

```bash
# Tests (sample scope only)
.venv/Scripts/pytest sample/pretrain-data-filter/tests --timeout=30 -v

# Lint & format (sample scope only)
.venv/Scripts/ruff check sample/pretrain-data-filter
.venv/Scripts/ruff format sample/pretrain-data-filter

# Type check (MYPYPATH needed to work around hyphenated directory name)
MYPYPATH=sample/pretrain-data-filter .venv/Scripts/mypy --ignore-missing-imports --explicit-package-bases sample/pretrain-data-filter/steps.py sample/pretrain-data-filter/run.py
```

## Project Structure

```
sample/pretrain-data-filter/
├── schema.py               # TaxonomyDict / SimpleTaxonomyDict TypedDict + schema detection
├── result.py               # FilterResult (BaseResult) — kept/removed/reason counts
├── filters/
│   ├── __init__.py
│   ├── length.py           # Min chars, words, mean word length
│   ├── repetition.py       # Gopher-style n-gram / line / paragraph duplication
│   └── pii.py              # Phone, RRN, BRN, email regex detection
├── steps.py                # QualityFilterStep, Hash/MinHash Compute/Lookup Steps, WriterStep
├── utils/
│   └── boilerplate.py      # 2-pass boilerplate line remover (pre-pipeline utility)
├── pipeline_config.yaml    # Pipeline DAG configuration
├── run.py                  # CLI entry point
└── tests/
    ├── dummy_data.py       # Test fixtures (dummy Korean text items)
    └── test_*.py           # Tests per module
```

## Pipeline Flow

```
Input JSONL (Taxonomy / SimpleTaxonomy)
  → QualityFilterStep (PARALLEL)       # 3 filters (length, repetition, pii), short-circuit
      → kept → HashComputeStep
      → removed → WriterStep
  → HashComputeStep (PARALLEL)         # SHA-256 해시 계산 (CPU-bound, 병렬)
      → main → HashLookupStep
  → HashLookupStep (SEQUENTIAL)        # 해시 집합 조회/삽입 (상태 의존, 순차)
      → kept → MinHashComputeStep
      → removed → WriterStep
  → MinHashComputeStep (PARALLEL)      # MinHash 시그니처 계산 (CPU-bound, 병렬)
      → main → MinHashLookupStep
  → MinHashLookupStep (SEQUENTIAL)     # LSH 인덱스 조회/삽입 (상태 의존, 순차)
      → kept → WriterStep
      → removed → WriterStep
  → WriterStep (SEQUENTIAL)            # kept.jsonl / removed.jsonl output
```

> **Dedup 설계**: 해시/시그니처 계산(PARALLEL) → 집합 조회(SEQUENTIAL) 분리.
> 큐 backpressure가 두 스텝 간 처리 속도 차이를 자연스럽게 조절한다.

## Important Notes

- All classes/functions at module level (spawn-mode multiprocessing)
- Step classes must be picklable
- Tests use dummy data from `tests/dummy_data.py`, not real corpus data
- MinHash dedup requires `datasketch` package — install with `pip install datasketch`
- Boilerplate detector (`utils/boilerplate.py`) is a 2-pass algorithm that runs **outside** the pipeline as a pre-processing step (streaming pipeline cannot do 2-pass)
- Filter functions follow `(text, **params) -> tuple[bool, str]` signature for uniformity
- News boilerplate filter is excluded from base scope (Naver-specific, 30+ regex patterns); add as extension if needed
- Reference project: `c:\workspace\hanati-pretrain-data-filter` — original standalone filter implementation
