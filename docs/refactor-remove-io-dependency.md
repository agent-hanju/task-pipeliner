# Refactor: Remove JSONL I/O dependency from framework

**Date**: 2026-03-21
**Branch**: samples

## Summary

`task_pipeliner` 프레임워크에서 JSONL 포맷에 종속된 코드를 제거하고, SOURCE step을 사용하는 쪽에서 명시적으로 정의하도록 변경했다.

## Motivation

- 프레임워크가 특정 데이터 포맷(JSONL)에 종속되어 범용성이 떨어짐
- `Pipeline` facade가 `JsonlSourceStep`을 자동 주입하는 암묵적 동작이 있어, 파이프라인 구성을 이해하기 어려움
- sample 프로젝트들이 이미 자체 I/O를 구현하고 있어 `JsonlReader`/`JsonlWriter`가 불필요

## Changes

### Removed from framework

| File | Removed |
|------|---------|
| `src/task_pipeliner/io.py` | 파일 전체 삭제 (`JsonlReader`, `JsonlWriter`, `JsonlSourceStep`, `count_jsonl_lines`) |
| `src/task_pipeliner/__init__.py` | `JsonlReader`, `JsonlWriter`, `JsonlSourceStep` export 제거 |
| `src/task_pipeliner/pipeline.py` | `JsonlSourceStep` 자동 주입 로직, `inputs` 파라미터 제거 |
| `src/task_pipeliner/cli.py` | `run` 커맨드의 `--input` 옵션 제거, `batch` 커맨드의 `--config` 옵션 제거 (job별 config으로 변경) |
| `tests/test_io.py` | 파일 전체 삭제 |

### Modified in sample projects

| File | Change |
|------|--------|
| `sample/pretrain-data-filter/steps.py` | `LoaderStep` (SOURCE) 추가 |
| `sample/pretrain-data-filter/pipeline_config.yaml` | `loader` step 명시 |
| `sample/pretrain-data-filter/run.py` | `Pipeline` facade 대신 `PipelineEngine` + `StepRegistry` 직접 사용으로 전환 |
| `sample/pretrain-data-filter/tests/test_config.py` | loader step 추가에 따른 기대값 수정 |

### Modified in tests

| File | Change |
|------|--------|
| `tests/dummy_steps.py` | `DummyJsonlSourceStep` 추가 (테스트용 JSONL SOURCE step) |
| `tests/test_cli.py` | SOURCE step을 config에 명시하도록 테스트 수정 |
| `tests/test_init.py` | `JsonlSourceStep` 관련 assertion 제거 |

## CLI changes

### `run` command

Before:
```bash
task-pipeliner run --config pipeline.yaml --input data/ --output out/
```

After:
```bash
task-pipeliner run --config pipeline.yaml --output out/
```

입력 경로는 config YAML의 SOURCE step에서 정의한다.

### `batch` command

Before:
```bash
task-pipeliner batch --config pipeline.yaml jobs.json
# jobs.json: [{"inputs": [...], "output_dir": "..."}]
```

After:
```bash
task-pipeliner batch jobs.json
# jobs.json: [{"config": "...", "inputs": [...], "output_dir": "..."}]
```

각 job이 자체 config를 지정한다.

## Design principle

SOURCE step은 프레임워크가 암묵적으로 주입하지 않고, 사용자가 config에 명시적으로 선언한다. taxonomy-converter 패턴을 표준으로 삼는다:

```yaml
pipeline:
  - type: loader
    paths: [./data]
    outputs:
      main: next_step
  - type: next_step
    ...
```
