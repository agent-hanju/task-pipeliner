# WBS — pretrain-data-filter (task-pipeliner sample)

> hanati-pretrain-data-filter의 필터링 + 중복 제거 로직을
> task-pipeliner 프레임워크 위에서 파이프라인으로 재구현한다.

---

## 의존 관계 맵

```
[W-01 schema] ──┬──▶ [W-03 length_filter]    ──┐
                │                                │
                ├──▶ [W-04 repetition_filter] ──┤
                │                                │
                ├──▶ [W-05 hangul_filter]     ──┤
                │                                │
                ├──▶ [W-06 quality_filter]    ──┤
                │                                ├──▶ [W-10 quality_step] ──┐
                ├──▶ [W-07 structural_filter] ──┤                           │
                │                                │                           │
                ├──▶ [W-08 footnote_filter]   ──┤    [W-02 result] ────┐   │
                │                                │         │            │   │
                ├──▶ [W-09 pii_filter]        ──┘         │            │   │
                │                                          │            │   │
                └──▶ [W-11a dedup 공통] ──▶ [W-11b hash_compute ──▶ W-11c hash_lookup]
                                          │                                │
                                          └▶ [W-11d minhash_compute ──▶ W-11e minhash_lookup]
                                                                           │
                     [W-12 writer_step] ◀──────────────────────────────────┘
                          │
                     [W-14 boilerplate_util] (선택, 파이프라인 외부 전처리)
                                                               │
                     모두 ──▶ [W-13 config] ──▶ [W-15 run.py]
```

> **Dedup 설계 원칙**: 해시/시그니처 계산(CPU-bound)은 PARALLEL 스텝에서 병렬 처리하고,
> 집합 조회/삽입(상태 의존)만 SEQUENTIAL 스텝에서 순차 처리한다.
> 두 스텝의 처리 주기가 다를 때(PARALLEL이 빠르게 쌓고, SEQUENTIAL이 하나씩 소비)
> 큐의 backpressure가 자연스럽게 흐름을 제어한다.

---

## Phase 1 — 기초 (Foundation)

### - [x] W-01 `schema.py` — 데이터 스키마 정의

**파일**: `sample/pretrain-data-filter/schema.py`, `sample/pretrain-data-filter/tests/test_schema.py`
**의존**: 없음

- [x] 레퍼런스 탐색 (TypedDict, hanati-pretrain-data-filter schema.py)
- [x] 테스트 작성 (red)
- [x] 구현
- [x] 테스트 통과 (green)
- [x] ruff / mypy 통과

```python
class TaxonomyDict(TypedDict):
    """전체 taxonomy 스키마.
    필드: dataset_name, id, text, language, type, method,
          category, industrial_field, template, metadata(source,
          published_date, collected_date, token_len, quality_level, author)
    """

class SimpleTaxonomyDict(TypedDict):
    """최소 스키마 — id + text."""

def detect_schema(record: dict) -> Literal["taxonomy", "simple"]:
    """1. 'metadata' 키 존재 → 'taxonomy'
    2. 그 외 → 'simple'
    """
```

**테스트:**
- `@pytest.mark.parametrize` — taxonomy / simple 레코드 감지
- 필수 필드 누락 시 동작 확인

---

### - [x] W-02 `result.py` — FilterResult 결과 클래스

**파일**: `sample/pretrain-data-filter/result.py`, `sample/pretrain-data-filter/tests/test_result.py`
**의존**: 없음

- [x] 레퍼런스 탐색 (BaseResult 인터페이스, taxonomy-converter의 TaxonomyResult)
- [x] 테스트 작성 (red)
- [x] 구현
- [x] 테스트 통과 (green)
- [x] ruff / mypy 통과

```python
@dataclass
class FilterResult(BaseResult):
    """파이프라인 결과 집계.
    필드: kept, removed, removed_reasons(dict[str, int])
    1. merge(): kept/removed 합산, removed_reasons 키별 합산
    2. write(): output_dir에 stats.json 기록
    """
```

**테스트:**
- merge 시 카운터 합산 검증
- removed_reasons 딕셔너리 합산 검증
- write 출력 JSON 구조 검증

---

## Phase 2 — 필터 모듈 (Filters)

> 모든 필터 함수는 모듈 레벨에 정의한다 (spawn-mode pickle 호환).
> 각 필터는 `(text: str, **params) -> tuple[bool, str]` 시그니처를 따른다.
> 반환: `(True, "")` = 통과, `(False, "reason_code")` = 제거.
>
> **Scope 제한**: 검증용 샘플이므로 대표 3개(length, repetition, pii)만 구현한다.
> 나머지 필터는 구현하지 않는다 (하단 "구현하지 않는 항목" 참조).
> 필터 종류보다 구조(PARALLEL 스텝 + 필터 함수 조합)가 올바르게 작동하는지에 집중한다.

### - [x] W-03 `filters/length.py` — 길이 필터

**파일**: `sample/pretrain-data-filter/filters/length.py`, `sample/pretrain-data-filter/tests/test_length_filter.py`
**의존**: 없음

- [x] 레퍼런스 탐색 (hanati-pretrain-data-filter filters/length.py)
- [x] 테스트 작성 (red)
- [x] 구현
- [x] 테스트 통과 (green)
- [x] ruff / mypy 통과

```python
def filter_length(
    text: str,
    min_chars: int = 200,
    min_words: int = 50,
    max_words: int = 100_000,
    min_mean_word_length: float = 1.5,
) -> tuple[bool, str]:
    """1. strip → 빈 문자열이면 (False, 'empty')
    2. len(text) < min_chars → (False, 'too_few_chars')
    3. 공백 분리 단어 수 < min_words → (False, 'too_few_words')
    4. 단어 수 > max_words → (False, 'too_many_words')
    5. 평균 단어 길이 < min_mean_word_length → (False, 'mean_word_too_short')
    6. 모두 통과 → (True, '')
    """
```

**테스트:**
- `@pytest.mark.parametrize` — empty, short chars, few words, many words, short mean word, pass
- 경계값 테스트 (min_chars=200 정확히 200자)

---

### - [x] W-04 `filters/repetition.py` — 반복 필터

**파일**: `sample/pretrain-data-filter/filters/repetition.py`, `sample/pretrain-data-filter/tests/test_repetition_filter.py`
**의존**: 없음

- [x] 레퍼런스 탐색 (Gopher 논문 반복 휴리스틱, hanati-pretrain-data-filter filters/repetition.py)
- [x] 테스트 작성 (red)
- [x] 구현
- [x] 테스트 통과 (green)
- [x] ruff / mypy 통과

```python
def filter_repetition(
    text: str,
    max_dup_line_frac: float = 0.3,
    max_dup_para_frac: float = 0.3,
    max_top_2gram_frac: float = 0.20,
    max_top_3gram_frac: float = 0.18,
    max_top_4gram_frac: float = 0.16,
    max_dup_5gram_frac: float = 0.15,
    max_dup_6gram_frac: float = 0.14,
    max_dup_7gram_frac: float = 0.13,
) -> tuple[bool, str]:
    """1. 줄 단위 중복 문자 비율 → 'dup_line_frac'
    2. 문단(\n\n) 단위 중복 문자 비율 → 'dup_para_frac'
    3. top n-gram (2,3,4) 최빈 n-gram 문자 비율 → 'top_{n}gram_frac'
    4. dup n-gram (5,6,7) 2회 이상 등장 n-gram 문자 합 비율 → 'dup_{n}gram_frac'
       (이전 n-gram 비율 < 1e-9이면 상위 n-gram 스킵)
    5. 모두 통과 → (True, '')
    """
```

**테스트:**
- 동일 줄 반복 텍스트 → `dup_line_frac`
- 동일 문단 반복 → `dup_para_frac`
- 높은 빈도 n-gram 텍스트 → `top_2gram_frac`
- 정상 텍스트 → 통과

---

### - [x] W-09 `filters/pii.py` — 개인정보 필터

**파일**: `sample/pretrain-data-filter/filters/pii.py`, `sample/pretrain-data-filter/tests/test_pii_filter.py`
**의존**: 없음

- [x] 레퍼런스 탐색 (한국 전화번호/주민번호/사업자번호/이메일 정규식)
- [x] 테스트 작성 (red)
- [x] 구현
- [x] 테스트 통과 (green)
- [x] ruff / mypy 통과

```python
def filter_pii(text: str) -> tuple[bool, str]:
    """1. 전화번호 (0XX-XXXX-XXXX) → (False, 'pii_detected_phone')
    2. 주민등록번호 (6자리-7자리) → (False, 'pii_detected_rrn')
    3. 사업자등록번호 (3-2-5) → (False, 'pii_detected_brn')
    4. 이메일 → (False, 'pii_detected_email')
    5. 어느 것도 매치 안 됨 → (True, '')
    """
```

**테스트:**
- 각 PII 유형별 감지 확인
- PII 없는 정상 텍스트 → 통과
- 유사 패턴이지만 PII 아닌 경우 (예: 날짜 형식)

> **뉴스 보일러플레이트 필터 (`news_boilerplate.py`)는 scope에서 제외.**
> 30+ 정규식 패턴이 뉴스 크롤링 데이터에 특화되어 있어,
> 범용 pretrain 필터 파이프라인에서는 선택적으로 추가한다.
> 필요 시 Phase 5(확장)에서 별도 W-item으로 추가.
>
> **hangul_ratio, quality, structural, footnote 필터는 구현하지 않는다.**
> 파이프라인 동작 검증 목적으로 length + repetition + pii 3개만 구현.

---

## Phase 3 — 파이프라인 스텝 (Steps)

### - [x] W-10 `steps.py` — QualityFilterStep (PARALLEL)

**파일**: `sample/pretrain-data-filter/steps.py`, `sample/pretrain-data-filter/tests/test_steps.py`
**의존**: W-01, W-02, W-03, W-04, W-09

- [x] 레퍼런스 탐색 (BaseStep, PARALLEL 스텝 패턴, taxonomy-converter PreprocessStep)
- [x] 테스트 작성 (red)
- [x] 구현
- [x] 테스트 통과 (green)
- [x] ruff / mypy 통과

```python
class QualityFilterStep(BaseStep[FilterResult]):
    """PARALLEL 스텝 — 아이템별 품질 필터 적용.
    step_type = StepType.PARALLEL
    outputs = ("kept", "removed")

    1. config에서 활성화된 필터 목록 결정
    2. process(item):
       a. text = item[text_key]
       b. 각 필터 순서대로 적용 (short-circuit)
       c. 첫 실패 필터 → emit(item | {_removed_reason: reason}, 'removed')
       d. 모두 통과 → emit(item, 'kept')
    3. FilterResult(kept=1 or removed=1, removed_reasons={reason: 1})
    """
```

**테스트:**
- 정상 아이템 → 'kept'로 emit
- 짧은 텍스트 → 'removed'로 emit, reason='length/too_few_chars'
- PII 포함 → 'removed'로 emit, reason='pii/pii_detected_phone'
- 필터 비활성화 config → 해당 필터 스킵 확인

---

### - [x] W-11a `dedup/normalize.py` — Dedup 공통 정규화

**파일**: `sample/pretrain-data-filter/dedup/normalize.py`, `sample/pretrain-data-filter/tests/test_dedup_normalize.py`
**의존**: 없음

- [x] 레퍼런스 탐색 (unicodedata.normalize, hanati-pretrain-data-filter dedup)
- [x] 테스트 작성 (red)
- [x] 구현
- [x] 테스트 통과 (green)
- [x] ruff / mypy 통과

```python
def normalize_for_dedup(text: str) -> str:
    """1. NFKC 유니코드 정규화
    2. 소문자 변환
    3. strip
    4. 연속 공백 → 단일 공백 축소
    """
```

**테스트:**
- NFKC 정규화 확인 (전각→반각 등)
- 대소문자 통합
- 연속 공백/탭 축소
- 빈 문자열 / 공백만 있는 입력

---

### - [x] W-11b `steps.py` — HashComputeStep (PARALLEL)

**파일**: `sample/pretrain-data-filter/steps.py`, `sample/pretrain-data-filter/tests/test_steps.py`
**의존**: W-01, W-02, W-11a

- [x] 레퍼런스 탐색 (BaseStep PARALLEL 패턴, hashlib)
- [x] 테스트 작성 (red)
- [x] 구현
- [x] 테스트 통과 (green)
- [x] ruff / mypy 통과

```python
class HashComputeStep(BaseStep[FilterResult]):
    """PARALLEL 스텝 — 해시값 사전 계산.
    step_type = StepType.PARALLEL
    outputs = ("main",)

    CPU-bound 해시 계산을 병렬로 처리하여 후속 순차 스텝의 부하를 줄인다.
    설정: text_key='text', hash_algo='sha256'

    process(item):
      1. text = item[text_key]
      2. normalized = normalize_for_dedup(text)
      3. hash_value = hashlib.new(hash_algo, normalized.encode()).hexdigest()
      4. item['_dedup_hash'] = hash_value
      5. emit(item, 'main')
    """
```

**테스트:**
- 아이템에 `_dedup_hash` 필드 추가 확인
- 동일 텍스트 → 동일 해시
- 정규화 후 동일한 텍스트 → 동일 해시 (대소문자, 공백 차이)
- 다른 텍스트 → 다른 해시

---

### - [x] W-11c `steps.py` — HashLookupStep (SEQUENTIAL)

**파일**: `sample/pretrain-data-filter/steps.py`, `sample/pretrain-data-filter/tests/test_steps.py`
**의존**: W-01, W-02, W-11b

- [x] 레퍼런스 탐색 (BaseStep SEQUENTIAL 패턴, state 활용)
- [x] 테스트 작성 (red)
- [x] 구현
- [x] 테스트 통과 (green)
- [x] ruff / mypy 통과

```python
class HashLookupStep(BaseStep[FilterResult]):
    """SEQUENTIAL 스텝 — 해시 집합 조회/삽입 (상태 의존).
    step_type = StepType.SEQUENTIAL
    outputs = ("kept", "removed")

    상태: _seen: set[str] — 인스턴스 변수로 관리

    process(item):
      1. hash_value = item['_dedup_hash']
      2. hash_value in _seen
         → emit({...item, _removed_reason: 'dedup/exact'}, 'removed')
         → return FilterResult(removed=1, removed_reasons={'dedup/exact': 1})
      3. _seen.add(hash_value)
      4. item에서 '_dedup_hash' 제거 (downstream에 불필요)
      5. emit(item, 'kept')
      6. return FilterResult(kept=1)
    """
```

**테스트:**
- 동일 해시 2회 → 두 번째는 removed
- 서로 다른 해시 → 모두 kept
- `_dedup_hash` 필드가 kept 아이템에서 제거됨
- `@pytest.mark.timeout(15)` — 데드락 방지

---

### - [x] W-11d `steps.py` — MinHashComputeStep (PARALLEL)

**파일**: `sample/pretrain-data-filter/steps.py`, `sample/pretrain-data-filter/tests/test_steps.py`
**의존**: W-01, W-02, W-11a

- [x] 레퍼런스 탐색 (datasketch MinHash, n-gram 생성)
- [x] 테스트 작성 (red)
- [x] 구현
- [x] 테스트 통과 (green)
- [x] ruff / mypy 통과

```python
class MinHashComputeStep(BaseStep[FilterResult]):
    """PARALLEL 스텝 — MinHash 시그니처 사전 계산.
    step_type = StepType.PARALLEL
    outputs = ("main",)

    CPU-bound MinHash 생성을 병렬로 처리.
    설정: text_key='text', num_perm=128, ngram_size=5

    process(item):
      1. text = item[text_key]
      2. normalized = normalize_for_dedup(text)
      3. words = normalized.split()
      4. ngrams = [' '.join(words[i:i+ngram_size]) for i in range(len(words)-ngram_size+1)]
      5. mh = MinHash(num_perm=num_perm)
      6. for ng in ngrams: mh.update(ng.encode('utf-8'))
      7. item['_minhash'] = mh  (MinHash 객체는 picklable)
      8. emit(item, 'main')
    """
```

**테스트:**
- 아이템에 `_minhash` 필드 추가 확인
- 동일 텍스트 → 동일 시그니처
- 유사 텍스트 → 높은 Jaccard 유사도
- 단어 수 < ngram_size → 빈 n-gram 처리 (시그니처 생성은 가능)

---

### - [x] W-11e `steps.py` — MinHashLookupStep (SEQUENTIAL)

**파일**: `sample/pretrain-data-filter/steps.py`, `sample/pretrain-data-filter/tests/test_steps.py`
**의존**: W-01, W-02, W-11d

- [x] 레퍼런스 탐색 (datasketch MinHashLSH, state 활용)
- [x] 테스트 작성 (red)
- [x] 구현
- [x] 테스트 통과 (green)
- [x] ruff / mypy 통과

```python
class MinHashLookupStep(BaseStep[FilterResult]):
    """SEQUENTIAL 스텝 — LSH 인덱스 조회/삽입 (상태 의존).
    step_type = StepType.SEQUENTIAL
    outputs = ("kept", "removed")

    상태: _lsh: MinHashLSH — 인스턴스 변수로 관리
          _counter: int — 삽입 키 생성용
    설정: threshold=0.8, num_perm=128

    process(item):
      1. mh = item['_minhash']
      2. matches = _lsh.query(mh)
      3. matches 존재
         → emit({...item, _removed_reason: 'dedup/minhash'}, 'removed')
         → return FilterResult(removed=1, removed_reasons={'dedup/minhash': 1})
      4. _lsh.insert(str(_counter), mh); _counter += 1
      5. item에서 '_minhash' 제거 (downstream에 불필요, 비직렬화 대상)
      6. emit(item, 'kept')
      7. return FilterResult(kept=1)
    """
```

**테스트:**
- 동일 시그니처 2회 → 두 번째는 removed
- 유사 텍스트 (Jaccard > threshold) → removed
- 서로 다른 텍스트 → 모두 kept
- `_minhash` 필드가 kept 아이템에서 제거됨
- `@pytest.mark.timeout(15)` — 데드락 방지

---

### - [x] W-12 `steps.py` — WriterStep (SEQUENTIAL)

**파일**: `sample/pretrain-data-filter/steps.py` (동일 파일), `sample/pretrain-data-filter/tests/test_steps.py`
**의존**: W-01, W-02

- [x] 레퍼런스 탐색 (taxonomy-converter WriterStep, orjson/json JSONL 쓰기)
- [x] 테스트 작성 (red)
- [x] 구현
- [x] 테스트 통과 (green)
- [x] ruff / mypy 통과

```python
class WriterStep(BaseStep[FilterResult]):
    """SEQUENTIAL 스텝 — kept/removed JSONL 파일 기록.
    step_type = StepType.SEQUENTIAL
    outputs = ()  # 터미널

    1. process(item):
       a. '_removed_reason' 키 존재 → removed.jsonl에 기록
       b. 없음 → kept.jsonl에 기록
    2. close(): 파일 핸들 닫기
    """
```

**테스트:**
- kept 아이템 → kept.jsonl에 기록 확인
- removed 아이템 → removed.jsonl에 기록 확인 (reason 포함)
- close() 후 파일 정상 닫힘

---

## Phase 4 — 통합 (Integration)

### - [x] W-13 `pipeline_config.yaml` — 파이프라인 설정

**파일**: `sample/pretrain-data-filter/pipeline_config.yaml`, `sample/pretrain-data-filter/tests/test_config.py`
**의존**: W-10, W-11b~W-11e, W-12

- [x] 레퍼런스 탐색 (PipelineConfig 스키마, taxonomy-converter pipeline_config.yaml)
- [x] 테스트 작성 (red) — config 로드 및 유효성 검증
- [x] 구현
- [x] 테스트 통과 (green)
- [x] ruff / mypy 통과

```yaml
# 파이프라인 DAG 구조
# Dedup은 계산(PARALLEL) → 판정(SEQUENTIAL) 2단계로 분리
pipeline:
  - type: quality_filter
    text_key: text
    filters:
      length: { min_chars: 200, min_words: 50 }
      repetition: { max_dup_line_frac: 0.3 }
      pii: {}
      # 미구현 (검증용 샘플이므로 생략):
      # hangul_ratio, quality, structural, footnote, news_boilerplate
    outputs:
      kept: hash_compute
      removed: writer

  # --- Exact Dedup: 계산(병렬) → 판정(순차) ---
  - type: hash_compute          # PARALLEL — SHA-256 해시 계산
    text_key: text
    hash_algo: sha256
    outputs:
      main: hash_lookup

  - type: hash_lookup            # SEQUENTIAL — 해시 집합 조회/삽입
    outputs:
      kept: minhash_compute
      removed: writer

  # --- MinHash Dedup: 계산(병렬) → 판정(순차) ---
  - type: minhash_compute        # PARALLEL — MinHash 시그니처 계산
    text_key: text
    num_perm: 128
    ngram_size: 5
    outputs:
      main: minhash_lookup

  - type: minhash_lookup         # SEQUENTIAL — LSH 인덱스 조회/삽입
    threshold: 0.8
    num_perm: 128
    outputs:
      kept: writer
      removed: writer

  - type: writer
    # 터미널 스텝 — outputs 없음

execution:
  workers: 4
  queue_size: 200
```

**테스트:**
- YAML 로드 → PipelineConfig 파싱 성공
- 필터 비활성화 시 정상 동작
- outputs 참조 무결성 확인

---

### - [x] W-14 `utils/boilerplate.py` — 보일러플레이트 전처리 유틸 (선택)

**파일**: `sample/pretrain-data-filter/utils/boilerplate.py`, `sample/pretrain-data-filter/tests/test_boilerplate.py`
**의존**: W-01

- [x] 레퍼런스 탐색 (hanati-pretrain-data-filter dedup/boilerplate.py)
- [x] 테스트 작성 (red)
- [x] 구현
- [x] 테스트 통과 (green)
- [x] ruff / mypy 통과

> **설계 배경**: 보일러플레이트 제거는 2-pass (전체 코퍼스 줄 빈도 집계 → 제거)
> 구조로, task-pipeliner의 스트리밍 파이프라인에 직접 맞지 않는다.
> 파이프라인 실행 전 전처리 스크립트로 분리한다.

```python
class BoilerplateDetector:
    """2-pass 보일러플레이트 줄 제거.
    설정: min_line_freq=100, min_line_length=10, max_blank_lines=1

    1. build_freq(docs: Iterable[str]):
       각 문서의 strip된 줄 빈도 집계 (Counter)
    2. clean(text: str) -> str | None:
       빈도 ≥ min_line_freq 줄 제거, 연속 빈줄 제한,
       결과 빈 문자열이면 None 반환
    """
```

**테스트:**
- 100회 반복 줄 → build_freq 후 clean에서 제거
- 빈도 미만 줄 → 유지
- 제거 후 빈 문서 → None 반환
- 연속 빈줄 제한 동작

---

### - [x] W-15 `run.py` — CLI 엔트리포인트

**파일**: `sample/pretrain-data-filter/run.py`, `sample/pretrain-data-filter/tests/test_run.py`
**의존**: W-10, W-11b~W-11e, W-12, W-13

- [x] 레퍼런스 탐색 (taxonomy-converter run.py, Pipeline 클래스 사용법)
- [x] 테스트 작성 (red)
- [x] 구현
- [x] 테스트 통과 (green)
- [x] ruff / mypy 통과

```python
def main() -> None:
    """CLI 엔트리포인트.
    인자: --config, --input (1+), --output, --workers, --no-minhash
    1. argparse로 인자 파싱
    2. Pipeline 인스턴스 생성
    3. register_all({
         'quality_filter': QualityFilterStep,
         'hash_compute': HashComputeStep,
         'hash_lookup': HashLookupStep,
         'minhash_compute': MinHashComputeStep,
         'minhash_lookup': MinHashLookupStep,
         'writer': WriterStep,
       })
    4. pipeline.run(config, inputs, output_dir)
    """
```

**테스트:**
- 더미 JSONL 입력 → 파이프라인 실행 → kept/removed 파일 생성 확인
- `--no-minhash` 시 MinHashDedupStep 비활성화 확인

---

### - [x] W-16 실제 데이터 실행 — taxonomy-converter 출력으로 E2E 검증

**의존**: W-15

- [x] `sample/taxonomy-converter/output/kept.jsonl`을 입력으로 파이프라인 실행
- [x] `sample/pretrain-data-filter/output/` 에 kept.jsonl, removed.jsonl 생성 확인
- [x] 결과 확인: kept/removed 건수, removed_reason 분포가 합리적인지 검토

```bash
.venv/Scripts/python sample/pretrain-data-filter/run.py \
  --config sample/pretrain-data-filter/pipeline_config.yaml \
  --input  sample/taxonomy-converter/output/kept.jsonl \
  --output sample/pretrain-data-filter/output
```

> 이 단계의 목적은 단위 테스트가 아니라, 실제 데이터가 파이프라인 전체를
> 정상적으로 통과하는지 눈으로 확인하는 것이다.

---

## 구현하지 않는 항목

> 이 프로젝트는 task-pipeliner 프레임워크의 검증용 샘플이므로,
> 아래 항목은 구현하지 않는다.

- `filters/hangul_ratio.py` — 한글 비율 필터
- `filters/quality.py` — 품질 필터 (기호, 불릿, 말줄임, 알파 비율, URL, 불용어)
- `filters/structural.py` — 구조 마커 필터
- `filters/footnote.py` — 각주 밀도 필터
- `filters/news_boilerplate.py` — 뉴스 보일러플레이트 필터 (30+ 정규식, 뉴스 특화)
- FieldExclusionStep — 필드 값 기반 제외
- 통계 샘플링 — removed_per_reason 샘플 출력
