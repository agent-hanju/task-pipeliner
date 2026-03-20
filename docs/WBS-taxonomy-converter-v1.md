# WBS: Naver News Taxonomy Converter (task-pipeliner 샘플)

> task-pipeliner 프레임워크를 활용해 네이버 뉴스 기사를 Taxonomy 스키마로 변환하는 파이프라인.
> 입력: 파일/디렉토리 경로 여러 개 (JSON 배열 또는 JSONL). 출력: JSONL (Taxonomy 스키마).
> 각 작업 단위 완료 시 해당 체크박스를 체크한다. 체크 기준: **구현 + 테스트 전부 통과**.

---

## 의존 관계 맵

```
[taxonomy] ──┬──▶ [text_rules] ──▶ [naver_rules] ──▶ [steps] ──▶ [pipeline_config]
             └──▶ [utils]     ──┘                      ▲
                                                       │
[loader] ──────────────────────────────────────────────┘
                                                       │
                                            [dummy_data] ──┘
```

- `taxonomy`, `utils`, `loader` 는 서로 독립 → Phase 1에서 병렬 작업 가능
- `text_rules`는 Phase 1 완료 후 착수
- `naver_rules`는 `text_rules` 완료 후 착수
- `steps`는 Phase 2 완료 후 착수
- `pipeline_config`은 `steps` 완료 후 착수

---

## Phase 1 — 데이터 모델 & 기반 (병렬 가능)

> 이후 모든 작업의 기반. 완료 후 Phase 2 착수 가능.

### - [x] W-01 `taxonomy.py` — Taxonomy 스키마 + Result 클래스

**파일**: `sample/taxonomy-converter/taxonomy.py`, `sample/taxonomy-converter/tests/test_taxonomy.py`

- [x] 레퍼런스 탐색 (TypedDict, task_pipeliner.BaseResult)
- [x] 테스트 작성 (red)
- [x] 구현
- [x] 테스트 통과 (green)
- [x] ruff / mypy 통과

```python
class TaxonomyMetadata(TypedDict):
    source: str                        # e.g., "HanaTI/NaverNewsEconomy"
    published_date: str | None         # ISO 8601 (YYYY-MM-DD) or None
    collected_date: str                # ISO 8601
    token_len: int | None
    quality_level: str | None          # "gold" | "silver" | "bronze" | None
    author: str | None

class TaxonomyDict(TypedDict):
    dataset_name: str                  # "naver_econ_news"
    id: str                            # "HanaTI-NaverNews-{date}-{index}"
    text: str
    language: str                      # "korean"
    type: str                          # "public"
    method: str                        # "document_parsing"
    category: str                      # "hass"
    industrial_field: str              # "finance"
    template: str                      # "article"
    metadata: TaxonomyMetadata

class SimpleTaxonomyDict(TypedDict):
    id: str
    text: str

class TaxonomyResult(BaseResult):
    """변환 결과를 추적하는 result. success/skipped/error 건수 집계."""
    success: int = 0
    skipped: int = 0
    errored: int = 0
    def merge(self, other) -> Self
    def write(self, output_dir) -> None   # count_summary.json 출력
```

테스트:
- `TaxonomyResult.merge()` — 두 결과 합산 정확성
- `TaxonomyResult.write()` — JSON 파일 생성 + 내용 검증

---

### - [x] W-02 `utils.py` — 공통 유틸리티 함수

**파일**: `sample/taxonomy-converter/utils.py`, `sample/taxonomy-converter/tests/test_utils.py`

- [x] 레퍼런스 탐색 (difflib.SequenceMatcher, datetime)
- [x] 테스트 작성 (red)
- [x] 구현
- [x] 테스트 통과 (green)
- [x] ruff / mypy 통과

```python
def merge_title(title: str, content: str, threshold: float = 0.8) -> str
    """제목과 본문 병합. 본문 시작이 제목과 threshold 이상 유사하면 제목 생략."""
    # 1. title이나 content가 빈 문자열이면 있는 쪽만 반환
    # 2. content[:len(title)]과 title의 SequenceMatcher ratio 계산
    # 3. ratio >= threshold → content만 반환
    # 4. 그 외 → f"{title}\n\n{content}"

def parse_date(raw: str | None) -> str | None
    """YYYYMMDD (6~8자리) → YYYY-MM-DD ISO 형식 변환. 파싱 실패 시 None."""

def make_metadata(source: str, published_date: str | None, **kwargs) -> TaxonomyMetadata
    """기본값이 채워진 TaxonomyMetadata 생성."""
```

테스트 (`@pytest.mark.parametrize`):
- `merge_title("제목", "제목으로 시작하는 본문")` → 제목 생략
- `merge_title("제목", "다른 시작의 본문")` → `"제목\n\n다른 시작의 본문"`
- `merge_title("", "본문")` → `"본문"`, `merge_title("제목", "")` → `"제목"`
- `parse_date("20240115")` → `"2024-01-15"`
- `parse_date("2024011")` → 7자리 처리, `parse_date(None)` → `None`
- `parse_date("abc")` → `None`
- `make_metadata` — 기본값 확인, kwargs override 확인

---

### - [x] W-03 `loader.py` — 다중 포맷 파일 로더

**파일**: `sample/taxonomy-converter/loader.py`, `sample/taxonomy-converter/tests/test_loader.py`

- [x] 레퍼런스 탐색 (orjson, pathlib.glob, BOM 처리)
- [x] 테스트 작성 (red)
- [x] 구현
- [x] 테스트 통과 (green)
- [x] ruff / mypy 통과

```python
def detect_format(path: Path) -> Literal["json", "jsonl"]
    """파일 선두 64KB를 읽어 포맷 판별.
    1. 첫 바이트(BOM 제거 후)가 '[' → json (배열)
    2. 첫 줄이 완전한 JSON 객체 → jsonl
    3. 그 외 → json"""

def extract_date_prefix(path: Path) -> str
    """파일명 stem에서 선두 8자리 숫자를 추출.
    예: '20240115_naver.json' → '20240115'
    숫자가 없거나 부족하면 '00000000' 반환."""

def load_json_file(path: Path) -> list[dict]
    """JSON 파일 로드.
    1. UTF-8 BOM 제거
    2. orjson.loads 파싱
    3. dict이고 'data' 키가 있으면 data 값 추출
    4. list가 아니면 [parsed]로 감싸기"""

def load_jsonl_file(path: Path) -> Generator[dict, None, None]
    """JSONL 파일 스트리밍 로드.
    1. 한 줄씩 읽기
    2. 빈 줄 건너뛰기
    3. 각 줄 orjson.loads 파싱 후 yield"""

def resolve_paths(paths: list[Path]) -> list[Path]
    """경로 목록 확장.
    1. 디렉토리 → 내부 *.json + *.jsonl 파일 수집 (정렬)
    2. 파일 → 그대로 유지"""

def load_items(paths: list[Path]) -> Generator[dict, None, None]
    """최종 로더. 여러 경로에서 아이템을 순차적으로 yield.
    1. resolve_paths로 경로 확장
    2. 각 파일에 대해:
       a. date_prefix = extract_date_prefix(file)
       b. format = detect_format(file)
       c. json → load_json_file, jsonl → load_jsonl_file
       d. 각 아이템에 '_date_prefix' 필드 주입
       e. yield item"""
```

테스트 (`@pytest.mark.parametrize`, `tmp_path` 활용):
- `detect_format`: JSON 배열 파일 → `"json"`, JSONL 파일 → `"jsonl"`
- `extract_date_prefix`: `"20240115_news.json"` → `"20240115"`, `"nodate.json"` → `"00000000"`
- `load_json_file`: `[{...}, {...}]` → 2건, `{"data": [...]}` → wrapper 해제
- `load_jsonl_file`: 3줄 JSONL → 3건, 빈 줄 무시
- `resolve_paths`: 디렉토리 경로 → 내부 파일 목록, 파일 경로 → 그대로
- `load_items`: 혼합 포맷 디렉토리 → 전체 아이템 yield + `_date_prefix` 주입 확인
- BOM 포함 JSON 파일 정상 로드

---

## Phase 2 — 텍스트 전처리 규칙 (Phase 1 완료 후)

### - [x] W-04 `text_rules.py` — 범용 텍스트 전처리 함수

**파일**: `sample/taxonomy-converter/text_rules.py`, `sample/taxonomy-converter/tests/test_text_rules.py`
**의존**: 없음 (순수 함수)

- [x] 레퍼런스 탐색 (re 모듈, 유니코드 공백 문자)
- [x] 테스트 작성 (red)
- [x] 구현
- [x] 테스트 통과 (green)
- [x] ruff / mypy 통과

```python
# === 정규화 함수 ===

def normalize_newlines(text: str) -> str
    """줄바꿈 정규화.
    \r\n → \n, \r → \n, \u2028 → \n, \u2029 → \n, \v → ' ', \f → ' '"""

def normalize_spaces(text: str) -> str
    """연속 공백 정규화. [ \t\xa0\u2003\u3000]+ → 단일 공백"""

def strip_lines(text: str) -> str
    """각 라인의 앞뒤 공백 제거 (라인 구조 유지)."""

def collapse_blank_lines(text: str) -> str
    """3개 이상 연속 빈 줄 → 2개로 축소. \n{3,} → \n\n"""

def normalize_text(text: str) -> str
    """위 4개 함수를 순서대로 적용하는 통합 정규화."""

# === 문서 레벨 변환 함수 ===

def truncate_after_pattern(text: str, pattern: re.Pattern) -> str
    """패턴 첫 매칭 위치 이후 전체 삭제."""

def remove_pattern(text: str, pattern: re.Pattern) -> str
    """패턴 매칭 부분을 빈 문자열로 치환."""

# === 라인 레벨 판별/변환 함수 ===

def is_valid_sentence_line(line: str) -> bool
    """유효한 문장 라인 판별.
    1) 마침표('.')로 끝나야 함
    2) 기호 문자(※■□▪▫▶▷◀◁►◄▸◂●○◆◇△▲▽▼→←↑↓↗↘↙↖★☆·•ㆍ・☞☛◉◎)로 시작하면 안 됨"""

def strip_leading_non_sentence_lines(text: str) -> str
    """문서 앞쪽에서 유효 문장이 아닌 라인들을 제거."""

def strip_trailing_non_sentence_lines(text: str) -> str
    """문서 뒤쪽에서 유효 문장이 아닌 라인들을 제거."""

def has_list_bullet_start(line: str) -> bool
    """기호 불릿(※■▶●◆△▲→☞·•ㆍ)으로 시작하는 라인인지 판별."""

def strip_leading_symbolic_list(text: str) -> str
    """문서 앞쪽의 불릿 리스트 블록 제거.
    알고리즘: 비어있지 않은 라인을 순회하며 불릿 시작 라인 추적.
    연속 3개 이상 깨끗한 라인이 나오면 마지막 불릿 라인까지 제거."""

def strip_trailing_symbolic_list(text: str) -> str
    """문서 뒤쪽의 불릿 리스트 블록 제거. (strip_leading의 역순)"""

def remove_bracket_line_start(line: str) -> str
    """라인 시작의 대괄호 태그 제거. [서울경제] 본문 → 본문"""

def remove_bracket_line_end(line: str) -> str
    """라인 끝의 대괄호 태그 제거. 본문 [단독] → 본문"""

def remove_kv_credit(line: str) -> str
    """(지역=통신사) 형태의 출처 표기 제거. (서울=뉴스1) 본문 → 본문"""

def remove_reporter_sign(line: str) -> str
    """기자 서명 제거. 이름 기자 = → (빈 문자열)"""

def remove_repeated_inline_bullets(line: str) -> str
    """라인 내 2회 이상 등장하는 불릿 기호(◆△▲◇●) 제거."""
```

테스트 (`@pytest.mark.parametrize`):
- `normalize_newlines`: `\r\n` → `\n`, `\u2028` → `\n`, `\v` → ` ` 등
- `normalize_spaces`: `"a  b\t\tc"` → `"a b c"`, NBSP 포함 케이스
- `strip_lines`: `"  hello  \n  world  "` → `"hello\nworld"`
- `collapse_blank_lines`: 3줄 공백 → 2줄, 2줄 → 유지
- `truncate_after_pattern`: 매칭 이전까지만 유지, 미매칭 시 원본 반환
- `is_valid_sentence_line`: `"정상 문장."` → True, `"▶ 기호"` → False, `"마침표 없음"` → False
- `strip_leading_non_sentence_lines` / `strip_trailing_non_sentence_lines`: 비문장 라인 제거 확인
- `strip_leading_symbolic_list` / `strip_trailing_symbolic_list`: 불릿 블록 제거 확인
- `remove_bracket_line_start`: `"[뉴스1] 본문"` → `"본문"`
- `remove_kv_credit`: `"(서울=뉴스1) 본문"` → `"본문"`
- `remove_reporter_sign`: `"홍길동 기자 = "` → `""`
- `remove_repeated_inline_bullets`: `"◆항목1◆항목2"` → `"항목1항목2"`

---

### - [x] W-05 `naver_rules.py` — 네이버 뉴스 전용 규칙

**파일**: `sample/taxonomy-converter/naver_rules.py`, `sample/taxonomy-converter/tests/test_naver_rules.py`
**의존**: W-04 (`text_rules.py`의 범용 함수 활용)

- [x] 레퍼런스 탐색 (네이버 뉴스 본문 패턴)
- [x] 테스트 작성 (red)
- [x] 구현
- [x] 테스트 통과 (green)
- [x] ruff / mypy 통과

```python
# === 문서 레벨 변환 (truncate/remove) ===

BLOCK_TRIGGER_PATTERNS: list[re.Pattern]
    # [관련기사], ▶ 관련기사 ◀, [관련 뉴스], [사진 영상 제보받습니다],
    # | 패밀리사이트, 이데일리TV\n＞, IT는 아이뉴스24

PHOTO_CAPTION_BLOCK_RE: re.Pattern
    # 3줄 이상 공백 + ]로 끝나는 텍스트 (사진 캡션 블록)

COPYRIGHT_CE_MULTI_RE: re.Pattern
    # <ⓒ아시아경제 무단\n전재 배포금지> 등 2줄 저작권 표기

DOT_NAV_BLOCK_RE: re.Pattern
    # ㆍ 구분자 반복 뉴스 내비게이션 블록

def apply_naver_transforms(text: str) -> str
    """위 패턴들을 순서대로 적용.
    1. block trigger 패턴에서 truncate
    2. photo caption block 제거
    3. copyright multi-line 제거
    4. dot nav block 제거
    5. bracket line start/end 제거 (text_rules 함수 사용)
    6. leading/trailing non-sentence 라인 제거
    7. leading/trailing symbolic list 제거"""

# === 라인 필터 (True면 해당 라인 제거) ===

NAVER_LINE_FILTER_PREDICATES: list[Callable[[str], bool]]
    # 기호 시작: ▲☞▶©ⓒ◆
    # 사진 관련: 사진은~, /사진=~, (왼쪽, (오른쪽, (가운데
    # CTA/프로모: [모바일], [핫*영상], ■핫뉴스/핫이슈, 네이버에서~
    # 구독/다운로드: 구독~, ~다운받기, <~앱~>, <~다운~>
    # SNS 링크: [트위터], [페이스북], [카카오스토리], [유튜브] 등
    # 미디어 서명: 디지털뉴스팀, ~디지털타임즈 등
    # 기타: #숫자, ↘ 포함, >>> 끝, - 시작+100자 미만

SHARED_LINE_FILTER_PREDICATES: list[Callable[[str], bool]]
    # 저작권: <ⓒ, <저작권자, ※저작권자, - Copyrights, ⓒ 포함
    # URL/연락처: http, 기사문의, 기자문의, 상담료, 전화번호
    # 바이라인: 2~5자 한글 + 기자, 이메일 끝
    # 기타: 자동생성 알고리즘, /, 방송사 제보, 기자/연합뉴스/금지/구독/가기 끝
    # 형식: 대괄호만 라인, ▷ 끝, ● 시작

def should_filter_line(line: str) -> bool
    """stripped 라인에 대해 네이버 + 공통 필터 적용. True면 제거 대상."""

# === 라인 인라인 변환 ===

def apply_naver_line_inline(line: str) -> str
    """라인 내 텍스트 변환.
    1. KV 출처 제거: (서울=뉴스1) → ''
    2. 기자 서명 제거: 이름 기자 = → ''
    3. 반복 불릿 제거: ◆◆ → ''"""
```

테스트 (`@pytest.mark.parametrize`):
- `apply_naver_transforms`: block trigger 패턴 이후 삭제, photo caption 제거 확인
- `should_filter_line`: 기호 시작, 저작권, URL, 바이라인 등 각 패턴별 True/False
- `apply_naver_line_inline`: KV 출처/기자 서명/반복 불릿 제거 확인
- 정상 뉴스 본문 라인은 필터링되지 않음 확인

---

## Phase 3 — Step 구현 (Phase 2 완료 후)

### - [x] W-06 `steps.py` — 파이프라인 Step 클래스

**파일**: `sample/taxonomy-converter/steps.py`, `sample/taxonomy-converter/tests/test_steps.py`
**의존**: W-01, W-02, W-04, W-05

- [x] 레퍼런스 탐색 (task_pipeliner.BaseStep, BaseAggStep, StepType)
- [x] 테스트 작성 (red)
- [x] 구현
- [x] 테스트 통과 (green)
- [x] ruff / mypy 통과

```python
class PreprocessStep(BaseStep[TaxonomyResult]):
    """텍스트 전처리 step.
    step_type = PARALLEL

    process(item, state, emit):
        1. raw_text = item.get("text", "")
        2. 정규화: normalize_text(raw_text)
        3. 문서 변환: apply_naver_transforms(text)
        4. 라인 필터: 각 라인에 should_filter_line → True면 제거
        5. 라인 인라인: 각 라인에 apply_naver_line_inline 적용
        6. 최종 정규화: normalize_text(text).strip()
        7. 검증: 비어있지 않은 라인 < 2 또는 총 글자수 < 100 → skip (emit 안 함)
        8. item에 cleaned text 반영 후 emit
        9. return TaxonomyResult(success=1) 또는 TaxonomyResult(skipped=1)
    """

class ConvertStep(BaseStep[TaxonomyResult]):
    """Taxonomy 스키마 변환 step.
    step_type = PARALLEL

    process(item, state, emit):
        1. index = str(item["index"])
        2. date_prefix = item.get("_date_prefix", "00000000")
        3. title = (item.get("title") or "").strip()
        4. cleaned = item["text"]  # PreprocessStep에서 정제됨
        5. text = merge_title(title, cleaned)
        6. published_date = parse_date(item.get("date"))
        7. metadata = make_metadata("HanaTI/NaverNewsEconomy", published_date)
        8. taxonomy_dict 조립 (dataset_name, id, text, language, ...)
        9. emit(taxonomy_dict)
        10. return TaxonomyResult(success=1)
        예외 발생 시: return TaxonomyResult(errored=1)
    """

class DeduplicateStep(BaseStep[TaxonomyResult]):
    """중복 제거 step.
    step_type = SEQUENTIAL  (상태 유지 필요)

    내부 상태:
        seen_ids: dict[str, int]         # doc_id → text_hash
        id_counters: dict[str, int]      # doc_id → suffix counter

    process(item, state, emit):
        1. doc_id = item["id"]
        2. text_hash = hash(item["text"])
        3. if doc_id not in seen_ids:
             seen_ids[doc_id] = text_hash → emit(item), success
        4. if seen_ids[doc_id] == text_hash:
             → 중복, emit 안 함, skipped
        5. if seen_ids[doc_id] != text_hash:
             id_counters[doc_id] += 1
             item["id"] = f"{doc_id}-{id_counters[doc_id]}"
             → emit(item), success
    """
```

테스트 (`@pytest.mark.timeout(15)`):
- **PreprocessStep**:
  - 정상 뉴스 텍스트 → 정제 후 emit 확인
  - 저작권/URL 라인 포함 텍스트 → 해당 라인 제거 확인
  - 너무 짧은 텍스트 → skip (emit 안 됨)
  - `TaxonomyResult.success/skipped` 정확성
- **ConvertStep**:
  - 정상 item → TaxonomyDict 구조 확인 (필수 필드 전부)
  - ID 형식: `HanaTI-NaverNews-{date}-{index}`
  - 제목-본문 유사 시 제목 생략 확인
  - 필수 필드 누락 → errored
- **DeduplicateStep**:
  - 고유 item → 모두 emit
  - 동일 id + 동일 text → 두 번째부터 skip
  - 동일 id + 다른 text → suffix 붙은 id로 emit
  - `@pytest.mark.parametrize("n", [0, 1, 5])` — 중복 건수별 정확성

---

### - [x] W-07 `dummy_data.py` — 테스트용 더미 데이터

**파일**: `sample/taxonomy-converter/tests/dummy_data.py`
**의존**: W-01

- [x] 구현
- [x] ruff 통과

```python
# 모듈 레벨 정의 (pickle 호환)

SAMPLE_NAVER_ITEM: dict
    # 정상 네이버 뉴스 기사 (index, title, text, date, _date_prefix 포함)

SAMPLE_SHORT_ITEM: dict
    # 너무 짧아서 skip 대상인 기사

SAMPLE_DIRTY_ITEM: dict
    # 저작권, URL, 바이라인 등 제거 대상 라인이 포함된 기사

SAMPLE_DUPLICATE_ITEMS: list[dict]
    # 동일 id + 동일 text인 2개 아이템

SAMPLE_SAME_ID_DIFF_TEXT: list[dict]
    # 동일 id + 다른 text인 2개 아이템
```

---

## Phase 4 — 통합 (Phase 3 완료 후)

### - [x] W-08 파이프라인 설정 + 통합 테스트

**파일**: `sample/taxonomy-converter/pipeline_config.yaml`, `sample/taxonomy-converter/run.py`, `sample/taxonomy-converter/tests/test_integration.py`
**의존**: W-03, W-06, W-07

- [x] 레퍼런스 탐색 (task_pipeliner.Pipeline, PipelineConfig YAML 형식)
- [x] 테스트 작성 (red)
- [x] 구현
- [x] 테스트 통과 (green)
- [x] ruff / mypy 통과

```yaml
# pipeline_config.yaml
pipeline:
  - type: preprocess
  - type: convert
  - type: deduplicate

execution:
  workers: 4
  queue_size: 200
  chunk_size: 100
```

```python
# run.py
from pathlib import Path
from task_pipeliner import Pipeline
from .loader import load_items
from .steps import PreprocessStep, ConvertStep, DeduplicateStep

def main(input_paths: list[Path], output_dir: Path, config_path: Path) -> None:
    """네이버 뉴스 변환 파이프라인 실행.
    1. loader.load_items(input_paths)로 입력 파일/디렉토리에서 아이템 로드
       - JSON 배열 / JSONL 자동 감지
       - 파일명에서 _date_prefix 추출 → 각 아이템에 주입
    2. Pipeline에 step 등록 (preprocess → convert → deduplicate)
    3. load_items의 Generator를 파이프라인 입력으로 전달
    4. 결과 JSONL을 output_dir에 출력"""

    pipeline = Pipeline()
    pipeline.register("preprocess", PreprocessStep)
    pipeline.register("convert", ConvertStep)
    pipeline.register("deduplicate", DeduplicateStep)
    # load_items를 Pipeline에 전달하는 방식은 구현 시 확정
    # (JsonlReader 대신 커스텀 Generator를 engine에 전달)
    pipeline.run(config=config_path, inputs=input_paths, output_dir=output_dir)
```

테스트:
- end-to-end: JSON/JSONL 입력 파일 → 파이프라인 실행 → `kept.jsonl` 존재 + 유효 JSON
- 정상 기사 N건 → 출력 N건, 각 TaxonomyDict 필수 필드 존재
- 중복 포함 입력 → 중복 제거 후 건수 일치
- 전처리 skip 대상 포함 → `removed.jsonl`에 기록
- `stats.json` 존재 + 각 step 카운터 정확성
- 디렉토리 경로 입력 → 내부 파일 전부 처리
- JSON 배열 + JSONL 혼합 입력 → 모두 정상 처리

---

## 병렬 작업 가능 구간 요약

| Phase | 작업 단위 | 병렬 가능 여부 |
|---|---|---|
| 1 | W-01, W-02, W-03 | **3개 동시 병렬** |
| 2 | W-04 → W-05 | 순차 |
| 3 | W-06, W-07 | W-07은 독립, W-06은 Phase 2 완료 후 |
| 4 | W-08 | Phase 3 완료 후 순차 |
