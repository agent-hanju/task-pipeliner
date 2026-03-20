"""Test fixtures: dummy news items for pipeline step testing."""

from __future__ import annotations

# Normal Naver news article with all expected fields
SAMPLE_NAVER_ITEM: dict = {
    "index": 1,
    "title": "한국 경제 성장률 전망",
    "text": (
        "한국 경제 성장률이 올해 3%를 기록할 것으로 전망된다.\n"
        "한국은행은 올해 경제 성장률을 3.0%로 상향 조정했다.\n"
        "이는 수출 호조와 내수 회복에 따른 것이다.\n"
        "전문가들은 하반기에도 이러한 추세가 이어질 것으로 보고 있다.\n"
        "다만 글로벌 경기 불확실성은 여전히 리스크 요인이다.\n"
        "정부는 경기 부양을 위해 추가 재정 지출을 검토하고 있다."
    ),
    "date": "20240115",
    "_date_prefix": "20240115",
}

# Article too short to pass preprocessing validation
SAMPLE_SHORT_ITEM: dict = {
    "index": 2,
    "title": "짧은 기사",
    "text": "짧은 내용.",
    "date": "20240115",
    "_date_prefix": "20240115",
}

# Article with copyright, URL, byline lines that should be filtered
SAMPLE_DIRTY_ITEM: dict = {
    "index": 3,
    "title": "더러운 기사 제목",
    "text": (
        "(서울=뉴스1) 홍길동 기자 = 서울시가 새로운 정책을 발표했다.\n"
        "이 정책은 시민들의 삶의 질을 높이기 위한 것이다.\n"
        "서울시는 올해 안에 시행할 계획이라고 밝혔다.\n"
        "주요 내용으로는 교통 개선과 환경 보호가 포함된다.\n"
        "시민들의 반응은 대체로 긍정적이다.\n"
        "전문가들도 이번 정책을 환영하고 있다.\n"
        "<ⓒ뉴스1 무단전재 금지>\n"
        "홍길동 기자\n"
        "reporter@news1.com\n"
        "▶ 관련기사 더보기"
    ),
    "date": "20240201",
    "_date_prefix": "20240201",
}

# Two items with same id and same text (exact duplicates)
SAMPLE_DUPLICATE_ITEMS: list[dict] = [
    {
        "id": "HanaTI-NaverNews-20240115-1",
        "text": "동일한 뉴스 본문 내용입니다.",
    },
    {
        "id": "HanaTI-NaverNews-20240115-1",
        "text": "동일한 뉴스 본문 내용입니다.",
    },
]

# Two items with same id but different text
SAMPLE_SAME_ID_DIFF_TEXT: list[dict] = [
    {
        "id": "HanaTI-NaverNews-20240115-1",
        "text": "첫 번째 버전의 뉴스 본문입니다.",
    },
    {
        "id": "HanaTI-NaverNews-20240115-1",
        "text": "두 번째 버전의 뉴스 본문으로 다른 내용을 담고 있습니다.",
    },
]
