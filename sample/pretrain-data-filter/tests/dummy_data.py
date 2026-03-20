"""Dummy test data for pretrain-data-filter tests."""

from __future__ import annotations

# Normal Korean text (passes all filters)
# 60+ words, 200+ chars, diverse vocabulary, no PII
NORMAL_TEXT = (
    "한국은행은 기준금리를 동결했다고 발표했다. "
    "이번 결정은 최근 경제 상황과 물가 동향을 종합적으로 고려한 것이다. "
    "전문가들은 하반기에 금리 인하 가능성이 있다고 분석하고 있다. "
    "소비자물가 상승률은 전년 대비 안정세를 보이고 있으며 "
    "수출은 반도체를 중심으로 회복세를 나타내고 있다. "
    "정부는 경기 부양을 위해 추가적인 재정 정책을 검토하고 있다. "
    "국내 주식시장은 외국인 투자자들의 매수세에 힘입어 상승세를 이어가고 있다. "
    "부동산 시장은 수도권을 중심으로 안정적인 흐름을 보이고 있다. "
    "고용 시장은 서비스업을 중심으로 개선되고 있으며 실업률은 낮은 수준을 유지하고 있다. "
    "중소기업 지원 정책이 확대되면서 창업 활동도 증가하는 추세이다."
)

SAMPLE_NORMAL_ITEM: dict = {
    "id": "doc_001",
    "text": NORMAL_TEXT,
}

SAMPLE_SHORT_ITEM: dict = {
    "id": "doc_002",
    "text": "짧은 텍스트",
}

SAMPLE_PII_ITEM: dict = {
    "id": "doc_003",
    "text": NORMAL_TEXT + " 연락처: 010-1234-5678 으로 문의하세요.",
}

SAMPLE_DUPLICATE_ITEMS: list[dict] = [
    {"id": "doc_004", "text": NORMAL_TEXT},
    {"id": "doc_005", "text": NORMAL_TEXT},  # exact duplicate text
]

SAMPLE_SIMILAR_ITEMS: list[dict] = [
    {"id": "doc_006", "text": NORMAL_TEXT},
    {
        "id": "doc_007",
        "text": NORMAL_TEXT.replace("한국은행은", "한국은행이").replace("동결했다고", "동결했다"),
    },
]
