"""Tests for naver_rules module."""

from __future__ import annotations

import pytest
from naver_rules import apply_naver_line_inline, apply_naver_transforms, should_filter_line


class TestApplyNaverTransforms:
    """apply_naver_transforms — document-level Naver transforms."""

    def test_block_trigger_truncation(self) -> None:
        text = "본문 내용입니다.\n[관련기사]\n관련 기사 1\n관련 기사 2"
        result = apply_naver_transforms(text)
        assert "[관련기사]" not in result
        assert "관련 기사 1" not in result

    def test_photo_caption_removal(self) -> None:
        text = "본문입니다.\n\n\n\n사진 캡션입니다]\n이후 내용."
        result = apply_naver_transforms(text)
        assert "사진 캡션" not in result

    def test_copyright_multiline_removal(self) -> None:
        text = "본문입니다.\n<ⓒ아시아경제 무단\n전재 배포금지>\n끝."
        result = apply_naver_transforms(text)
        assert "ⓒ아시아경제" not in result

    def test_bracket_removal(self) -> None:
        text = "[서울경제] 첫 문장입니다.\n두 번째 문장입니다."
        result = apply_naver_transforms(text)
        assert "[서울경제]" not in result

    def test_preserves_normal_content(self) -> None:
        text = "정상적인 뉴스 본문입니다.\n두 번째 문장도 있습니다."
        result = apply_naver_transforms(text)
        assert "정상적인 뉴스 본문입니다." in result


class TestShouldFilterLine:
    """should_filter_line — line filtering predicate."""

    @pytest.mark.parametrize(
        ("line", "expected"),
        [
            # Symbol starts — should filter
            ("▲ 사진 설명", True),
            ("☞ 바로가기", True),
            ("▶ 관련기사", True),
            ("ⓒ 무단전재", True),
            ("◆ 핫뉴스", True),
            # Photo-related
            ("사진은 기사와 관련 없음", True),
            ("/사진=홍길동 기자", True),
            ("(왼쪽부터) 홍길동", True),
            # CTA / promo
            ("[모바일] 앱 다운로드", True),
            ("네이버에서 구독하기", True),
            # Subscribe
            ("구독하기 버튼을 눌러주세요", True),
            # SNS
            ("[트위터] @news", True),
            ("[페이스북] 뉴스", True),
            # Copyright (shared)
            ("<ⓒ한국경제 무단전재 금지>", True),
            ("<저작권자 무단전재>", True),
            ("※저작권자 보호", True),
            # URL
            ("자세한 내용은 http://example.com 참조", True),
            # Byline
            ("홍길동 기자", True),
            ("user@example.com", True),
            # Ending patterns
            ("뉴스 연합뉴스", True),
            # Normal content — should NOT filter
            ("정상적인 뉴스 본문입니다.", False),
            ("경제 성장률이 3%를 기록했다.", False),
            ("", False),
        ],
        ids=[
            "triangle-up",
            "finger-right",
            "arrow-right",
            "copyright-c",
            "diamond",
            "photo-prefix",
            "photo-credit",
            "left-paren",
            "mobile-cta",
            "naver-sub",
            "subscribe",
            "twitter",
            "facebook",
            "copyright-angle",
            "copyright-author",
            "copyright-note",
            "url",
            "byline",
            "email",
            "ending-news",
            "normal-1",
            "normal-2",
            "empty",
        ],
    )
    def test_filter(self, line: str, expected: bool) -> None:
        assert should_filter_line(line) == expected


class TestApplyNaverLineInline:
    """apply_naver_line_inline — inline text transforms."""

    def test_kv_credit_removal(self) -> None:
        result = apply_naver_line_inline("(서울=뉴스1) 본문 내용입니다.")
        assert result == "본문 내용입니다."

    def test_reporter_sign_removal(self) -> None:
        result = apply_naver_line_inline("홍길동 기자 = 서울시가 발표했다.")
        assert result == "서울시가 발표했다."

    def test_repeated_bullet_removal(self) -> None:
        result = apply_naver_line_inline("◆항목1◆항목2◆항목3")
        assert result == "항목1항목2항목3"

    def test_normal_line_unchanged(self) -> None:
        line = "일반적인 뉴스 본문 내용입니다."
        assert apply_naver_line_inline(line) == line
