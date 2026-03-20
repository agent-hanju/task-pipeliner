"""Tests for text_rules module."""

from __future__ import annotations

import re

import pytest
from text_rules import (
    collapse_blank_lines,
    has_list_bullet_start,
    is_valid_sentence_line,
    normalize_newlines,
    normalize_spaces,
    normalize_text,
    remove_bracket_line_end,
    remove_bracket_line_start,
    remove_kv_credit,
    remove_repeated_inline_bullets,
    remove_reporter_sign,
    strip_leading_non_sentence_lines,
    strip_leading_symbolic_list,
    strip_lines,
    strip_trailing_non_sentence_lines,
    strip_trailing_symbolic_list,
    truncate_after_pattern,
)


class TestNormalizeNewlines:
    """normalize_newlines — line break normalization."""

    @pytest.mark.parametrize(
        ("inp", "expected"),
        [
            ("a\r\nb", "a\nb"),
            ("a\rb", "a\nb"),
            ("a\u2028b", "a\nb"),
            ("a\u2029b", "a\nb"),
            ("a\vb", "a b"),
            ("a\fb", "a b"),
            ("a\nb", "a\nb"),
        ],
        ids=["crlf", "cr", "line-sep", "para-sep", "vtab", "formfeed", "lf-unchanged"],
    )
    def test_normalize(self, inp: str, expected: str) -> None:
        assert normalize_newlines(inp) == expected


class TestNormalizeSpaces:
    """normalize_spaces — consecutive space collapsing."""

    @pytest.mark.parametrize(
        ("inp", "expected"),
        [
            ("a  b\t\tc", "a b c"),
            ("a\xa0\xa0b", "a b"),
            ("a\u3000b", "a b"),
            ("a b", "a b"),
        ],
        ids=["tabs-spaces", "nbsp", "ideographic", "single-space"],
    )
    def test_normalize(self, inp: str, expected: str) -> None:
        assert normalize_spaces(inp) == expected


class TestStripLines:
    """strip_lines — per-line whitespace stripping."""

    def test_basic(self) -> None:
        assert strip_lines("  hello  \n  world  ") == "hello\nworld"

    def test_empty_lines_preserved(self) -> None:
        assert strip_lines("a\n\nb") == "a\n\nb"


class TestCollapseBlankLines:
    """collapse_blank_lines — 3+ blank lines → 2."""

    def test_three_to_two(self) -> None:
        assert collapse_blank_lines("a\n\n\nb") == "a\n\nb"

    def test_two_unchanged(self) -> None:
        assert collapse_blank_lines("a\n\nb") == "a\n\nb"

    def test_five_to_two(self) -> None:
        assert collapse_blank_lines("a\n\n\n\n\nb") == "a\n\nb"


class TestNormalizeText:
    """normalize_text — integrated normalization."""

    def test_combined(self) -> None:
        text = "  hello  \r\n  world  \n\n\n\nend  "
        result = normalize_text(text)
        assert result == "hello\nworld\n\nend"


class TestTruncateAfterPattern:
    """truncate_after_pattern — cut text at first match."""

    def test_match(self) -> None:
        pattern = re.compile(r"\[관련기사\]")
        result = truncate_after_pattern("본문입니다. [관련기사] 추가내용", pattern)
        assert result == "본문입니다. "

    def test_no_match(self) -> None:
        pattern = re.compile(r"\[관련기사\]")
        text = "본문입니다."
        assert truncate_after_pattern(text, pattern) == text



class TestIsValidSentenceLine:
    """is_valid_sentence_line — sentence validity check."""

    @pytest.mark.parametrize(
        ("line", "expected"),
        [
            ("정상 문장입니다.", True),
            ("마침표 없음", False),
            ("▶ 기호 시작.", False),
            ("※ 참고사항.", False),
            ("", False),
            ("   ", False),
        ],
        ids=["valid", "no-period", "symbol-start", "note-symbol", "empty", "whitespace"],
    )
    def test_validity(self, line: str, expected: bool) -> None:
        assert is_valid_sentence_line(line) == expected


class TestStripNonSentenceLines:
    """strip_leading/trailing_non_sentence_lines."""

    def test_strip_leading(self) -> None:
        text = "[헤더]\n기자명\n본문 첫 문장입니다.\n두 번째 문장."
        result = strip_leading_non_sentence_lines(text)
        assert result.startswith("본문 첫 문장입니다.")

    def test_strip_trailing(self) -> None:
        text = "본문입니다.\n끝 문장.\n저작권 표기\n기자 이름"
        result = strip_trailing_non_sentence_lines(text)
        assert result.endswith("끝 문장.")


class TestHasListBulletStart:
    """has_list_bullet_start — bullet detection."""

    @pytest.mark.parametrize(
        ("line", "expected"),
        [
            ("■ 항목1", True),
            ("▶ 항목2", True),
            ("● 항목3", True),
            ("일반 텍스트", False),
            ("", False),
        ],
        ids=["square", "arrow", "circle", "normal", "empty"],
    )
    def test_bullet(self, line: str, expected: bool) -> None:
        assert has_list_bullet_start(line) == expected


class TestStripSymbolicList:
    """strip_leading/trailing_symbolic_list — bullet block removal."""

    def test_strip_leading(self) -> None:
        lines = [
            "■ 항목1",
            "● 항목2",
            "▶ 항목3",
            "정상 문장1.",
            "정상 문장2.",
            "정상 문장3.",
            "정상 문장4.",
        ]
        text = "\n".join(lines)
        result = strip_leading_symbolic_list(text)
        assert "■ 항목1" not in result
        assert "정상 문장1." in result

    def test_strip_trailing(self) -> None:
        lines = [
            "정상 문장1.",
            "정상 문장2.",
            "정상 문장3.",
            "정상 문장4.",
            "■ 항목1",
            "● 항목2",
            "▶ 항목3",
        ]
        text = "\n".join(lines)
        result = strip_trailing_symbolic_list(text)
        assert "■ 항목1" not in result
        assert "정상 문장4." in result

    def test_no_bullets(self) -> None:
        text = "일반 문장1.\n일반 문장2.\n일반 문장3."
        assert strip_leading_symbolic_list(text) == text
        assert strip_trailing_symbolic_list(text) == text


class TestRemoveBracketLineStart:
    """remove_bracket_line_start — bracket tag removal."""

    @pytest.mark.parametrize(
        ("line", "expected"),
        [
            ("[뉴스1] 본문", "본문"),
            ("[서울경제] 기사 내용입니다.", "기사 내용입니다."),
            ("태그 없는 본문", "태그 없는 본문"),
        ],
        ids=["news1", "seoul-econ", "no-tag"],
    )
    def test_remove(self, line: str, expected: str) -> None:
        assert remove_bracket_line_start(line) == expected


class TestRemoveBracketLineEnd:
    """remove_bracket_line_end — trailing bracket tag removal."""

    def test_remove(self) -> None:
        assert remove_bracket_line_end("본문 [단독]") == "본문"

    def test_no_tag(self) -> None:
        assert remove_bracket_line_end("일반 텍스트") == "일반 텍스트"


class TestRemoveKvCredit:
    """remove_kv_credit — (지역=통신사) removal."""

    @pytest.mark.parametrize(
        ("line", "expected"),
        [
            ("(서울=뉴스1) 본문입니다.", "본문입니다."),
            ("(부산=연합뉴스) 기사.", "기사."),
            ("일반 텍스트", "일반 텍스트"),
        ],
        ids=["seoul", "busan", "no-credit"],
    )
    def test_remove(self, line: str, expected: str) -> None:
        assert remove_kv_credit(line) == expected


class TestRemoveReporterSign:
    """remove_reporter_sign — reporter signature removal."""

    def test_remove(self) -> None:
        assert remove_reporter_sign("홍길동 기자 = ") == ""

    def test_with_content(self) -> None:
        result = remove_reporter_sign("김철수 기자 = 서울시가 발표했다.")
        assert result == "서울시가 발표했다."

    def test_no_sign(self) -> None:
        assert remove_reporter_sign("일반 문장입니다.") == "일반 문장입니다."


class TestRemoveRepeatedInlineBullets:
    """remove_repeated_inline_bullets — repeated bullet removal."""

    def test_remove(self) -> None:
        assert remove_repeated_inline_bullets("◆항목1◆항목2") == "항목1항목2"

    def test_single_bullet_kept(self) -> None:
        assert remove_repeated_inline_bullets("◆단일항목") == "◆단일항목"

    def test_no_bullets(self) -> None:
        assert remove_repeated_inline_bullets("일반 텍스트") == "일반 텍스트"
