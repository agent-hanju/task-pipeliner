"""Naver news-specific preprocessing rules: patterns, filters, transforms."""

from __future__ import annotations

import re
from collections.abc import Callable

from text_rules import (
    remove_bracket_line_end,
    remove_bracket_line_start,
    remove_kv_credit,
    remove_repeated_inline_bullets,
    remove_reporter_sign,
    strip_leading_non_sentence_lines,
    strip_leading_symbolic_list,
    strip_trailing_non_sentence_lines,
    strip_trailing_symbolic_list,
    truncate_after_pattern,
)

# === Document-level transform patterns ===

BLOCK_TRIGGER_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"\[관련기사\]"),
    re.compile(r"▶\s*관련기사\s*◀"),
    re.compile(r"\[관련\s*뉴스\]"),
    re.compile(r"\[사진\s*영상\s*제보받습니다\]"),
    re.compile(r"\|\s*패밀리사이트"),
    re.compile(r"이데일리TV\n＞"),
    re.compile(r"IT는\s*아이뉴스24"),
]

PHOTO_CAPTION_BLOCK_RE: re.Pattern[str] = re.compile(r"\n{3,}[^\n]*\][ \t]*$", re.MULTILINE)

COPYRIGHT_CE_MULTI_RE: re.Pattern[str] = re.compile(
    r"<ⓒ[^>]*무단\s*\n\s*전재\s*배포금지>", re.MULTILINE
)

DOT_NAV_BLOCK_RE: re.Pattern[str] = re.compile(r"(?:ㆍ[^\n]+){3,}", re.MULTILINE)


def apply_naver_transforms(text: str) -> str:
    """Apply all Naver-specific document-level transforms in order."""
    # 1. Block trigger truncation
    for pattern in BLOCK_TRIGGER_PATTERNS:
        text = truncate_after_pattern(text, pattern)

    # 2. Photo caption block removal
    text = PHOTO_CAPTION_BLOCK_RE.sub("", text)

    # 3. Copyright multi-line removal
    text = COPYRIGHT_CE_MULTI_RE.sub("", text)

    # 4. Dot nav block removal
    text = DOT_NAV_BLOCK_RE.sub("", text)

    # 5. Bracket line start/end removal (per-line)
    lines = text.split("\n")
    lines = [remove_bracket_line_end(remove_bracket_line_start(line)) for line in lines]
    text = "\n".join(lines)

    # 6. Leading/trailing non-sentence line removal
    text = strip_leading_non_sentence_lines(text)
    text = strip_trailing_non_sentence_lines(text)

    # 7. Leading/trailing symbolic list removal
    text = strip_leading_symbolic_list(text)
    text = strip_trailing_symbolic_list(text)

    return text


# === Line filters ===


def _starts_with_any(line: str, prefixes: tuple[str, ...]) -> bool:
    return line.startswith(prefixes)


def _ends_with_any(line: str, suffixes: tuple[str, ...]) -> bool:
    return line.endswith(suffixes)


def _contains(line: str, substring: str) -> bool:
    return substring in line


def _contains_any(line: str, substrings: tuple[str, ...]) -> bool:
    return any(s in line for s in substrings)


# Pre-compiled regex patterns for line filters
_HASH_NUM_RE = re.compile(r"^#\d+")
_PHONE_RE = re.compile(r"\d{2,4}-\d{3,4}-\d{4}")
_BYLINE_RE = re.compile(r"^[가-힣]{2,5}\s*기자$")
_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+$")
_BRACKET_ONLY_RE = re.compile(r"^\[+[^\]]*\]+$")


# --- Naver-specific line filter functions (module-level, picklable) ---


def _naver_symbol_start(ln: str) -> bool:
    return _starts_with_any(ln, ("▲", "☞", "▶", "©", "ⓒ", "◆"))


def _naver_photo_prefix(ln: str) -> bool:
    return ln.startswith("사진은")


def _naver_photo_credit(ln: str) -> bool:
    return ln.startswith("/사진=")


def _naver_photo_position(ln: str) -> bool:
    return _starts_with_any(ln, ("(왼쪽", "(오른쪽", "(가운데"))


def _naver_mobile_cta(ln: str) -> bool:
    return ln.startswith("[모바일]")


def _naver_hot_video(ln: str) -> bool:
    return ln.startswith("[핫") and "영상" in ln


def _naver_hot_news(ln: str) -> bool:
    return _starts_with_any(ln, ("■핫뉴스", "■핫이슈"))


def _naver_naver_sub(ln: str) -> bool:
    return ln.startswith("네이버에서")


def _naver_subscribe(ln: str) -> bool:
    return ln.startswith("구독")


def _naver_download(ln: str) -> bool:
    return "다운받기" in ln


def _naver_app_tag(ln: str) -> bool:
    return "<" in ln and "앱" in ln and ">" in ln


def _naver_download_tag(ln: str) -> bool:
    return "<" in ln and "다운" in ln and ">" in ln


def _naver_sns_links(ln: str) -> bool:
    return _starts_with_any(ln, ("[트위터]", "[페이스북]", "[카카오스토리]", "[유튜브]"))


def _naver_digital_news(ln: str) -> bool:
    return ln.startswith("디지털뉴스팀")


def _naver_digital_times(ln: str) -> bool:
    return ln.endswith("디지털타임즈")


def _naver_hash_num(ln: str) -> bool:
    return bool(_HASH_NUM_RE.match(ln))


def _naver_arrow_down(ln: str) -> bool:
    return "↘" in ln


def _naver_triple_gt(ln: str) -> bool:
    return ln.endswith(">>>")


def _naver_short_dash(ln: str) -> bool:
    return ln.startswith("-") and len(ln) < 100


NAVER_LINE_FILTER_PREDICATES: list[Callable[[str], bool]] = [
    _naver_symbol_start,
    _naver_photo_prefix,
    _naver_photo_credit,
    _naver_photo_position,
    _naver_mobile_cta,
    _naver_hot_video,
    _naver_hot_news,
    _naver_naver_sub,
    _naver_subscribe,
    _naver_download,
    _naver_app_tag,
    _naver_download_tag,
    _naver_sns_links,
    _naver_digital_news,
    _naver_digital_times,
    _naver_hash_num,
    _naver_arrow_down,
    _naver_triple_gt,
    _naver_short_dash,
]


# --- Shared line filter functions (module-level, picklable) ---


def _shared_copyright_angle(ln: str) -> bool:
    return ln.startswith("<ⓒ")


def _shared_copyright_author(ln: str) -> bool:
    return ln.startswith("<저작권자")


def _shared_copyright_note(ln: str) -> bool:
    return ln.startswith("※저작권자")


def _shared_copyright_en(ln: str) -> bool:
    return ln.startswith("- Copyrights")


def _shared_copyright_symbol(ln: str) -> bool:
    return "ⓒ" in ln and len(ln) < 80


def _shared_url(ln: str) -> bool:
    return "http" in ln


def _shared_inquiry(ln: str) -> bool:
    return _contains_any(ln, ("기사문의", "기자문의", "상담료"))


def _shared_phone(ln: str) -> bool:
    return bool(_PHONE_RE.search(ln))


def _shared_byline(ln: str) -> bool:
    return bool(_BYLINE_RE.match(ln))


def _shared_email(ln: str) -> bool:
    return bool(_EMAIL_RE.search(ln))


def _shared_auto_generated(ln: str) -> bool:
    return "자동생성" in ln and "알고리즘" in ln


def _shared_slash_start(ln: str) -> bool:
    return ln.startswith("/")


def _shared_broadcast_tip(ln: str) -> bool:
    return "방송사 제보" in ln


def _shared_ending_keywords(ln: str) -> bool:
    return _ends_with_any(ln, ("기자", "연합뉴스", "금지", "구독", "가기"))


def _shared_bracket_only(ln: str) -> bool:
    return bool(_BRACKET_ONLY_RE.match(ln))


def _shared_triangle_end(ln: str) -> bool:
    return ln.endswith("▷")


def _shared_bullet_start(ln: str) -> bool:
    return ln.startswith("●")


SHARED_LINE_FILTER_PREDICATES: list[Callable[[str], bool]] = [
    _shared_copyright_angle,
    _shared_copyright_author,
    _shared_copyright_note,
    _shared_copyright_en,
    _shared_copyright_symbol,
    _shared_url,
    _shared_inquiry,
    _shared_phone,
    _shared_byline,
    _shared_email,
    _shared_auto_generated,
    _shared_slash_start,
    _shared_broadcast_tip,
    _shared_ending_keywords,
    _shared_bracket_only,
    _shared_triangle_end,
    _shared_bullet_start,
]


def should_filter_line(line: str) -> bool:
    """Return True if the line should be removed."""
    stripped = line.strip()
    if not stripped:
        return False
    for pred in NAVER_LINE_FILTER_PREDICATES:
        if pred(stripped):
            return True
    for pred in SHARED_LINE_FILTER_PREDICATES:
        if pred(stripped):
            return True
    return False


# === Line inline transforms ===


def apply_naver_line_inline(line: str) -> str:
    """Apply inline transforms to a single line."""
    line = remove_kv_credit(line)
    line = remove_reporter_sign(line)
    line = remove_repeated_inline_bullets(line)
    return line
