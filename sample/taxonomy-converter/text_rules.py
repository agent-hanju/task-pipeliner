"""Generic text preprocessing: normalize, line filter, inline transform."""

from __future__ import annotations

import re

# === Normalization functions ===

_NEWLINE_RE = re.compile(r"\r\n|\r|\u2028|\u2029")
_VTAB_RE = re.compile(r"[\v\f]")
_SPACES_RE = re.compile(r"[ \t\xa0\u2003\u3000]+")
_BLANK_LINES_RE = re.compile(r"\n{3,}")

# Symbol characters that invalidate a sentence line start
_SYMBOL_START_CHARS = frozenset("※■□▪▫▶▷◀◁►◄▸◂●○◆◇△▲▽▼→←↑↓↗↘↙↖★☆·•ㆍ・☞☛◉◎")

# Bullet start characters (subset of symbols used for list items)
_BULLET_CHARS = frozenset("※■▶●◆△▲→☞·•ㆍ")

# Bracket tag patterns
_BRACKET_START_RE = re.compile(r"^\[[^\]]*\]\s*")
_BRACKET_END_RE = re.compile(r"\s*\[[^\]]*\]$")

# KV credit: (지역=통신사)
_KV_CREDIT_RE = re.compile(r"\([^)]{1,10}=[^)]{1,20}\)\s*")

# Reporter sign: 2-5 char Korean name + 기자 + optional whitespace + =
_REPORTER_SIGN_RE = re.compile(r"[가-힣]{2,5}\s*기자\s*=\s*")

# Repeated inline bullets
_REPEATED_BULLET_RE = re.compile(r"([◆△▲◇●])\1+|([◆△▲◇●])(?=.*\2)")


def normalize_newlines(text: str) -> str:
    r"""Normalize line breaks: \r\n/\r/\u2028/\u2029 → \n, \v/\f → ' '."""
    text = _NEWLINE_RE.sub("\n", text)
    text = _VTAB_RE.sub(" ", text)
    return text


def normalize_spaces(text: str) -> str:
    """Collapse consecutive spaces (including NBSP, em-space, ideographic space)."""
    return _SPACES_RE.sub(" ", text)


def strip_lines(text: str) -> str:
    """Strip leading/trailing whitespace from each line."""
    return "\n".join(line.strip() for line in text.split("\n"))


def collapse_blank_lines(text: str) -> str:
    """Collapse 3+ consecutive blank lines to 2."""
    return _BLANK_LINES_RE.sub("\n\n", text)


def normalize_text(text: str) -> str:
    """Apply all 4 normalization steps in order."""
    text = normalize_newlines(text)
    text = normalize_spaces(text)
    text = strip_lines(text)
    text = collapse_blank_lines(text)
    return text


# === Document-level transforms ===


def truncate_after_pattern(text: str, pattern: re.Pattern[str]) -> str:
    """Remove everything from the first match of pattern onward."""
    m = pattern.search(text)
    if m:
        return text[: m.start()]
    return text



# === Line-level判定/transform functions ===


def is_valid_sentence_line(line: str) -> bool:
    """Check if a line is a valid sentence.

    Valid if: ends with '.' and does not start with a symbol character.
    """
    stripped = line.strip()
    if not stripped:
        return False
    if stripped[0] in _SYMBOL_START_CHARS:
        return False
    return stripped.endswith(".")


def strip_leading_non_sentence_lines(text: str) -> str:
    """Remove leading lines that are not valid sentences."""
    lines = text.split("\n")
    start = 0
    for i, line in enumerate(lines):
        if line.strip() == "":
            continue
        if is_valid_sentence_line(line):
            start = i
            break
    else:
        return text
    return "\n".join(lines[start:])


def strip_trailing_non_sentence_lines(text: str) -> str:
    """Remove trailing lines that are not valid sentences."""
    lines = text.split("\n")
    end = len(lines)
    for i in range(len(lines) - 1, -1, -1):
        if lines[i].strip() == "":
            continue
        if is_valid_sentence_line(lines[i]):
            end = i + 1
            break
    else:
        return text
    return "\n".join(lines[:end])


def has_list_bullet_start(line: str) -> bool:
    """Check if line starts with a bullet symbol."""
    stripped = line.strip()
    if not stripped:
        return False
    return stripped[0] in _BULLET_CHARS


def strip_leading_symbolic_list(text: str) -> str:
    """Remove leading bullet list block from text.

    Algorithm: scan non-empty lines; track last bullet line index.
    Once 3+ consecutive clean (non-bullet) lines appear, remove
    everything up to and including the last bullet line.
    """
    lines = text.split("\n")
    last_bullet_idx = -1
    clean_count = 0

    for i, line in enumerate(lines):
        if not line.strip():
            continue
        if has_list_bullet_start(line):
            last_bullet_idx = i
            clean_count = 0
        else:
            clean_count += 1
            if clean_count >= 3 and last_bullet_idx >= 0:
                return "\n".join(lines[last_bullet_idx + 1 :])

    return text


def strip_trailing_symbolic_list(text: str) -> str:
    """Remove trailing bullet list block from text (reverse of leading)."""
    lines = text.split("\n")
    last_bullet_idx = len(lines)
    clean_count = 0

    for i in range(len(lines) - 1, -1, -1):
        if not lines[i].strip():
            continue
        if has_list_bullet_start(lines[i]):
            last_bullet_idx = i
            clean_count = 0
        else:
            clean_count += 1
            if clean_count >= 3 and last_bullet_idx < len(lines):
                return "\n".join(lines[:last_bullet_idx])

    return text


def remove_bracket_line_start(line: str) -> str:
    """Remove bracket tag at start of line. '[서울경제] 본문' → '본문'."""
    return _BRACKET_START_RE.sub("", line)


def remove_bracket_line_end(line: str) -> str:
    """Remove bracket tag at end of line. '본문 [단독]' → '본문'."""
    return _BRACKET_END_RE.sub("", line)


def remove_kv_credit(line: str) -> str:
    """Remove (지역=통신사) credit. '(서울=뉴스1) 본문' → '본문'."""
    return _KV_CREDIT_RE.sub("", line)


def remove_reporter_sign(line: str) -> str:
    """Remove reporter signature. '홍길동 기자 = ' → ''."""
    return _REPORTER_SIGN_RE.sub("", line).strip()


def remove_repeated_inline_bullets(line: str) -> str:
    """Remove bullet symbols that appear 2+ times inline."""
    bullets_in_line = [c for c in line if c in "◆△▲◇●"]
    if len(bullets_in_line) < 2:
        return line
    return re.sub(r"[◆△▲◇●]", "", line)
