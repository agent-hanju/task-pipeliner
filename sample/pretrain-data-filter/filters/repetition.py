"""Repetition filter — W-04.

Gopher-style n-gram / line / paragraph duplication heuristics.
"""

from __future__ import annotations

from collections import Counter


def _dup_char_frac(segments: list[str]) -> float:
    """Character fraction contributed by duplicate segments."""
    if not segments:
        return 0.0
    total_chars = sum(len(s) for s in segments)
    if total_chars == 0:
        return 0.0
    counter = Counter(segments)
    dup_chars = sum(len(s) * count for s, count in counter.items() if count > 1)
    return dup_chars / total_chars


def _word_ngrams(words: list[str], n: int) -> Counter[tuple[str, ...]]:
    """Count word n-grams."""
    if len(words) < n:
        return Counter()
    return Counter(tuple(words[i : i + n]) for i in range(len(words) - n + 1))


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
    """Filter by repetition heuristics.

    1. Duplicate line char fraction → 'dup_line_frac'
    2. Duplicate paragraph char fraction → 'dup_para_frac'
    3. Top n-gram (2,3,4) char fraction → 'top_{n}gram_frac'
    4. Dup n-gram (5,6,7) char fraction → 'dup_{n}gram_frac'
    5. All pass → (True, '')
    """
    if not text.strip():
        return (True, "")

    # 1. Duplicate line fraction
    lines = [line.strip() for line in text.split("\n") if line.strip()]
    if _dup_char_frac(lines) > max_dup_line_frac:
        return (False, "dup_line_frac")

    # 2. Duplicate paragraph fraction
    paras = [p.strip() for p in text.split("\n\n") if p.strip()]
    if _dup_char_frac(paras) > max_dup_para_frac:
        return (False, "dup_para_frac")

    words = text.split()
    total_chars = len(text)
    if total_chars == 0:
        return (True, "")

    # 3. Top n-gram checks (2, 3, 4)
    top_thresholds = {
        2: max_top_2gram_frac,
        3: max_top_3gram_frac,
        4: max_top_4gram_frac,
    }
    for n, threshold in top_thresholds.items():
        counter = _word_ngrams(words, n)
        if counter:
            top_ng, top_count = counter.most_common(1)[0]
            ngram_char_len = sum(len(w) for w in top_ng)
            frac = (top_count * ngram_char_len) / total_chars
            if frac > threshold:
                return (False, f"top_{n}gram_frac")

    # 4. Dup n-gram checks (5, 6, 7) — skip if previous n-gram frac < 1e-9
    dup_thresholds = {
        5: max_dup_5gram_frac,
        6: max_dup_6gram_frac,
        7: max_dup_7gram_frac,
    }
    prev_frac = 1.0  # start high so first check always runs
    for n, threshold in dup_thresholds.items():
        if prev_frac < 1e-9:
            break
        counter = _word_ngrams(words, n)
        if counter:
            dup_chars = sum(
                count * sum(len(w) for w in ng) for ng, count in counter.items() if count > 1
            )
            frac = dup_chars / total_chars
            prev_frac = frac
            if frac > threshold:
                return (False, f"dup_{n}gram_frac")
        else:
            prev_frac = 0.0

    return (True, "")
