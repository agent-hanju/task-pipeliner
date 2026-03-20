"""Length filter — W-03."""

from __future__ import annotations


def filter_length(
    text: str,
    min_chars: int = 200,
    min_words: int = 50,
    max_words: int = 100_000,
    min_mean_word_length: float = 1.5,
) -> tuple[bool, str]:
    """Filter by text length criteria.

    1. strip → empty → (False, 'empty')
    2. len(text) < min_chars → (False, 'too_few_chars')
    3. word count < min_words → (False, 'too_few_words')
    4. word count > max_words → (False, 'too_many_words')
    5. mean word length < min_mean_word_length → (False, 'mean_word_too_short')
    6. All pass → (True, '')
    """
    text = text.strip()
    if not text:
        return (False, "empty")

    if len(text) < min_chars:
        return (False, "too_few_chars")

    words = text.split()
    num_words = len(words)

    if num_words < min_words:
        return (False, "too_few_words")
    if num_words > max_words:
        return (False, "too_many_words")

    mean_word_len = sum(len(w) for w in words) / num_words
    if mean_word_len < min_mean_word_length:
        return (False, "mean_word_too_short")

    return (True, "")
