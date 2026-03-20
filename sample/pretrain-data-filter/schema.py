"""Data schema definitions — W-01.

TaxonomyDict: full taxonomy schema with metadata.
SimpleTaxonomyDict: minimal schema with id + text.
detect_schema: detect which schema a record matches.
"""

from __future__ import annotations

from typing import Literal, TypedDict


class MetadataDict(TypedDict, total=False):
    """Metadata sub-schema."""

    source: str
    published_date: str
    collected_date: str
    token_len: int
    quality_level: str
    author: str


class TaxonomyDict(TypedDict, total=False):
    """Full taxonomy schema."""

    dataset_name: str
    id: str
    text: str
    language: str
    type: str
    method: str
    category: str
    industrial_field: str
    template: str
    metadata: MetadataDict


class SimpleTaxonomyDict(TypedDict):
    """Minimal schema — id + text."""

    id: str
    text: str


def detect_schema(record: dict[str, object]) -> Literal["taxonomy", "simple"]:
    """Detect schema type.

    1. 'metadata' key present → 'taxonomy'
    2. Otherwise → 'simple'
    """
    if "metadata" in record:
        return "taxonomy"
    return "simple"
