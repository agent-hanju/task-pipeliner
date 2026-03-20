"""Multi-format file loader: JSON array / JSONL, directory traversal."""

from __future__ import annotations

import logging
import re
from collections.abc import Generator
from pathlib import Path
from typing import Literal

import orjson

logger = logging.getLogger(__name__)

_DATE_PREFIX_RE = re.compile(r"^\d{8}")
_BOM = b"\xef\xbb\xbf"


def detect_format(path: Path) -> Literal["json", "jsonl"]:
    """Detect file format by reading the first 64KB.

    - First non-whitespace byte (after BOM removal) is '[' → json (array)
    - First line is a complete JSON object → jsonl
    - Otherwise → json
    """
    logger.debug("path=%s", path)
    raw = path.read_bytes()[:65536]
    if raw.startswith(_BOM):
        raw = raw[len(_BOM) :]
    stripped = raw.lstrip()
    if stripped and stripped[0:1] == b"[":
        return "json"
    # Try parsing first line as JSON object
    first_line_end = raw.find(b"\n")
    first_line = raw[:first_line_end] if first_line_end != -1 else raw
    first_line = first_line.strip()
    if first_line:
        try:
            obj = orjson.loads(first_line)
            if isinstance(obj, dict):
                return "jsonl"
        except orjson.JSONDecodeError:
            pass
    return "json"


def extract_date_prefix(path: Path) -> str:
    """Extract leading 8-digit date from filename stem.

    Example: '20240115_naver.json' → '20240115'
    Returns '00000000' if no 8-digit prefix found.
    """
    logger.debug("path=%s", path)
    stem = path.stem
    match = _DATE_PREFIX_RE.match(stem)
    if match:
        return match.group()
    return "00000000"


def load_json_file(path: Path) -> list[dict]:
    """Load a JSON file. Handles BOM, 'data' key wrapper, and non-list results."""
    logger.debug("path=%s", path)
    raw = path.read_bytes()
    if raw.startswith(_BOM):
        raw = raw[len(_BOM) :]
    parsed = orjson.loads(raw)
    if isinstance(parsed, dict) and "data" in parsed:
        parsed = parsed["data"]
    if not isinstance(parsed, list):
        parsed = [parsed]
    logger.debug("loaded %d items", len(parsed))
    return parsed


def load_jsonl_file(path: Path) -> Generator[dict, None, None]:
    """Stream-load a JSONL file, yielding one dict per non-empty line."""
    logger.debug("path=%s", path)
    count = 0
    with path.open("r", encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield orjson.loads(line)
            count += 1
    logger.debug("yielded %d items", count)


def resolve_paths(paths: list[Path]) -> list[Path]:
    """Expand directories to their contained *.json and *.jsonl files (sorted)."""
    logger.debug("paths=%d", len(paths))
    result: list[Path] = []
    for p in paths:
        if p.is_dir():
            children = sorted(f for f in p.iterdir() if f.suffix in (".json", ".jsonl"))
            result.extend(children)
        else:
            result.append(p)
    logger.debug("resolved %d files", len(result))
    return result


def load_items(paths: list[Path]) -> Generator[dict, None, None]:
    """Load items from multiple paths. Injects '_date_prefix' into each item."""
    logger.debug("paths=%d", len(paths))
    resolved = resolve_paths(paths)
    for file_path in resolved:
        date_prefix = extract_date_prefix(file_path)
        fmt = detect_format(file_path)
        logger.info("loading file=%s format=%s date_prefix=%s", file_path.name, fmt, date_prefix)
        if fmt == "json":
            items = load_json_file(file_path)
            for item in items:
                item["_date_prefix"] = date_prefix
                yield item
        else:
            for item in load_jsonl_file(file_path):
                item["_date_prefix"] = date_prefix
                yield item
