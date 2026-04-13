"""Utility helpers for dates, JSONL, CSV, compression, and text normalization."""

import csv
import gzip
import hashlib
import json
import math
import os
import re
from contextlib import contextmanager
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Iterator, List, Sequence, Tuple


DATE_FMT = "%Y-%m-%d"


def parse_date(value: str) -> date:
    return datetime.strptime(value, DATE_FMT).date()


def format_date(value: date) -> str:
    return value.strftime(DATE_FMT)


def coerce_date(value, default_month: int = 1, default_day: int = 1) -> str:
    coerced, _ = coerce_date_with_precision(
        value, default_month=default_month, default_day=default_day
    )
    return coerced


def coerce_date_with_precision(
    value, default_month: int = 1, default_day: int = 1
) -> Tuple[str, str]:
    if isinstance(value, date):
        return format_date(value), "day"
    if value is None:
        raise ValueError("Date value is required")
    if isinstance(value, int):
        return "%04d-%02d-%02d" % (value, default_month, default_day), "year_imputed"
    value = str(value).strip()
    if re.fullmatch(r"\d{4}", value):
        return "%s-%02d-%02d" % (value, default_month, default_day), "year_imputed"
    if re.fullmatch(r"\d{4}-\d{2}", value):
        return "%s-%02d" % (value, default_day), "month_imputed"
    parse_date(value)
    return value, "day"


def add_months(base: date, months: int) -> date:
    month_index = base.month - 1 + months
    year = base.year + month_index // 12
    month = month_index % 12 + 1
    day = min(base.day, _days_in_month(year, month))
    return date(year, month, day)


def _days_in_month(year: int, month: int) -> int:
    if month == 2:
        leap = year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)
        return 29 if leap else 28
    if month in {4, 6, 9, 11}:
        return 30
    return 31


def slugify(value: str) -> str:
    text = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower())
    return text.strip("-") or "record"


def normalize_doi(value: str) -> str:
    text = (value or "").strip()
    text = re.sub(r"^https?://(dx\.)?doi\.org/", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^doi:\s*", "", text, flags=re.IGNORECASE)
    return text.lower()


def dedupe_preserve_order(values: Sequence[str]) -> List[str]:
    seen = set()
    output = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        output.append(value)
    return output


def tokenize(text: str) -> List[str]:
    cleaned = re.sub(r"[^a-zA-Z0-9 ]+", " ", text.lower())
    return [token for token in cleaned.split() if len(token) > 2]


def sigmoid(value: float) -> float:
    if value >= 0:
        exp_neg = math.exp(-value)
        return 1.0 / (1.0 + exp_neg)
    exp_pos = math.exp(value)
    return exp_pos / (1.0 + exp_pos)


def dot(left: Sequence[float], right: Sequence[float]) -> float:
    return sum(a * b for a, b in zip(left, right))


@contextmanager
def open_text(path: Path, mode: str = "rt"):
    if "b" in mode:
        raise ValueError("open_text only supports text mode")
    path = Path(path)
    if path.suffix == ".gz":
        handle = gzip.open(path, mode, encoding="utf-8", newline="")
    else:
        handle = path.open(mode, encoding="utf-8", newline="")
    try:
        yield handle
    finally:
        handle.close()


@contextmanager
def open_binary(path: Path, mode: str = "rb"):
    if "b" not in mode:
        raise ValueError("open_binary only supports binary mode")
    path = Path(path)
    if path.suffix == ".gz":
        handle = gzip.open(path, mode)
    else:
        handle = path.open(mode)
    try:
        yield handle
    finally:
        handle.close()


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = _temp_path_for_atomic(path)
    with tmp_path.open("w", encoding="utf-8") as handle:
        handle.write(text)
    os.replace(tmp_path, path)


def write_json(path: Path, payload) -> None:
    atomic_write_text(path, json.dumps(payload, indent=2, sort_keys=True))


def read_json(path: Path):
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_jsonl(path: Path, rows: Iterable) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = _temp_path_for_atomic(path)
    with open_text(tmp_path, "wt") as handle:
        for row in rows:
            payload = asdict(row) if is_dataclass(row) else row
            handle.write(json.dumps(payload, sort_keys=True))
            handle.write("\n")
    os.replace(tmp_path, path)


def iter_jsonl(path: Path) -> Iterator[dict]:
    with open_text(path, "rt") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def read_jsonl(path: Path) -> List[dict]:
    return list(iter_jsonl(path))


def iter_csv_rows(path: Path) -> Iterator[dict]:
    with open_text(path, "rt") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            yield dict(row)


def write_csv(path: Path, rows: List[dict], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = _temp_path_for_atomic(path)
    with open_text(tmp_path, "wt") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    os.replace(tmp_path, path)


def hash_file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def count_jsonl_rows(path: Path) -> int:
    with open_text(path, "rt") as handle:
        return sum(1 for line in handle if line.strip())


def discover_files(root: Path, suffixes: Sequence[str]) -> List[Path]:
    root = Path(root)
    discovered = []
    suffixes = tuple(suffixes)
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        name = path.name.lower()
        if any(name.endswith(suffix) for suffix in suffixes):
            discovered.append(path)
    return discovered


def flatten_list(values: Sequence[Sequence[str]]) -> List[str]:
    output = []
    for group in values:
        output.extend(group)
    return output


def first_nonempty(*values):
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def restore_inverted_abstract(inverted_index) -> str:
    if not inverted_index:
        return ""
    placed = {}
    for token, positions in inverted_index.items():
        for position in positions:
            placed[int(position)] = token
    return " ".join(token for _, token in sorted(placed.items()))


def _temp_path_for_atomic(path: Path) -> Path:
    if path.suffix == ".gz":
        base = path.name[: -len(".gz")]
        return path.parent / (base + ".tmp.gz")
    return path.parent / (path.name + ".tmp")
