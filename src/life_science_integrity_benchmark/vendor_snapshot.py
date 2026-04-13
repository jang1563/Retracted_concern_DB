"""Vendor-archive collection helpers and raw-snapshot staging utilities."""

import csv
import gzip
import json
import shutil
import tarfile
from calendar import monthrange
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional

from .collectors import extract_notice_rows_for_export, normalize_notice_label
from .utils import hash_file_sha256, normalize_doi, open_text, parse_date, write_csv, write_json


RAW_SNAPSHOT_SIDECAR_NAMES = {
    "source_versions.json",
    "sha256_manifest.tsv",
    "fetch.log",
    "manifest",
    "LICENSE.txt",
    "RELEASE_NOTES.txt",
}

NOTICE_EXPORT_FIELDNAMES = [
    "doi",
    "notice_type",
    "notice_date",
    "source_name",
    "source_url",
    "rights_status",
]


def freeze_period(snapshot_label: str) -> Dict[str, object]:
    base = snapshot_label
    if base.endswith("-freeze"):
        base = base[: -len("-freeze")]
    parts = base.split("-")
    if len(parts) != 2:
        raise ValueError("snapshot label must look like YYYY-MM-freeze")
    year = int(parts[0])
    month = int(parts[1])
    if month < 1 or month > 12:
        raise ValueError("snapshot month must be 1-12")
    last_day = monthrange(year, month)[1]
    freeze_end = "%04d-%02d-%02d" % (year, month, last_day)
    return {
        "snapshot_label": snapshot_label,
        "period_key": "%04d-%02d" % (year, month),
        "year": year,
        "month": month,
        "freeze_end": freeze_end,
    }


def extract_crossref_official_notices(
    archive_path: Path,
    output_path: Path,
    snapshot_label: str,
) -> Dict[str, object]:
    archive_path = Path(archive_path)
    output_path = Path(output_path)
    freeze_end = freeze_period(snapshot_label)["freeze_end"]
    rows: List[dict] = []
    seen = set()
    scanned_records = 0

    with tarfile.open(archive_path, "r:*") as archive:
        for member in archive:
            if not member.isfile():
                continue
            file_obj = archive.extractfile(member)
            if file_obj is None:
                continue
            payload = file_obj.read()
            for line_number, record in enumerate(iter_crossref_member_records(payload, member.name), start=1):
                scanned_records += 1
                try:
                    notices = extract_notice_rows_for_export(
                        record,
                        snapshot_id=snapshot_label,
                        file_id=member.name,
                        line_number=line_number,
                        source_name_override="Crossref Metadata Plus",
                        rights_status_override="metadata_only",
                    )
                except ValueError:
                    continue
                for notice in notices:
                    if notice["notice_date"] > freeze_end:
                        continue
                    export_row = {
                        "doi": notice["doi"],
                        "notice_type": notice["notice_type"],
                        "notice_date": notice["notice_date"],
                        "source_name": notice["source_name"],
                        "source_url": notice["source_url"],
                        "rights_status": notice["rights_status"],
                    }
                    key = tuple(export_row[field] for field in NOTICE_EXPORT_FIELDNAMES[:-1]) + (
                        export_row["rights_status"],
                    )
                    if key in seen:
                        continue
                    seen.add(key)
                    rows.append(export_row)

    rows.sort(key=lambda row: (row["doi"], row["notice_date"], row["notice_type"], row["source_url"]))
    write_csv(output_path, rows, NOTICE_EXPORT_FIELDNAMES)
    return {
        "output_path": output_path,
        "row_count": len(rows),
        "scanned_records": scanned_records,
        "freeze_end": freeze_end,
    }


def extract_retraction_watch_csv(
    source_csv_path: Path,
    output_path: Path,
    snapshot_label: str,
) -> Dict[str, object]:
    source_csv_path = Path(source_csv_path)
    output_path = Path(output_path)
    freeze_end = freeze_period(snapshot_label)["freeze_end"]
    rows: List[dict] = []
    seen = set()
    scanned_rows = 0

    with open_text(source_csv_path, "rt") as handle:
        reader = csv.DictReader(handle)
        for source_row in reader:
            scanned_rows += 1
            doi = _first_nonempty(
                source_row.get("OriginalPaperDOI"),
                source_row.get("Original Paper DOI"),
                source_row.get("doi"),
            )
            doi = normalize_doi(doi or "")
            if not doi or doi in {"unavailable", "unavailable.", "na", "n/a"}:
                continue

            notice_type = normalize_notice_label(
                _first_nonempty(
                    source_row.get("RetractionNature"),
                    source_row.get("Retraction Nature"),
                    source_row.get("notice_type"),
                )
            )
            if notice_type not in {"retraction", "expression_of_concern", "major_correction"}:
                continue

            notice_date = _first_nonempty(
                source_row.get("RetractionDate"),
                source_row.get("Retraction Date"),
                source_row.get("notice_date"),
            )
            if not notice_date:
                continue
            notice_date = str(notice_date).strip()
            try:
                notice_date = str(parse_date(notice_date))
            except ValueError:
                continue
            if notice_date > freeze_end:
                continue

            source_url = _extract_retraction_watch_source_url(source_row)
            export_row = {
                "doi": doi,
                "notice_type": notice_type,
                "notice_date": notice_date,
                "source_name": "Retraction Watch",
                "source_url": source_url,
                "rights_status": "metadata_only",
            }
            key = tuple(export_row[field] for field in NOTICE_EXPORT_FIELDNAMES)
            if key in seen:
                continue
            seen.add(key)
            rows.append(export_row)

    rows.sort(key=lambda row: (row["doi"], row["notice_date"], row["notice_type"], row["source_url"]))
    write_csv(output_path, rows, NOTICE_EXPORT_FIELDNAMES)
    return {
        "output_path": output_path,
        "row_count": len(rows),
        "scanned_rows": scanned_rows,
        "freeze_end": freeze_end,
    }


def stage_vendor_archive_to_raw_snapshot(
    vendor_root: Path,
    raw_root: Path,
    snapshot_label: str,
    mode: str = "copy",
    allow_missing_crossref: bool = False,
) -> Dict[str, object]:
    vendor_root = Path(vendor_root)
    raw_root = Path(raw_root)
    period = freeze_period(snapshot_label)
    period_key = period["period_key"]
    freeze_end = period["freeze_end"]
    raw_root.mkdir(parents=True, exist_ok=True)

    openalex_vendor = vendor_root / "openalex" / period_key
    crossref_vendor = vendor_root / "crossref" / period_key
    retraction_watch_vendor = _latest_retraction_watch_dir(vendor_root / "retraction_watch", freeze_end)
    pubmed_baseline_vendor = vendor_root / "pubmed" / "baseline" / str(period["year"])
    pubmed_update_vendor = vendor_root / "pubmed" / "updatefiles" / period_key

    openalex_raw = raw_root / "openalex"
    notices_raw = raw_root / "official_notices"
    pubmed_raw = raw_root / "pubmed"
    for path in (openalex_raw, notices_raw, pubmed_raw):
        path.mkdir(parents=True, exist_ok=True)

    openalex_files = _stage_openalex_vendor(openalex_vendor, openalex_raw, mode=mode)
    crossref_archive = _find_first_file(crossref_vendor, ("all.json.tar.gz",))
    crossref_result = {"row_count": 0, "skipped": False}
    if crossref_archive is None:
        if not allow_missing_crossref:
            raise FileNotFoundError("Missing Crossref archive under %s" % crossref_vendor)
        crossref_result["skipped"] = True
    else:
        crossref_output = notices_raw / ("crossref_updates_%s.csv.gz" % period_key)
        crossref_result = extract_crossref_official_notices(
            crossref_archive,
            crossref_output,
            snapshot_label=snapshot_label,
        )

    retraction_watch_output = None
    retraction_watch_result = {"row_count": 0}
    if retraction_watch_vendor is not None:
        source_csv = _find_first_file(
            retraction_watch_vendor,
            ("retraction_watch.csv", "retraction_watch.csv.gz", ".csv", ".csv.gz"),
        )
        if source_csv is not None:
            retraction_watch_output = notices_raw / (
                "retraction_watch_%s.csv.gz" % retraction_watch_vendor.name
            )
            retraction_watch_result = extract_retraction_watch_csv(
                source_csv,
                retraction_watch_output,
                snapshot_label=snapshot_label,
            )

    pubmed_files = _stage_pubmed_vendor(pubmed_baseline_vendor, pubmed_update_vendor, pubmed_raw, mode=mode)

    _write_bucket_metadata(
        openalex_raw,
        snapshot_label=snapshot_label,
        batches=[
            {
                "source_name": "OpenAlex works snapshot",
                "vendor_path": str(openalex_vendor),
                "staged_file_count": len(openalex_files),
            }
        ],
    )
    notice_batches = []
    if crossref_archive is not None:
        notice_batches.append(
            {
                "source_name": "Crossref Metadata Plus",
                "vendor_path": str(crossref_archive),
                "staged_file_count": crossref_result["row_count"],
            }
        )
    if retraction_watch_output is not None:
        notice_batches.append(
            {
                "source_name": "Retraction Watch",
                "vendor_path": str(retraction_watch_vendor),
                "staged_file_count": retraction_watch_result["row_count"],
            }
        )
    _write_bucket_metadata(notices_raw, snapshot_label=snapshot_label, batches=notice_batches)
    _write_bucket_metadata(
        pubmed_raw,
        snapshot_label=snapshot_label,
        batches=[
            {
                "source_name": "PubMed baseline + updatefiles",
                "vendor_path": str(pubmed_baseline_vendor),
                "update_vendor_path": str(pubmed_update_vendor),
                "staged_file_count": len(pubmed_files),
            }
        ],
    )

    return {
        "snapshot_label": snapshot_label,
        "raw_root": raw_root,
        "openalex_staged_files": len(openalex_files),
        "crossref_notice_rows": crossref_result["row_count"],
        "crossref_skipped": bool(crossref_result.get("skipped", False)),
        "retraction_watch_rows": retraction_watch_result["row_count"],
        "pubmed_staged_files": len(pubmed_files),
    }


def validate_vendor_archive(
    vendor_root: Path,
    snapshot_label: str,
    allow_missing_crossref: bool = False,
) -> Dict[str, object]:
    vendor_root = Path(vendor_root)
    period = freeze_period(snapshot_label)
    period_key = period["period_key"]
    freeze_end = period["freeze_end"]
    checks: List[dict] = []

    checks.append(
        _vendor_check(
            "openalex",
            vendor_root / "openalex" / period_key,
            required_any=(".gz",),
        )
    )
    crossref_check = _vendor_check(
        "crossref",
        vendor_root / "crossref" / period_key,
        required_exact=("all.json.tar.gz",),
    )
    if allow_missing_crossref and not crossref_check["passed"]:
        crossref_check = {
            **crossref_check,
            "passed": True,
            "required": False,
            "reason": "optional in open-data-only mode",
        }
    checks.append(crossref_check)
    checks.append(
        _vendor_check(
            "pubmed_baseline",
            vendor_root / "pubmed" / "baseline" / str(period["year"]),
            required_any=(".xml.gz", ".xml"),
        )
    )
    checks.append(
        _vendor_check(
            "pubmed_updatefiles",
            vendor_root / "pubmed" / "updatefiles" / period_key,
            required_any=(".xml.gz", ".xml"),
        )
    )

    rw_dir = _latest_retraction_watch_dir(vendor_root / "retraction_watch", freeze_end)
    if rw_dir is None:
        checks.append(
            {
                "label": "retraction_watch",
                "path": str(vendor_root / "retraction_watch"),
                "passed": False,
                "reason": "no dated directory found at or before freeze_end",
            }
        )
    else:
        checks.append(
            _vendor_check(
                "retraction_watch",
                rw_dir,
                required_any=(".csv", ".csv.gz"),
            )
        )

    return {
        "snapshot_label": snapshot_label,
        "freeze_end": freeze_end,
        "allow_missing_crossref": allow_missing_crossref,
        "passed": all(item["passed"] for item in checks),
        "checks": checks,
    }


def iter_crossref_member_records(payload: bytes, member_name: str) -> Iterator[dict]:
    member_bytes = payload
    if member_name.endswith(".gz"):
        member_bytes = gzip.decompress(member_bytes)
    text = member_bytes.decode("utf-8", errors="replace").strip()
    if not text:
        return
    if "\n" in text:
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                yield parsed
            elif isinstance(parsed, list):
                for item in parsed:
                    if isinstance(item, dict):
                        yield item
        return
    parsed = json.loads(text)
    if isinstance(parsed, dict):
        yield parsed
    elif isinstance(parsed, list):
        for item in parsed:
            if isinstance(item, dict):
                yield item


def _write_bucket_metadata(bucket_root: Path, snapshot_label: str, batches: List[dict]) -> None:
    bucket_root.mkdir(parents=True, exist_ok=True)
    source_versions_path = bucket_root / "source_versions.json"
    sha_manifest_path = bucket_root / "sha256_manifest.tsv"
    ingest_files = [
        path
        for path in sorted(bucket_root.rglob("*"))
        if path.is_file() and path.name not in {"source_versions.json", "sha256_manifest.tsv", "fetch.log"}
    ]
    write_json(
        source_versions_path,
        {
            "snapshot_label": snapshot_label,
            "generated_at": _utcnow_iso(),
            "batches": batches,
        },
    )
    _write_sha256_manifest(ingest_files + [source_versions_path], sha_manifest_path, bucket_root)


def _write_sha256_manifest(paths: Iterable[Path], output_path: Path, base_root: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows = ["relative_path\tsize_bytes\tsha256"]
    for path in sorted(Path(item) for item in paths):
        if not path.exists() or path == output_path:
            continue
        relative_path = str(path.relative_to(base_root))
        rows.append(
            "%s\t%s\t%s"
            % (
                relative_path,
                path.stat().st_size,
                hash_file_sha256(path),
            )
        )
    output_path.write_text("\n".join(rows) + "\n", encoding="utf-8")


def _stage_openalex_vendor(vendor_dir: Path, raw_dir: Path, mode: str) -> List[Path]:
    if not vendor_dir.exists():
        raise FileNotFoundError("Missing OpenAlex vendor archive under %s" % vendor_dir)
    staged = []
    for path in sorted(vendor_dir.rglob("*")):
        if not path.is_file():
            continue
        if path.name in {"manifest", "LICENSE.txt", "RELEASE_NOTES.txt"} or path.name.endswith(".gz"):
            destination = raw_dir / path.relative_to(vendor_dir)
            _copy_or_symlink(path, destination, mode)
            staged.append(destination)
    if not any(path.suffix == ".gz" for path in staged):
        raise FileNotFoundError("No OpenAlex .gz shards found under %s" % vendor_dir)
    return staged


def _stage_pubmed_vendor(baseline_dir: Path, update_dir: Path, raw_dir: Path, mode: str) -> List[Path]:
    staged = []
    for source_dir, prefix in ((baseline_dir, "baseline"), (update_dir, "updatefiles")):
        if not source_dir.exists():
            raise FileNotFoundError("Missing PubMed vendor archive under %s" % source_dir)
        for path in sorted(source_dir.rglob("*")):
            if not path.is_file():
                continue
            if not (path.name.endswith(".xml") or path.name.endswith(".xml.gz")):
                continue
            destination = raw_dir / prefix / path.relative_to(source_dir)
            _copy_or_symlink(path, destination, mode)
            staged.append(destination)
    return staged


def _copy_or_symlink(source: Path, destination: Path, mode: str) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() or destination.is_symlink():
        destination.unlink()
    if mode == "symlink":
        destination.symlink_to(source.resolve())
        return
    if source.suffix == ".gz":
        shutil.copy2(source, destination)
    else:
        shutil.copy2(source, destination)


def _vendor_check(
    label: str,
    path: Path,
    required_exact: Iterable[str] = (),
    required_any: Iterable[str] = (),
) -> dict:
    path = Path(path)
    if not path.exists():
        return {"label": label, "path": str(path), "passed": False, "reason": "missing_directory"}
    metadata_files = {path / "source_versions.json", path / "sha256_manifest.tsv"}
    missing_metadata = [str(item.name) for item in metadata_files if not item.exists()]
    if missing_metadata:
        return {
            "label": label,
            "path": str(path),
            "passed": False,
            "reason": "missing_metadata:%s" % ",".join(sorted(missing_metadata)),
        }
    names = {item.name for item in path.rglob("*") if item.is_file()}
    for item in required_exact:
        if item in names:
            return {"label": label, "path": str(path), "passed": True, "reason": "ok"}
    if required_any:
        for file_path in path.rglob("*"):
            if not file_path.is_file():
                continue
            if any(file_path.name.endswith(suffix) for suffix in required_any):
                return {"label": label, "path": str(path), "passed": True, "reason": "ok"}
    return {"label": label, "path": str(path), "passed": False, "reason": "missing_required_files"}


def _latest_retraction_watch_dir(root: Path, freeze_end: str) -> Optional[Path]:
    root = Path(root)
    if not root.exists():
        return None
    candidates = []
    future_candidates = []
    for path in root.iterdir():
        if not path.is_dir():
            continue
        try:
            date_value = parse_date(path.name)
        except ValueError:
            continue
        if str(date_value) <= freeze_end:
            candidates.append(path)
        else:
            future_candidates.append(path)
    if not candidates:
        if future_candidates:
            return sorted(future_candidates)[-1]
        return None
    return sorted(candidates)[-1]


def _find_first_file(root: Path, matches: Iterable[str]) -> Optional[Path]:
    root = Path(root)
    if not root.exists():
        return None
    exact_names = {item for item in matches if not item.startswith(".")}
    suffixes = tuple(item for item in matches if item.startswith("."))
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        if path.name in exact_names:
            return path
        if suffixes and any(path.name.endswith(suffix) for suffix in suffixes):
            return path
    return None


def _extract_retraction_watch_source_url(row: dict) -> str:
    urls = _first_nonempty(row.get("URLS"), row.get("URLs"), row.get("urls"))
    if urls:
        first_url = str(urls).split(";")[0].strip()
        if first_url:
            return first_url
    retraction_doi = _first_nonempty(
        row.get("RetractionDOI"),
        row.get("Retraction DOI"),
    )
    retraction_doi = str(retraction_doi or "").strip()
    if retraction_doi and retraction_doi.lower() not in {"unavailable", "na", "n/a", "0"}:
        return "https://doi.org/%s" % retraction_doi.lower()
    return ""


def _first_nonempty(*values):
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
