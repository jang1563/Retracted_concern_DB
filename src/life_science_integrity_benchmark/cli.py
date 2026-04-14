"""Command-line interface for the benchmark scaffold and ingest pipeline."""

import argparse
import os
from dataclasses import asdict
from pathlib import Path

from .adjudication import export_adjudication_pack
from .audit import build_leakage_report
from .baselines import (
    run_task_a_baselines,
    run_task_a_robustness,
    run_task_b_baseline,
    split_records_for_manifest,
)
from .constants import (
    MANIFEST_DB_PATH,
    NOTICE_COLLECTOR,
    OPENALEX_COLLECTOR,
    SNAPSHOT_DATE,
    SOURCE_FAMILY_OPENALEX_NOTICES,
)
from .dataset import (
    bootstrap_sample_sources,
    build_benchmark_records,
    build_release_summary,
    export_release_bundle,
    load_benchmark_records,
    load_source_bundle,
)
from .ingest import (
    ingest_snapshot,
    normalize_real_source_exports,
    register_snapshot,
    scaffold_real_source_layout,
)
from .manifest import ManifestStore
from .materialize import materialize_canonical_snapshot
from .reporting import build_experiment_report
from .site import build_site, export_internal_curation_queue
from .splits import build_split_manifests
from .utils import read_json, read_jsonl, write_json
from .validate import validate_snapshot
from .vendor_snapshot import (
    extract_crossref_official_notices,
    extract_retraction_watch_csv,
    stage_vendor_archive_to_raw_snapshot,
    validate_vendor_archive,
)


ROOT = Path.cwd()


def main(argv=None):
    parser = argparse.ArgumentParser(description="Life-science integrity benchmark CLI")
    parser.add_argument("--root-dir", default=os.environ.get("LSIB_ROOT_DIR"))
    parser.add_argument("--source-dir")
    parser.add_argument("--release-dir")
    parser.add_argument("--site-dir")
    parser.add_argument("--snapshot-date", default=SNAPSHOT_DATE)

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("bootstrap-sample")

    scaffold = subparsers.add_parser("scaffold-real-ingest")
    scaffold.add_argument("--raw-dir", required=True)

    register = subparsers.add_parser("register-snapshot")
    register.add_argument("--snapshot-id", required=True)
    register.add_argument("--raw-root", required=True)
    register.add_argument(
        "--source-family",
        default=SOURCE_FAMILY_OPENALEX_NOTICES,
    )

    ingest = subparsers.add_parser("ingest-snapshot")
    ingest.add_argument("--snapshot-id", required=True)
    ingest.add_argument("--collector", required=True)

    materialize = subparsers.add_parser("materialize-canonical")
    materialize.add_argument("--snapshot-id", required=True)

    validate = subparsers.add_parser("validate-snapshot")
    validate.add_argument("--snapshot-id", required=True)

    normalize = subparsers.add_parser("normalize-real-sources")
    normalize.add_argument("--raw-dir", required=True)
    normalize.add_argument("--normalized-dir", required=True)

    extract_crossref = subparsers.add_parser("extract-crossref-official-notices")
    extract_crossref.add_argument("--archive-path", required=True)
    extract_crossref.add_argument("--output-path", required=True)
    extract_crossref.add_argument("--snapshot-label", required=True)

    extract_rw = subparsers.add_parser("extract-retraction-watch-csv")
    extract_rw.add_argument("--source-csv", required=True)
    extract_rw.add_argument("--output-path", required=True)
    extract_rw.add_argument("--snapshot-label", required=True)

    stage_vendor = subparsers.add_parser("stage-vendor-archive")
    stage_vendor.add_argument("--vendor-root", required=True)
    stage_vendor.add_argument("--raw-root", required=True)
    stage_vendor.add_argument("--snapshot-label", required=True)
    stage_vendor.add_argument("--mode", default="copy", choices=["copy", "symlink"])
    stage_vendor.add_argument("--allow-missing-crossref", action="store_true")

    validate_vendor = subparsers.add_parser("validate-vendor-archive")
    validate_vendor.add_argument("--vendor-root", required=True)
    validate_vendor.add_argument("--snapshot-label", required=True)
    validate_vendor.add_argument("--allow-missing-crossref", action="store_true")

    subparsers.add_parser("build-core")
    subparsers.add_parser("build-splits")
    subparsers.add_parser("audit-leakage")

    task_a = subparsers.add_parser("train-task-a")
    task_a.add_argument("--text-backend", default="hashing")

    subparsers.add_parser("train-task-b")

    adjudication = subparsers.add_parser("make-adjudication-set")
    adjudication.add_argument("--sample-size", type=int, default=500)

    subparsers.add_parser("build-site")
    subparsers.add_parser("build-report")
    subparsers.add_parser("demo")

    args = parser.parse_args(argv)
    root_dir = Path(args.root_dir).resolve() if args.root_dir else ROOT
    source_dir = Path(args.source_dir) if args.source_dir else root_dir / "data" / "sources"
    release_dir = (
        Path(args.release_dir) if args.release_dir else root_dir / "artifacts" / "sample_release"
    )
    site_dir = Path(args.site_dir) if args.site_dir else root_dir / "artifacts" / "site"
    snapshot_date = args.snapshot_date

    if args.command == "bootstrap-sample":
        paths = bootstrap_sample_sources(source_dir)
        for name, path in paths.items():
            print("%s: %s" % (name, path))
        return

    if args.command == "scaffold-real-ingest":
        paths = scaffold_real_source_layout(Path(args.raw_dir))
        for name, path in paths.items():
            print("%s: %s" % (name, path))
        return

    if args.command == "register-snapshot":
        result = register_snapshot(
            snapshot_id=args.snapshot_id,
            raw_root=Path(args.raw_root),
            root_dir=root_dir,
            source_family=args.source_family,
            snapshot_date=snapshot_date,
        )
        print("snapshot_id:", result["snapshot_id"])
        print("registered_files:", result["registered_files"])
        return

    if args.command == "ingest-snapshot":
        result = ingest_snapshot(
            snapshot_id=args.snapshot_id,
            collector_name=args.collector,
            root_dir=root_dir,
        )
        print("snapshot_id:", result["snapshot_id"])
        print("collector:", result["collector"])
        print("processed_files:", result["processed_files"])
        return

    if args.command == "materialize-canonical":
        paths = materialize_canonical_snapshot(
            snapshot_id=args.snapshot_id,
            root_dir=root_dir,
            manifest=ManifestStore(root_dir / MANIFEST_DB_PATH),
        )
        for name, path in paths.items():
            print("%s: %s" % (name, path))
        return

    if args.command == "validate-snapshot":
        report = validate_snapshot(
            snapshot_id=args.snapshot_id,
            root_dir=root_dir,
            manifest=ManifestStore(root_dir / MANIFEST_DB_PATH),
        )
        path = release_dir / ("%s_validation.json" % args.snapshot_id)
        write_json(path, report)
        print("validation_report:", path)
        if not report["passed"]:
            raise SystemExit("Validation failed")
        return

    if args.command == "normalize-real-sources":
        paths = normalize_real_source_exports(Path(args.raw_dir), Path(args.normalized_dir))
        for name, path in paths.items():
            print("%s: %s" % (name, path))
        return

    if args.command == "extract-crossref-official-notices":
        result = extract_crossref_official_notices(
            archive_path=Path(args.archive_path),
            output_path=Path(args.output_path),
            snapshot_label=args.snapshot_label,
        )
        for name, value in result.items():
            print("%s: %s" % (name, value))
        return

    if args.command == "extract-retraction-watch-csv":
        result = extract_retraction_watch_csv(
            source_csv_path=Path(args.source_csv),
            output_path=Path(args.output_path),
            snapshot_label=args.snapshot_label,
        )
        for name, value in result.items():
            print("%s: %s" % (name, value))
        return

    if args.command == "stage-vendor-archive":
        result = stage_vendor_archive_to_raw_snapshot(
            vendor_root=Path(args.vendor_root),
            raw_root=Path(args.raw_root),
            snapshot_label=args.snapshot_label,
            mode=args.mode,
            allow_missing_crossref=args.allow_missing_crossref,
        )
        for name, value in result.items():
            print("%s: %s" % (name, value))
        return

    if args.command == "validate-vendor-archive":
        result = validate_vendor_archive(
            vendor_root=Path(args.vendor_root),
            snapshot_label=args.snapshot_label,
            allow_missing_crossref=args.allow_missing_crossref,
        )
        path = release_dir / ("%s_vendor_validation.json" % args.snapshot_label)
        write_json(path, result)
        print("vendor_validation_report:", path)
        if not result["passed"]:
            raise SystemExit("Vendor archive validation failed")
        return

    if args.command == "demo":
        bootstrap_sample_sources(source_dir)
        records = _build_core(source_dir, release_dir, snapshot_date)
        manifests = _build_splits(records, release_dir)
        _audit_leakage(records, release_dir, snapshot_date)
        _train_task_a(records, manifests, release_dir, text_backend="hashing")
        _train_task_b(records, release_dir)
        _make_adjudication_set(records, release_dir, sample_size=500)
        _build_site(records, release_dir, site_dir)
        _build_report(release_dir)
        print("Demo build complete.")
        return

    if args.command == "build-report":
        _build_report(release_dir)
        return

    if args.command == "build-core":
        _build_core(source_dir, release_dir, snapshot_date)
        return

    records = _load_records(release_dir, source_dir, snapshot_date)
    if args.command == "build-splits":
        _build_splits(records, release_dir)
    elif args.command == "audit-leakage":
        _audit_leakage(records, release_dir, snapshot_date)
    elif args.command == "train-task-a":
        manifests = _build_splits(records, release_dir)
        _train_task_a(records, manifests, release_dir, text_backend=args.text_backend)
    elif args.command == "train-task-b":
        _train_task_b(records, release_dir)
    elif args.command == "make-adjudication-set":
        _make_adjudication_set(records, release_dir, sample_size=args.sample_size)
    elif args.command == "build-site":
        _build_site(records, release_dir, site_dir)


def _load_records(release_dir: Path, source_dir: Path, snapshot_date: str):
    benchmark_path = release_dir / "benchmark_v1.jsonl"
    if benchmark_path.exists():
        return load_benchmark_records(read_jsonl(benchmark_path))
    return _build_core(source_dir, release_dir, snapshot_date)


def _build_core(source_dir: Path, release_dir: Path, snapshot_date: str):
    articles, notices, signals = load_source_bundle(source_dir)
    if not articles:
        raise SystemExit("No article sources found under %s" % source_dir)
    records = build_benchmark_records(articles, notices, signals, snapshot_date=snapshot_date)
    paths = export_release_bundle(records, release_dir)
    collection_summary_path = source_dir / "collection_summary.json"
    if collection_summary_path.exists():
        write_json(release_dir / "collection_summary.json", read_json(collection_summary_path))
    print("benchmark_jsonl:", paths["jsonl"])
    print("benchmark_csv:", paths["csv"])
    print("summary:", paths["summary"])
    return records


def _build_splits(records, release_dir: Path):
    manifests = build_split_manifests(records)
    payload = {name: asdict(manifest) for name, manifest in manifests.items()}
    path = release_dir / "splits.json"
    write_json(path, payload)
    print("splits:", path)
    return manifests


def _audit_leakage(records, release_dir: Path, snapshot_date: str):
    report = build_leakage_report(records, snapshot_date=snapshot_date)
    path = release_dir / "leakage_report.json"
    write_json(path, asdict(report))
    print("leakage_report:", path)
    return report


def _train_task_a(records, manifests, release_dir: Path, text_backend: str):
    outputs = {}
    for task_name in ("task_a_12m", "task_a_36m"):
        manifest = manifests[task_name]
        train_records, _, test_records = split_records_for_manifest(records, manifest)
        runs = run_task_a_baselines(
            train_records=train_records,
            test_records=test_records,
            horizon="12m" if task_name.endswith("12m") else "36m",
            text_backend=text_backend,
        )
        outputs[task_name] = [asdict(run) for run in runs]
    path = release_dir / "task_a_baselines.json"
    write_json(path, outputs)
    print("task_a_baselines:", path)

    robustness = run_task_a_robustness(records, manifests, text_backend=text_backend)
    robustness_payload = {
        split_name: [asdict(run) for run in runs]
        for split_name, runs in robustness.items()
    }
    robustness_path = release_dir / "task_a_robustness.json"
    write_json(robustness_path, robustness_payload)
    print("task_a_robustness:", robustness_path)

    return outputs


def _train_task_b(records, release_dir: Path):
    run = run_task_b_baseline(records)
    path = release_dir / "task_b_baseline.json"
    write_json(path, asdict(run))
    print("task_b_baseline:", path)
    return run


def _make_adjudication_set(records, release_dir: Path, sample_size: int):
    csv_path = release_dir / "adjudication_queue.csv"
    summary_path = release_dir / "adjudication_queue_summary.json"
    protocol_path = release_dir / "adjudication_protocol.md"
    result = export_adjudication_pack(
        records,
        csv_path,
        summary_path,
        sample_size=sample_size,
        protocol_path=protocol_path,
    )
    print("adjudication_csv:", result["csv"])
    print("adjudication_summary:", result["summary"])
    print("adjudication_protocol:", result["protocol"])
    return result


def _build_site(records, release_dir: Path, site_dir: Path):
    summary = build_release_summary(records)
    paths = build_site(records, site_dir, summary)
    queue_path = export_internal_curation_queue(
        records, release_dir / "internal_curation_queue.json"
    )
    for name, path in paths.items():
        print("%s: %s" % (name, path))
    print("internal_curation_queue:", queue_path)
    return paths


def _build_report(release_dir: Path):
    collection_summary = {}
    collection_summary_path = release_dir / "collection_summary.json"
    if collection_summary_path.exists():
        collection_summary = read_json(collection_summary_path)
    robustness_path = release_dir / "task_a_robustness.json"
    task_a_robustness = read_json(robustness_path) if robustness_path.exists() else {}
    report_paths = build_experiment_report(
        summary=read_json(release_dir / "summary.json"),
        splits=read_json(release_dir / "splits.json"),
        leakage_report=read_json(release_dir / "leakage_report.json"),
        task_a_baselines=read_json(release_dir / "task_a_baselines.json"),
        task_b_baseline=read_json(release_dir / "task_b_baseline.json"),
        markdown_path=release_dir / "experiment_report.md",
        json_path=release_dir / "experiment_report.json",
        ingest_summary=collection_summary,
        task_a_robustness=task_a_robustness,
    )
    for name, path in report_paths.items():
        print("%s: %s" % (name, path))
    return report_paths


if __name__ == "__main__":
    main()
