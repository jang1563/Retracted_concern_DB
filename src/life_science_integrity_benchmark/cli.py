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
    build_openalex_scope_allowlist,
    ingest_snapshot,
    normalize_real_source_exports,
    register_snapshot,
    scaffold_real_source_layout,
)
from .manifest import ManifestStore
from .materialize import materialize_canonical_snapshot
from .reporting import (
    build_calibration_svg,
    build_experiment_report,
    build_pr_curve_svg,
    build_results_v0_2_markdown,
    update_readme_for_v0_2,
)
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


def _build_ingest_progress_callback():
    heartbeat_path = os.environ.get("LSIB_STEP_HEARTBEAT_PATH")
    heartbeat_label = os.environ.get("LSIB_STEP_HEARTBEAT_LABEL", "ingest_snapshot")

    def callback(event: dict):
        message = _format_ingest_progress_event(event)
        if message:
            print(message, flush=True)
        heartbeat = _format_ingest_heartbeat(event, heartbeat_label)
        if heartbeat_path and heartbeat:
            heartbeat_target = Path(heartbeat_path)
            heartbeat_target.parent.mkdir(parents=True, exist_ok=True)
            heartbeat_target.write_text(heartbeat + "\n", encoding="utf-8")

    return callback


def _ingest_progress_every_seconds() -> float:
    raw_value = os.environ.get("LSIB_INGEST_PROGRESS_EVERY_SECONDS", "30")
    try:
        return float(raw_value)
    except ValueError:
        return 30.0


def _format_ingest_progress_event(event: dict) -> str:
    event_name = event.get("event")
    collector = event.get("collector", "unknown")
    file_index = event.get("file_index")
    total_files = event.get("total_files")
    relative_path = event.get("relative_path")
    file_pos = "%s/%s" % (file_index, total_files) if file_index and total_files else "n/a"
    if event_name == "start":
        return "ingest_progress: event=start collector=%s total_files=%s" % (
            collector,
            event.get("total_files", 0),
        )
    if event_name == "file_started":
        return "ingest_progress: event=file_started collector=%s file=%s path=%s" % (
            collector,
            file_pos,
            relative_path,
        )
    if event_name == "file_progress":
        return (
            "ingest_progress: event=file_progress collector=%s file=%s raw_records=%s "
            "normalized_rows=%s quarantined_rows=%s scope_skipped_rows=%s path=%s"
        ) % (
            collector,
            file_pos,
            event.get("raw_records_seen", 0),
            event.get("normalized_rows", 0),
            event.get("quarantined_rows", 0),
            event.get("scope_skipped_rows", 0),
            relative_path,
        )
    if event_name == "file_skipped":
        return "ingest_progress: event=file_skipped collector=%s file=%s path=%s" % (
            collector,
            file_pos,
            relative_path,
        )
    if event_name == "file_completed":
        return (
            "ingest_progress: event=file_completed collector=%s file=%s raw_records=%s "
            "normalized_rows=%s quarantined_rows=%s scope_skipped_rows=%s path=%s"
        ) % (
            collector,
            file_pos,
            event.get("raw_records_seen", 0),
            event.get("normalized_rows", 0),
            event.get("quarantined_rows", 0),
            event.get("scope_skipped_rows", 0),
            relative_path,
        )
    if event_name == "finished":
        return (
            "ingest_progress: event=finished collector=%s total_files=%s processed_files=%s "
            "skipped_files=%s normalized_rows=%s quarantined_rows=%s scope_skipped_rows=%s"
        ) % (
            collector,
            event.get("total_files", 0),
            event.get("processed_files", 0),
            event.get("skipped_files", 0),
            event.get("total_normalized_rows", 0),
            event.get("total_quarantined_rows", 0),
            event.get("total_scope_skipped_rows", 0),
        )
    if event_name == "failed":
        return "ingest_progress: event=failed collector=%s processed_files=%s skipped_files=%s" % (
            collector,
            event.get("processed_files", 0),
            event.get("skipped_files", 0),
        )
    return ""


def _format_ingest_heartbeat(event: dict, heartbeat_label: str) -> str:
    event_name = event.get("event")
    if event_name == "start":
        return "%s 0/%s started" % (heartbeat_label, event.get("total_files", 0))
    if event_name in {"file_started", "file_progress", "file_completed", "file_skipped"}:
        return (
            "%s %s/%s raw_records=%s normalized_rows=%s quarantined_rows=%s scope_skipped_rows=%s path=%s"
        ) % (
            heartbeat_label,
            event.get("file_index", 0),
            event.get("total_files", 0),
            event.get("raw_records_seen", 0),
            event.get("normalized_rows", 0),
            event.get("quarantined_rows", 0),
            event.get("scope_skipped_rows", 0),
            event.get("relative_path", ""),
        )
    if event_name == "finished":
        return (
            "%s completed processed=%s skipped=%s normalized_rows=%s quarantined_rows=%s scope_skipped_rows=%s"
        ) % (
            heartbeat_label,
            event.get("processed_files", 0),
            event.get("skipped_files", 0),
            event.get("total_normalized_rows", 0),
            event.get("total_quarantined_rows", 0),
            event.get("total_scope_skipped_rows", 0),
        )
    if event_name == "failed":
        return "%s failed processed=%s skipped=%s" % (
            heartbeat_label,
            event.get("processed_files", 0),
            event.get("skipped_files", 0),
        )
    return ""


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

    allowlist = subparsers.add_parser("build-openalex-scope-allowlist")
    allowlist.add_argument("--snapshot-id", required=True)
    allowlist.add_argument("--output-path", required=True)

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
    results_v02 = subparsers.add_parser("build-results-v0-2")
    results_v02.add_argument(
        "--run-root",
        default="/athena/masonlab/scratch/users/jak4013/lsib/20260410-overnight-rerun1",
    )
    results_v02.add_argument("--snapshot-label", default="2026-03-freeze")
    results_v02.add_argument("--snapshot-id", default="public_open_data_2026_03_freeze")
    results_v02.add_argument("--output-path")
    readme_v02 = subparsers.add_parser("build-readme-v0-2")
    readme_v02.add_argument("--output-path")
    readme_v02.add_argument("--results-doc-path", default="docs/results_v0.2.md")
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
            progress_callback=_build_ingest_progress_callback(),
            progress_every_seconds=_ingest_progress_every_seconds(),
        )
        print("snapshot_id:", result["snapshot_id"])
        print("collector:", result["collector"])
        print("total_files:", result["total_files"])
        print("processed_files:", result["processed_files"])
        print("skipped_files:", result["skipped_files"])
        return

    if args.command == "build-openalex-scope-allowlist":
        result = build_openalex_scope_allowlist(
            snapshot_id=args.snapshot_id,
            output_path=Path(args.output_path),
            root_dir=root_dir,
        )
        print("snapshot_id:", result["snapshot_id"])
        print("output_path:", result["output_path"])
        print("doi_count:", result["doi_count"])
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

    if args.command == "build-results-v0-2":
        output_path = (
            Path(args.output_path)
            if args.output_path
            else root_dir / "docs" / "results_v0.2.md"
        )
        _build_results_v0_2_doc(
            release_dir=release_dir,
            output_path=output_path,
            run_root=args.run_root,
            snapshot_label=args.snapshot_label,
            snapshot_id=args.snapshot_id,
        )
        return

    if args.command == "build-readme-v0-2":
        output_path = Path(args.output_path) if args.output_path else root_dir / "README.md"
        _build_readme_v0_2_doc(
            release_dir=release_dir,
            output_path=output_path,
            results_doc_path=args.results_doc_path,
        )
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
    release_collection_summary_path = release_dir / "collection_summary.json"
    if collection_summary_path.exists():
        write_json(release_collection_summary_path, read_json(collection_summary_path))
    else:
        release_collection_summary_path.unlink(missing_ok=True)
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

    svg_path = release_dir / "task_a_calibration_curves.svg"
    svg_path.write_text(build_calibration_svg(outputs), encoding="utf-8")
    print("task_a_calibration_curves:", svg_path)

    pr_svg_path = release_dir / "task_a_pr_curves.svg"
    pr_svg_path.write_text(build_pr_curve_svg(outputs), encoding="utf-8")
    print("task_a_pr_curves:", pr_svg_path)

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


def _build_results_v0_2_doc(
    release_dir: Path,
    output_path: Path,
    run_root: str,
    snapshot_label: str,
    snapshot_id: str,
):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    robustness_path = release_dir / "task_a_robustness.json"
    markdown = build_results_v0_2_markdown(
        summary=read_json(release_dir / "summary.json"),
        leakage_report=read_json(release_dir / "leakage_report.json"),
        task_a_baselines=read_json(release_dir / "task_a_baselines.json"),
        task_a_robustness=read_json(robustness_path) if robustness_path.exists() else {},
        task_b_baseline=read_json(release_dir / "task_b_baseline.json"),
        run_root=run_root,
        snapshot_label=snapshot_label,
        snapshot_id=snapshot_id,
    )
    output_path.write_text(markdown, encoding="utf-8")
    print("results_v0_2:", output_path)
    return output_path


def _build_readme_v0_2_doc(
    release_dir: Path,
    output_path: Path,
    results_doc_path: str,
):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    readme_text = output_path.read_text(encoding="utf-8")
    updated = update_readme_for_v0_2(
        readme_text=readme_text,
        summary=read_json(release_dir / "summary.json"),
        leakage_report=read_json(release_dir / "leakage_report.json"),
        task_a_baselines=read_json(release_dir / "task_a_baselines.json"),
        task_b_baseline=read_json(release_dir / "task_b_baseline.json"),
        results_doc_path=results_doc_path,
    )
    output_path.write_text(updated, encoding="utf-8")
    print("readme_v0_2:", output_path)
    return output_path


if __name__ == "__main__":
    main()
