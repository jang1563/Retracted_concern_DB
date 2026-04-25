import csv
import gzip
import os
import subprocess
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from life_science_integrity_benchmark.audit import build_leakage_report
from life_science_integrity_benchmark.dataset import (
    bootstrap_sample_sources,
    build_benchmark_records,
    build_release_summary,
    export_release_bundle,
    load_article_sources,
    load_benchmark_records,
    load_source_bundle,
    load_notice_sources,
    load_signal_sources,
)
from life_science_integrity_benchmark.ingest import (
    build_openalex_scope_allowlist,
    ingest_snapshot,
    normalize_real_source_exports,
    register_snapshot,
    scaffold_real_source_layout,
)
from life_science_integrity_benchmark.manifest import ManifestStore, SnapshotModifiedError
from life_science_integrity_benchmark.materialize import materialize_canonical_snapshot
from life_science_integrity_benchmark.reporting import (
    build_experiment_report,
    build_results_v0_2_markdown,
    update_readme_for_v0_2,
)
from life_science_integrity_benchmark.sample_data import SAMPLE_ARTICLES, SAMPLE_NOTICES, SAMPLE_SIGNALS
from life_science_integrity_benchmark.site import build_site, export_internal_curation_queue
from life_science_integrity_benchmark.splits import build_split_manifests
from life_science_integrity_benchmark.utils import (
    coerce_bool,
    read_json,
    read_jsonl,
    write_csv,
    write_json,
    write_jsonl,
)
from life_science_integrity_benchmark.validate import validate_snapshot
from life_science_integrity_benchmark.vendor_snapshot import (
    extract_crossref_official_notices,
    extract_retraction_watch_csv,
    stage_vendor_archive_to_raw_snapshot,
    validate_vendor_archive,
)

import io
import json
import tarfile


class BenchmarkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.repo_root = Path(__file__).resolve().parents[1]
        cls.cayuga_script_dir = cls.repo_root / "scripts" / "cayuga"
        cls.raw_snapshot_script_dir = cls.repo_root / "scripts" / "raw_snapshot"
        cls.python_bin = os.environ.get("TEST_PYTHON_BIN") or sys.executable

    def setUp(self):
        articles = load_article_sources(SAMPLE_ARTICLES)
        notices = load_notice_sources(SAMPLE_NOTICES)
        signals = load_signal_sources(SAMPLE_SIGNALS)
        self.records = build_benchmark_records(articles, notices, signals)

    def run_cayuga_script(self, script_name: str, *args: str, check: bool = True):
        return subprocess.run(
            ["/bin/bash", str(self.cayuga_script_dir / script_name), *args],
            cwd=self.repo_root,
            capture_output=True,
            text=True,
            check=check,
        )

    def run_raw_snapshot_script(self, script_name: str, *args: str, check: bool = True, env=None):
        return subprocess.run(
            ["/bin/bash", str(self.raw_snapshot_script_dir / script_name), *args],
            cwd=self.repo_root,
            capture_output=True,
            text=True,
            env=env,
            check=check,
        )

    def test_scope_filter_excludes_non_target_work_types(self):
        dois = {record.doi for record in self.records}
        self.assertNotIn("10.5555/lsib.2023.0097", dois)
        self.assertNotIn("10.5555/lsib.2024.0098", dois)
        self.assertNotIn("10.5555/lsib.2025.0099", dois)
        self.assertEqual(len(self.records), 16)

    def test_grouped_holdout_and_noisy_date_manifests(self):
        noisy_record = replace(
            self.records[0],
            doi="10.5555/lsib.2021.noisy",
            publication_date="2021-01-01",
            publication_year=2021,
            publication_date_precision="year_imputed",
            task_a_date_bucket="noisy_date",
            eligible_for_task_a_12m=False,
            eligible_for_task_a_36m=False,
        )
        manifests = build_split_manifests(self.records + [noisy_record])
        self.assertNotIn("task_a_12m_author_cluster_holdout", manifests)
        holdout = manifests["task_a_12m_venue_holdout"]
        by_doi = {record.doi: record for record in self.records + [noisy_record]}
        held_out_venues = {by_doi[doi].venue for doi in holdout.test_dois}
        overlap = {
            by_doi[doi].venue
            for doi in holdout.train_dois + holdout.val_dois
            if by_doi[doi].venue in held_out_venues
        }
        self.assertEqual(overlap, set())
        holdout_labels = {
            by_doi[doi].any_signal_or_notice_within_12m for doi in holdout.test_dois
        }
        self.assertEqual(holdout_labels, {False, True})
        self.assertIn("task_a_12m_noisy_date", manifests)
        self.assertIn("10.5555/lsib.2021.noisy", manifests["task_a_12m_noisy_date"].train_dois + manifests["task_a_12m_noisy_date"].val_dois + manifests["task_a_12m_noisy_date"].test_dois)

    def test_time_splits_do_not_duplicate_dois_for_sparse_years(self):
        def assert_disjoint(manifest):
            train = set(manifest.train_dois)
            val = set(manifest.val_dois)
            test = set(manifest.test_dois)
            self.assertEqual(train & val, set())
            self.assertEqual(train & test, set())
            self.assertEqual(val & test, set())

        one_year_records = [
            replace(
                self.records[0],
                doi="10.5555/lsib.sparse.1",
                publication_date="2021-01-01",
                publication_year=2021,
            ),
            replace(
                self.records[1],
                doi="10.5555/lsib.sparse.2",
                publication_date="2021-06-01",
                publication_year=2021,
            ),
        ]
        one_year_manifest = build_split_manifests(one_year_records)["task_a_12m"]
        assert_disjoint(one_year_manifest)
        self.assertEqual(one_year_manifest.test_dois, [])

        two_year_records = [
            replace(
                self.records[0],
                doi="10.5555/lsib.sparse.3",
                publication_date="2020-01-01",
                publication_year=2020,
            ),
            replace(
                self.records[1],
                doi="10.5555/lsib.sparse.4",
                publication_date="2021-01-01",
                publication_year=2021,
            ),
        ]
        two_year_manifest = build_split_manifests(two_year_records)["task_a_12m"]
        assert_disjoint(two_year_manifest)
        self.assertEqual(two_year_manifest.val_dois, [])
        self.assertEqual(two_year_manifest.train_dois, ["10.5555/lsib.sparse.3"])
        self.assertEqual(two_year_manifest.test_dois, ["10.5555/lsib.sparse.4"])

    def test_task_a_robustness_runs_on_every_grouped_holdout(self):
        from life_science_integrity_benchmark.baselines import run_task_a_robustness

        manifests = build_split_manifests(self.records)
        result = run_task_a_robustness(self.records, manifests, text_backend="hashing")

        # Primary time splits and statistically valid grouped holdouts must be
        # present; one-record author-cluster holdouts are intentionally skipped.
        # Noisy-date splits must NOT be in the robustness pass (they have their
        # own analysis split).
        expected_present = {
            "task_a_12m",
            "task_a_12m_venue_holdout",
            "task_a_12m_publisher_holdout",
            "task_a_36m",
            "task_a_36m_venue_holdout",
            "task_a_36m_publisher_holdout",
        }
        self.assertTrue(expected_present.issubset(result.keys()))
        self.assertNotIn("task_a_12m_author_cluster_holdout", result)
        self.assertNotIn("task_a_36m_author_cluster_holdout", result)
        self.assertNotIn("task_a_12m_noisy_date", result)
        self.assertNotIn("task_a_36m_noisy_date", result)
        self.assertNotIn("task_b", result)

        # On every non-empty split we should get three baseline runs
        # (metadata_logistic, abstract_encoder, fusion) with proper
        # per-split task names so downstream consumers can tell them
        # apart.
        for split_name, runs in result.items():
            if not runs:
                continue
            self.assertEqual(len(runs), 3)
            model_names = {r.model_name for r in runs}
            self.assertEqual(
                model_names,
                {
                    "metadata_logistic_baseline",
                    "abstract_encoder_baseline",
                    "metadata_text_fusion_baseline",
                },
            )
            for r in runs:
                self.assertEqual(r.task_name, split_name)
                self.assertIn("AUPRC", r.metrics)

    def test_leakage_audit_flags_future_censored_feature(self):
        bad_record = replace(
            self.records[0],
            author_history_cutoff_date="2018-12-31",
            publication_date="2018-02-15",
        )
        report = build_leakage_report([bad_record], snapshot_date="2026-04-09")
        self.assertFalse(report.passed)
        self.assertEqual(report.feature_cutoff_violations[0]["field"], "author_history_signal_count")

    # --- Leakage-audit sensitivity battery -----------------------------
    # Each of the tests below injects exactly one known-leaky pattern
    # into a clean record and asserts that the audit (a) fails overall
    # and (b) attributes the failure to the right check. These are the
    # "audit has teeth" tests — they prove the leakage_report.passed=True
    # in the demo is informative, not vacuous.

    def test_leakage_audit_passes_on_clean_records(self):
        # Baseline: on the unmodified sample corpus, the audit must PASS
        # and every violation list must be empty. Without this, any test
        # that checks `passed=False` on a mutated record is vacuously
        # satisfied.
        report = build_leakage_report(self.records, snapshot_date="2026-04-09")
        self.assertTrue(report.passed)
        self.assertEqual(report.leaked_fields_found, [])
        self.assertEqual(report.records_with_invalid_event_order, [])
        self.assertEqual(report.records_with_snapshot_violations, [])
        self.assertEqual(report.records_missing_feature_provenance, [])
        self.assertEqual(report.feature_cutoff_violations, [])

    def test_leakage_audit_flags_invalid_event_order(self):
        # first_signal_date or first_notice_date dated BEFORE publication
        # is logically impossible and must be caught. Covers both fields.
        signal_before_pub = replace(
            self.records[0],
            first_signal_date="2000-01-01",
            first_notice_date=None,
        )
        report = build_leakage_report([signal_before_pub], snapshot_date="2026-04-09")
        self.assertFalse(report.passed)
        self.assertIn(signal_before_pub.doi, report.records_with_invalid_event_order)

        notice_before_pub = replace(
            self.records[0],
            first_signal_date=None,
            first_notice_date="2000-01-01",
        )
        report = build_leakage_report([notice_before_pub], snapshot_date="2026-04-09")
        self.assertFalse(report.passed)
        self.assertIn(notice_before_pub.doi, report.records_with_invalid_event_order)

    def test_leakage_audit_flags_post_snapshot_events(self):
        # Post-publication events dated AFTER the declared snapshot must
        # be caught — otherwise the "as-of-snapshot" framing is violated
        # and the release contains future information.
        late_signal = replace(
            self.records[0],
            first_signal_date="2099-01-01",
            first_notice_date=None,
        )
        report = build_leakage_report([late_signal], snapshot_date="2026-04-09")
        self.assertFalse(report.passed)
        self.assertIn(late_signal.doi, report.records_with_snapshot_violations)

        late_notice = replace(
            self.records[0],
            first_signal_date=None,
            first_notice_date="2099-01-01",
        )
        report = build_leakage_report([late_notice], snapshot_date="2026-04-09")
        self.assertFalse(report.passed)
        self.assertIn(late_notice.doi, report.records_with_snapshot_violations)

    def test_leakage_audit_flags_record_snapshot_date_mismatch(self):
        bad_record = replace(self.records[0], snapshot_date="2026-04-08")
        report = build_leakage_report([bad_record], snapshot_date="2026-04-09")
        self.assertFalse(report.passed)
        self.assertIn(bad_record.doi, report.records_with_snapshot_violations)

    def test_leakage_audit_flags_wrong_task_a_feature_cutoff(self):
        # task_a_feature_cutoff_date must equal publication_date exactly;
        # any drift means Task A is seeing features censored at a
        # different-than-documented horizon.
        mismatched = replace(
            self.records[0],
            task_a_feature_cutoff_date="2020-01-01",
        )
        report = build_leakage_report([mismatched], snapshot_date="2026-04-09")
        self.assertFalse(report.passed)
        offending_fields = {v["field"] for v in report.feature_cutoff_violations}
        self.assertIn("task_a_feature_cutoff_date", offending_fields)

    def test_leakage_audit_flags_missing_provenance_fields(self):
        # Feature provenance is mandatory; missing any of the three
        # cutoff dates means the record cannot be evaluated under the
        # leakage contract.
        missing_task_cutoff = replace(self.records[0], task_a_feature_cutoff_date="")
        report = build_leakage_report([missing_task_cutoff], snapshot_date="2026-04-09")
        self.assertFalse(report.passed)
        self.assertIn(missing_task_cutoff.doi, report.records_missing_feature_provenance)

        missing_author_cutoff = replace(self.records[0], author_history_cutoff_date="")
        report = build_leakage_report([missing_author_cutoff], snapshot_date="2026-04-09")
        self.assertFalse(report.passed)
        self.assertIn(missing_author_cutoff.doi, report.records_missing_feature_provenance)

        missing_journal_cutoff = replace(self.records[0], journal_history_cutoff_date="")
        report = build_leakage_report([missing_journal_cutoff], snapshot_date="2026-04-09")
        self.assertFalse(report.passed)
        self.assertIn(missing_journal_cutoff.doi, report.records_missing_feature_provenance)

    def test_leakage_audit_flags_future_censored_journal_history(self):
        # Analog of the existing future-censored-author-history test,
        # extended to journal_history_cutoff_date so both censored
        # history fields are mechanically verified.
        bad_record = replace(
            self.records[0],
            journal_history_cutoff_date="2099-12-31",
            publication_date="2018-02-15",
        )
        report = build_leakage_report([bad_record], snapshot_date="2099-12-31")
        self.assertFalse(report.passed)
        offending_fields = {v["field"] for v in report.feature_cutoff_violations}
        self.assertIn("journal_history_signal_count", offending_fields)

    def test_release_reload_preserves_provenance(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            export_release_bundle(self.records, output_dir)
            reloaded = load_benchmark_records(read_jsonl(output_dir / "benchmark_v1.jsonl"))
            self.assertEqual(len(reloaded), len(self.records))
            self.assertEqual(
                reloaded[0].provenance[0].source_name,
                self.records[0].provenance[0].source_name,
            )

    def test_release_csv_includes_task_a_feature_columns(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            export_release_bundle(self.records, output_dir)
            with (output_dir / "benchmark_v1.csv").open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            csv_by_doi = {row["doi"]: row for row in rows}
            record = self.records[0]
            exported = csv_by_doi[record.doi]

            self.assertIn("abstract", exported)
            self.assertIn("is_pubmed_indexed", exported)
            self.assertIn("openalex_life_science_score", exported)
            self.assertEqual(exported["abstract"], record.abstract)
            self.assertEqual(exported["is_pubmed_indexed"], str(record.is_pubmed_indexed))
            self.assertEqual(
                exported["openalex_life_science_score"],
                str(record.openalex_life_science_score),
            )

    def test_public_site_hides_curator_only_signals_and_internal_queue_is_separate(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "site"
            summary = build_release_summary(self.records)
            build_site(self.records, output_dir, summary)
            public_page = (
                output_dir / "records" / "10-5555-lsib-2018-0001.html"
            ).read_text(encoding="utf-8")
            self.assertIn("Crossref Retraction Watch", public_page)
            self.assertNotIn("Problematic Paper Screener", public_page)
            self.assertFalse((output_dir / "curation_queue.html").exists())

            queue_path = export_internal_curation_queue(
                self.records, Path(tmpdir) / "internal_curation_queue.json"
            )
            queue = read_json(queue_path)
            self.assertTrue(any(item["doi"] == "10.5555/lsib.2020.0006" for item in queue))

    def test_site_build_removes_stale_public_record_pages(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "site"
            build_site(self.records, output_dir, build_release_summary(self.records))
            stale_page = output_dir / "records" / "stale-record.html"
            stale_page.write_text("old public page", encoding="utf-8")

            reduced_records = [
                record
                for record in self.records
                if record.doi != "10.5555/lsib.2018.0001"
            ]
            build_site(reduced_records, output_dir, build_release_summary(reduced_records))

            self.assertFalse(stale_page.exists())
            self.assertFalse(
                (output_dir / "records" / "10-5555-lsib-2018-0001.html").exists()
            )

    def test_public_site_sanitizes_non_http_source_links(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "site"
            record = replace(
                self.records[0],
                provenance=[
                    replace(
                        self.records[0].provenance[0],
                        source_url="javascript:alert(1)",
                    )
                ],
            )
            build_site([record], output_dir, build_release_summary([record]))

            page = next((output_dir / "records").glob("*.html")).read_text(encoding="utf-8")
            self.assertNotIn("javascript:alert", page)
            self.assertIn('href="#"', page)

    def test_adjudication_rows_distinguish_none_known_from_curator_review(self):
        from life_science_integrity_benchmark.adjudication import (
            build_adjudication_rows,
            export_adjudication_pack,
        )

        rows = build_adjudication_rows(self.records, sample_size=len(self.records))
        rows_by_doi = {row["doi"]: row for row in rows}
        official = next(record for record in self.records if record.auto_publish)
        curator = next(record for record in self.records if record.curator_review_required)
        none_known = next(
            record
            for record in self.records
            if not record.auto_publish and not record.curator_review_required
        )

        self.assertEqual(
            rows_by_doi[official.doi]["public_release_eligibility"],
            "official_notice",
        )
        self.assertEqual(
            rows_by_doi[curator.doi]["public_release_eligibility"],
            "curator_review",
        )
        self.assertEqual(
            rows_by_doi[none_known.doi]["public_release_eligibility"],
            "none_known_at_snapshot",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            export_adjudication_pack(
                self.records,
                csv_path=root / "adjudication_queue.csv",
                summary_path=root / "adjudication_queue_summary.json",
                sample_size=3,
                protocol_path=root / "adjudication_protocol.md",
            )
            protocol = (root / "adjudication_protocol.md").read_text(encoding="utf-8")
            self.assertIn("none_known_at_snapshot", protocol)
            self.assertIn("background/negative record", protocol)

    def test_github_issue_templates_route_sensitive_reports_out_of_public_issues(self):
        config = (self.repo_root / ".github" / "ISSUE_TEMPLATE" / "config.yml").read_text(
            encoding="utf-8"
        )
        bug_template = (
            self.repo_root / ".github" / "ISSUE_TEMPLATE" / "bug_report.yml"
        ).read_text(encoding="utf-8")
        source_template = (
            self.repo_root / ".github" / "ISSUE_TEMPLATE" / "new_signal_source.yml"
        ).read_text(encoding="utf-8")
        governance_policy = (self.repo_root / "docs" / "governance_policy.md").read_text(
            encoding="utf-8"
        )
        contributing = (self.repo_root / "CONTRIBUTING.md").read_text(encoding="utf-8")

        self.assertIn("blank_issues_enabled: false", config)
        self.assertIn("record-level integrity dispute", bug_template)
        self.assertIn("SECURITY.md", bug_template)
        self.assertIn("rights-safe metadata/links only", source_template)
        self.assertIn("no private material or unreviewed allegations", source_template)
        self.assertIn("Public GitHub issues are not the record-dispute channel", governance_policy)
        self.assertIn("SECURITY.md", governance_policy)
        self.assertIn("rights-safe public metadata or links", contributing)

    def test_ci_workflow_verifies_release_artifact_files(self):
        workflow = (self.repo_root / ".github" / "workflows" / "ci.yml").read_text(
            encoding="utf-8"
        )

        for required_path in [
            "artifacts/sample_release/benchmark_v1.jsonl",
            "artifacts/sample_release/benchmark_v1.csv",
            "artifacts/sample_release/adjudication_queue.csv",
            "artifacts/sample_release/adjudication_queue_summary.json",
            "artifacts/sample_release/adjudication_protocol.md",
            "artifacts/sample_release/internal_curation_queue.json",
        ]:
            self.assertIn("test -f %s" % required_path, workflow)

    def test_full_snapshot_ingest_materialize_and_build_core(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            raw_root = root / "data" / "raw" / "snapshot_alpha"
            (raw_root / "openalex").mkdir(parents=True)
            (raw_root / "official_notices").mkdir(parents=True)
            (raw_root / "pubmed").mkdir(parents=True)

            write_jsonl(
                raw_root / "openalex" / "works.jsonl.gz",
                [
                    {
                        "doi": "10.2000/test.0",
                        "title": "Earlier flagged article",
                        "abstract": "Earlier wet-lab paper",
                        "publication_date": "2021-05-01",
                        "type": "article",
                        "concepts": [{"display_name": "Biology", "score": 0.92}],
                        "authorships": [
                            {
                                "author": {"display_name": "Alex Example"},
                                "institutions": [{"display_name": "Example Institute"}],
                            }
                        ],
                        "host_venue": {"display_name": "Example Journal"},
                        "publisher": "Example Publisher",
                        "referenced_works_count": 12,
                        "is_oa": True,
                    },
                    {
                        "doi": "https://doi.org/10.2000/test.1",
                        "display_name": "Duplicate weak row",
                        "publication_year": 2023,
                        "type": "article",
                        "concepts": [{"display_name": "Biology", "score": 0.95}],
                        "authorships": [
                            {
                                "author": {"display_name": "Alex Example"},
                                "institutions": [{"display_name": "Example Institute"}],
                            }
                        ],
                        "host_venue": {"display_name": "Example Journal"},
                        "publisher": "Example Publisher",
                        "referenced_works_count": 20,
                    },
                    {
                        "doi": "doi:10.2000/test.1",
                        "display_name": "Follow-up flagged paper",
                        "abstract": "Follow up proteomics benchmark",
                        "publication_date": "2023-07-05",
                        "type": "article",
                        "concepts": [{"display_name": "Biology", "score": 0.94}],
                        "authorships": [
                            {
                                "author": {"display_name": "Alex Example"},
                                "institutions": [{"display_name": "Example Institute"}],
                            }
                        ],
                        "host_venue": {"display_name": "Example Journal"},
                        "publisher": "Example Publisher",
                        "referenced_works_count": 22,
                        "is_oa": True,
                    },
                    {
                        "ids": {"doi": "https://doi.org/10.2000/test.2"},
                        "display_name": "Year-only bioinformatics row",
                        "abstract": "Genome graph benchmark",
                        "publication_year": 2021,
                        "type_crossref": "journal-article",
                        "primary_topic": {
                            "subfield": {"display_name": "Bioinformatics"},
                            "field": {"display_name": "Computer Science"},
                            "domain": {"display_name": "Life Sciences"},
                        },
                        "concepts": [{"display_name": "Bioinformatics", "score": 0.91}],
                        "authorships": [
                            {
                                "author": {"display_name": "Robin Example"},
                                "institutions": [{"display_name": "Omics Center"}],
                            }
                        ],
                        "primary_location": {
                            "source": {
                                "display_name": "Genome Pipeline Notes",
                                "host_organization_name": "Blue Oak Journals",
                            }
                        },
                        "referenced_works_count": 8,
                        "open_access": {"is_oa": True, "oa_status": "gold"},
                    },
                    {
                        "ids": {"doi": "https://doi.org/10.2000/test.3"},
                        "display_name": "Low-score nested ids paper",
                        "abstract_inverted_index": {
                            "clinical": [0],
                            "proteomics": [1],
                            "benchmark": [2],
                        },
                        "publication_year": 2022,
                        "type_crossref": "journal-article",
                        "concepts": [{"display_name": "Chemistry", "score": 0.12}],
                        "authorships": [
                            {
                                "author": {"display_name": "Casey Clinician"},
                                "institutions": [{"display_name": "Metro Medical Center"}],
                            }
                        ],
                        "locations": [
                            {
                                "source": {
                                    "display_name": "Clinical Biomarker Systems",
                                    "host_organization_name": "Summit Academic",
                                }
                            }
                        ],
                        "open_access": {"is_oa": True, "oa_status": "gold"},
                    },
                    {
                        "title": "Missing DOI row",
                        "publication_year": 2020,
                        "type": "article",
                    },
                ],
            )
            write_csv(
                raw_root / "official_notices" / "notices.csv.gz",
                rows=[
                    {
                        "doi": "10.2000/test.0",
                        "notice_type": "retraction",
                        "notice_date": "2022-06-01",
                        "source_name": "Crossmark",
                        "source_url": "https://example.org/retract0",
                    },
                    {
                        "doi": "10.2000/test.1",
                        "notice_type": "expression of concern",
                        "notice_date": "2024-01-01",
                        "source_name": "Crossmark",
                        "source_url": "https://example.org/eoc1",
                    },
                    {
                        "doi": "10.9999/orphan",
                        "notice_type": "retraction",
                        "notice_date": "2024-03-01",
                        "source_name": "Crossmark",
                        "source_url": "https://example.org/orphan",
                    },
                    {
                        "doi": "10.2000/test.2",
                        "notice_type": "weirdflag",
                        "notice_date": "2024-02-02",
                        "source_name": "Crossmark",
                        "source_url": "https://example.org/weird",
                    },
                ],
                fieldnames=["doi", "notice_type", "notice_date", "source_name", "source_url"],
            )
            (raw_root / "pubmed" / "pubmed.xml").write_text(
                """<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation>
      <PMID>11111111</PMID>
      <Article>
        <Journal><Title>Clinical Biomarker Systems</Title></Journal>
        <ELocationID EIdType="doi">10.2000/test.3</ELocationID>
        <PublicationTypeList><PublicationType>Journal Article</PublicationType></PublicationTypeList>
      </Article>
      <MeshHeadingList>
        <MeshHeading><DescriptorName>Biomarkers</DescriptorName></MeshHeading>
        <MeshHeading><DescriptorName>Patients</DescriptorName></MeshHeading>
      </MeshHeadingList>
      <KeywordList><Keyword>clinical proteomics</Keyword></KeywordList>
    </MedlineCitation>
    <PubmedData>
      <ArticleIdList><ArticleId IdType="doi">10.2000/test.3</ArticleId></ArticleIdList>
    </PubmedData>
  </PubmedArticle>
</PubmedArticleSet>
""",
                encoding="utf-8",
            )

            register_snapshot(
                snapshot_id="snapshot_alpha",
                raw_root=raw_root,
                root_dir=root,
                snapshot_date="2026-04-09",
            )
            ingest_snapshot("snapshot_alpha", "openalex_bulk", root_dir=root)
            ingest_snapshot("snapshot_alpha", "local_notice_export", root_dir=root)
            ingest_snapshot("snapshot_alpha", "pubmed_index", root_dir=root)
            paths = materialize_canonical_snapshot(
                "snapshot_alpha",
                root_dir=root,
                manifest=ManifestStore(root / "data" / "manifests" / "ingest.sqlite3"),
            )
            validation = validate_snapshot(
                "snapshot_alpha",
                root_dir=root,
                manifest=ManifestStore(root / "data" / "manifests" / "ingest.sqlite3"),
            )
            self.assertTrue(validation["passed"])

            summary = read_json(paths["collection_summary"])
            self.assertEqual(summary["duplicate_doi_count"], 1)
            self.assertEqual(summary["orphan_notice_count"], 1)
            self.assertEqual(summary["pubmed_join_count"], 1)
            self.assertIn("openalex_bulk:missing_doi", summary["quarantine_counts_by_error_code"])
            self.assertIn("local_notice_export:unknown_notice_type", summary["quarantine_counts_by_error_code"])

            canonical_articles = read_jsonl(paths["articles_dir"] / "part-00000.jsonl.gz")
            by_doi = {row["doi"]: row for row in canonical_articles}
            self.assertEqual(by_doi["10.2000/test.1"]["publication_date"], "2023-07-05")
            self.assertEqual(by_doi["10.2000/test.1"]["author_history_signal_count"], 1)
            self.assertEqual(by_doi["10.2000/test.2"]["task_a_date_bucket"], "noisy_date")
            self.assertTrue(by_doi["10.2000/test.3"]["is_pubmed_indexed"])
            self.assertEqual(by_doi["10.2000/test.3"]["pmid"], "11111111")
            self.assertEqual(by_doi["10.2000/test.3"]["subfield"], "biomedicine")

            articles, notices, signals = load_source_bundle(paths["canonical_root"])
            records = build_benchmark_records(articles, notices, signals, snapshot_date="2026-04-09")
            record_by_doi = {record.doi: record for record in records}
            self.assertEqual(record_by_doi["10.2000/test.1"].notice_status, "editorial_notice")
            self.assertFalse(record_by_doi["10.2000/test.2"].eligible_for_task_a_12m)
            self.assertIn("10.2000/test.3", record_by_doi)
            self.assertTrue(record_by_doi["10.2000/test.3"].is_pubmed_indexed)

            manifests = build_split_manifests(records)
            noisy_manifest = manifests["task_a_12m_noisy_date"]
            self.assertIn(
                "10.2000/test.2",
                noisy_manifest.train_dois + noisy_manifest.val_dois + noisy_manifest.test_dois,
            )

    def test_validate_snapshot_rejects_article_publication_after_snapshot(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            raw_root = root / "data" / "raw" / "snapshot_future_article"
            (raw_root / "openalex").mkdir(parents=True)
            write_jsonl(
                raw_root / "openalex" / "works.jsonl.gz",
                [
                    {
                        "doi": "10.2200/future.article",
                        "title": "Post-snapshot article",
                        "abstract": "This row should not be canonical for the freeze.",
                        "publication_date": "2026-04-10",
                        "type": "article",
                        "concepts": [{"display_name": "Biology", "score": 0.9}],
                        "authorships": [],
                    }
                ],
            )

            register_snapshot(
                snapshot_id="snapshot_future_article",
                raw_root=raw_root,
                root_dir=root,
                snapshot_date="2026-04-09",
            )
            ingest_snapshot("snapshot_future_article", "openalex_bulk", root_dir=root)
            manifest = ManifestStore(root / "data" / "manifests" / "ingest.sqlite3")
            materialize_canonical_snapshot(
                "snapshot_future_article",
                root_dir=root,
                manifest=manifest,
            )

            validation = validate_snapshot(
                "snapshot_future_article",
                root_dir=root,
                manifest=manifest,
            )
            self.assertFalse(validation["passed"])
            self.assertIn(
                "article publication date later than snapshot: 10.2200/future.article",
                validation["violations"],
            )

    def test_validate_snapshot_checks_collection_summary_counts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            raw_root = root / "data" / "raw" / "snapshot_summary_counts"
            (raw_root / "openalex").mkdir(parents=True)
            write_jsonl(
                raw_root / "openalex" / "works.jsonl.gz",
                [
                    {
                        "doi": "10.2200/summary.article",
                        "title": "Summary count article",
                        "abstract": "Canonical summary counts must match shards.",
                        "publication_date": "2024-04-10",
                        "type": "article",
                        "concepts": [{"display_name": "Biology", "score": 0.9}],
                        "authorships": [],
                    }
                ],
            )

            register_snapshot(
                snapshot_id="snapshot_summary_counts",
                raw_root=raw_root,
                root_dir=root,
                snapshot_date="2026-04-09",
            )
            ingest_snapshot("snapshot_summary_counts", "openalex_bulk", root_dir=root)
            manifest = ManifestStore(root / "data" / "manifests" / "ingest.sqlite3")
            paths = materialize_canonical_snapshot(
                "snapshot_summary_counts",
                root_dir=root,
                manifest=manifest,
            )
            summary = read_json(paths["collection_summary"])
            summary["canonical_article_count"] = 2
            summary["canonical_notice_count"] = 1
            summary["orphan_notice_count"] = 1
            write_json(paths["collection_summary"], summary)

            validation = validate_snapshot(
                "snapshot_summary_counts",
                root_dir=root,
                manifest=manifest,
            )
            self.assertFalse(validation["passed"])
            self.assertIn(
                "collection summary canonical_article_count mismatch: summary=2 actual=1",
                validation["violations"],
            )
            self.assertIn(
                "collection summary canonical_notice_count mismatch: summary=1 actual=0",
                validation["violations"],
            )
            self.assertIn(
                "collection summary orphan_notice_count mismatch: summary=1 actual=0",
                validation["violations"],
            )

    def test_materialize_shard_writer_removes_stale_shards(self):
        from life_science_integrity_benchmark.materialize import _write_shards

        with tempfile.TemporaryDirectory() as tmpdir:
            shard_dir = Path(tmpdir) / "articles"
            write_jsonl(shard_dir / "part-00001.jsonl.gz", [{"doi": "stale"}])

            _write_shards(shard_dir, [{"doi": "fresh"}], "part")

            self.assertFalse((shard_dir / "part-00001.jsonl.gz").exists())
            self.assertEqual(read_jsonl(shard_dir / "part-00000.jsonl.gz"), [{"doi": "fresh"}])

    def test_materialize_replaces_stale_manifest_artifact_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            raw_root = root / "data" / "raw" / "snapshot_manifest_artifacts"
            (raw_root / "openalex").mkdir(parents=True)
            write_jsonl(
                raw_root / "openalex" / "works.jsonl.gz",
                [
                    {
                        "doi": "10.2300/manifest.artifact",
                        "title": "Manifest artifact row",
                        "abstract": "Manifest artifacts should mirror current shards.",
                        "publication_date": "2024-02-02",
                        "type": "article",
                        "concepts": [{"display_name": "Biology", "score": 0.9}],
                        "authorships": [],
                    }
                ],
            )
            register_snapshot(
                snapshot_id="snapshot_manifest_artifacts",
                raw_root=raw_root,
                root_dir=root,
                snapshot_date="2026-04-09",
            )
            ingest_snapshot("snapshot_manifest_artifacts", "openalex_bulk", root_dir=root)
            store = ManifestStore(root / "data" / "manifests" / "ingest.sqlite3")
            materialize_canonical_snapshot(
                "snapshot_manifest_artifacts",
                root_dir=root,
                manifest=store,
            )
            stale_relative_path = (
                "data/normalized/snapshot_manifest_artifacts/"
                "canonical/articles/part-99999.jsonl.gz"
            )
            store.upsert_artifact(
                "snapshot_manifest_artifacts",
                "canonical_articles",
                stale_relative_path,
                999,
            )

            materialize_canonical_snapshot(
                "snapshot_manifest_artifacts",
                root_dir=root,
                manifest=store,
            )

            artifact_paths = [
                row["relative_path"]
                for row in store.list_artifacts(
                    "snapshot_manifest_artifacts",
                    "canonical_articles",
                )
            ]
            self.assertNotIn(stale_relative_path, artifact_paths)
            self.assertEqual(len(artifact_paths), 1)
            self.assertTrue(artifact_paths[0].endswith("canonical/articles/part-00000.jsonl.gz"))

    def test_notice_provider_adapters_extract_target_doi(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            raw_root = root / "data" / "raw" / "snapshot_notice_adapter"
            (raw_root / "openalex").mkdir(parents=True)
            (raw_root / "official_notices").mkdir(parents=True)

            write_jsonl(
                raw_root / "openalex" / "works.jsonl.gz",
                [
                    {
                        "doi": "10.2100/article.crossref",
                        "title": "Crossref target article",
                        "abstract": "Article for relation-based notice extraction",
                        "publication_date": "2022-01-15",
                        "type": "article",
                        "concepts": [{"display_name": "Biology", "score": 0.9}],
                        "authorships": [],
                    },
                    {
                        "doi": "10.2100/article.crossmark",
                        "title": "Crossmark target article",
                        "abstract": "Article for update-to extraction",
                        "publication_date": "2023-02-10",
                        "type": "article",
                        "concepts": [{"display_name": "Biology", "score": 0.9}],
                        "authorships": [],
                    },
                ],
            )
            write_jsonl(
                raw_root / "official_notices" / "provider_notices.jsonl.gz",
                [
                    {
                        "DOI": "10.9999/notice.retraction",
                        "type": "retraction",
                        "relation": {
                            "is-retraction-of": [
                                {"id": "10.2100/article.crossref", "id-type": "doi"}
                            ]
                        },
                        "created": {"date-parts": [[2024, 5, 20]]},
                        "URL": "https://doi.org/10.9999/notice.retraction",
                    },
                    {
                        "DOI": "10.2100/article.crossmark",
                        "update-to": [
                            {
                                "label": "Expression of Concern",
                                "DOI": "10.9999/notice.concern",
                                "updated": "2025-01-02",
                            }
                        ],
                        "URL": "https://doi.org/10.2100/article.crossmark",
                    },
                ],
            )

            register_snapshot(
                snapshot_id="snapshot_notice_adapter",
                raw_root=raw_root,
                root_dir=root,
                snapshot_date="2026-04-09",
            )
            ingest_snapshot("snapshot_notice_adapter", "openalex_bulk", root_dir=root)
            ingest_snapshot("snapshot_notice_adapter", "local_notice_export", root_dir=root)
            paths = materialize_canonical_snapshot(
                "snapshot_notice_adapter",
                root_dir=root,
                manifest=ManifestStore(root / "data" / "manifests" / "ingest.sqlite3"),
            )
            notices = read_jsonl(paths["official_notices_dir"] / "part-00000.jsonl.gz")
            by_key = {(row["doi"], row["notice_type"]): row for row in notices}
            self.assertIn(("10.2100/article.crossref", "retraction"), by_key)
            self.assertIn(("10.2100/article.crossmark", "expression_of_concern"), by_key)
            self.assertEqual(
                by_key[("10.2100/article.crossref", "retraction")]["notice_date"],
                "2024-05-20",
            )
            self.assertEqual(
                by_key[("10.2100/article.crossmark", "expression_of_concern")]["source_url"],
                "https://doi.org/10.9999/notice.concern",
            )
            self.assertEqual(
                by_key[("10.2100/article.crossref", "retraction")]["source_name"],
                "Crossref/Crossmark Export",
            )

    def test_snapshot_rerun_is_idempotent_and_mutation_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            raw_root = root / "data" / "raw" / "snapshot_beta"
            (raw_root / "openalex").mkdir(parents=True)
            (raw_root / "official_notices").mkdir(parents=True)
            write_jsonl(
                raw_root / "openalex" / "works.jsonl.gz",
                [
                    {
                        "doi": "10.3000/test.1",
                        "title": "Stable row",
                        "abstract": "Stable abstract",
                        "publication_date": "2022-02-02",
                        "type": "article",
                        "concepts": [{"display_name": "Biology", "score": 0.9}],
                        "authorships": [],
                    }
                ],
            )
            register_snapshot(
                snapshot_id="snapshot_beta",
                raw_root=raw_root,
                root_dir=root,
                snapshot_date="2026-04-09",
            )
            first = ingest_snapshot("snapshot_beta", "openalex_bulk", root_dir=root)
            second = ingest_snapshot("snapshot_beta", "openalex_bulk", root_dir=root)
            self.assertEqual(first["processed_files"], 1)
            self.assertEqual(second["processed_files"], 0)

            write_jsonl(
                raw_root / "openalex" / "extra.jsonl.gz",
                [
                    {
                        "doi": "10.3000/test.extra",
                        "title": "Late-added row",
                        "abstract": "Should invalidate the frozen snapshot",
                        "publication_date": "2022-03-03",
                        "type": "article",
                        "concepts": [{"display_name": "Biology", "score": 0.9}],
                        "authorships": [],
                    }
                ],
            )
            with self.assertRaisesRegex(SnapshotModifiedError, "added file"):
                ingest_snapshot("snapshot_beta", "openalex_bulk", root_dir=root)
            (raw_root / "openalex" / "extra.jsonl.gz").unlink()

            write_jsonl(
                raw_root / "openalex" / "works.jsonl.gz",
                [
                    {
                        "doi": "10.3000/test.1",
                        "title": "Modified row",
                        "abstract": "Changed",
                        "publication_date": "2022-02-02",
                        "type": "article",
                        "concepts": [{"display_name": "Biology", "score": 0.9}],
                        "authorships": [],
                    }
                ],
            )
            with self.assertRaises(SnapshotModifiedError):
                ingest_snapshot("snapshot_beta", "openalex_bulk", root_dir=root)

    def test_cli_ingest_snapshot_emits_progress_and_updates_heartbeat(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            raw_root = root / "data" / "raw" / "snapshot_progress"
            heartbeat_path = root / "heartbeat.txt"
            scaffold_real_source_layout(raw_root)

            register_snapshot(
                snapshot_id="snapshot_progress",
                raw_root=raw_root,
                root_dir=root,
                snapshot_date="2026-04-09",
            )

            env = os.environ.copy()
            env["PYTHONPATH"] = str(self.repo_root / "src")
            env["LSIB_STEP_HEARTBEAT_PATH"] = str(heartbeat_path)
            env["LSIB_STEP_HEARTBEAT_LABEL"] = "ingest_openalex"
            env["LSIB_INGEST_PROGRESS_EVERY_SECONDS"] = "0"

            result = subprocess.run(
                [
                    self.python_bin,
                    "-m",
                    "life_science_integrity_benchmark.cli",
                    "--root-dir",
                    str(root),
                    "ingest-snapshot",
                    "--snapshot-id",
                    "snapshot_progress",
                    "--collector",
                    "openalex_bulk",
                ],
                cwd=self.repo_root,
                env=env,
                capture_output=True,
                text=True,
                check=True,
            )

            self.assertIn("ingest_progress: event=start collector=openalex_bulk total_files=1", result.stdout)
            self.assertIn("ingest_progress: event=file_started collector=openalex_bulk file=1/1", result.stdout)
            self.assertIn("ingest_progress: event=file_progress collector=openalex_bulk file=1/1", result.stdout)
            self.assertIn("ingest_progress: event=file_completed collector=openalex_bulk file=1/1", result.stdout)
            self.assertIn("ingest_progress: event=finished collector=openalex_bulk total_files=1", result.stdout)
            self.assertIn("total_files: 1", result.stdout)
            self.assertIn("processed_files: 1", result.stdout)
            self.assertIn("skipped_files: 0", result.stdout)

            heartbeat_text = heartbeat_path.read_text(encoding="utf-8")
            self.assertIn("ingest_openalex completed processed=1 skipped=0", heartbeat_text)

    def test_cli_build_core_removes_stale_collection_summary_when_source_has_none(self):
        from life_science_integrity_benchmark.cli import _build_core

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source_dir = root / "data" / "sources"
            release_dir = root / "artifacts" / "sample_release"
            bootstrap_sample_sources(source_dir)
            write_json(
                release_dir / "collection_summary.json",
                {"snapshot_id": "stale-real-ingest", "canonical_article_count": 999},
            )

            _build_core(source_dir, release_dir, snapshot_date="2026-04-09")

            self.assertFalse((release_dir / "collection_summary.json").exists())
            self.assertTrue((release_dir / "summary.json").exists())

    def test_malformed_json_is_quarantined_without_blocking_other_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            raw_root = root / "data" / "raw" / "snapshot_gamma"
            scaffold_real_source_layout(raw_root)
            openalex_path = raw_root / "openalex" / "broken.jsonl"
            openalex_path.write_text(
                '{"doi":"10.4000/test.1","title":"Good row","abstract":"A","publication_date":"2021-01-01","type":"article","concepts":[{"display_name":"Biology","score":0.9}],"authorships":[]}\n'
                '{"doi": "10.4000/test.2", bad json\n',
                encoding="utf-8",
            )
            register_snapshot(
                snapshot_id="snapshot_gamma",
                raw_root=raw_root,
                root_dir=root,
                snapshot_date="2026-04-09",
            )
            ingest_snapshot("snapshot_gamma", "openalex_bulk", root_dir=root)
            store = ManifestStore(root / "data" / "manifests" / "ingest.sqlite3")
            file_rows = store.list_files("snapshot_gamma", "openalex_bulk")
            broken_row = next(row for row in file_rows if row["relative_path"].endswith("broken.jsonl"))
            self.assertEqual(int(broken_row["parsed_rows"]), 1)
            self.assertEqual(int(broken_row["quarantined_rows"]), 1)

    def test_bad_openalex_numeric_is_quarantined_without_blocking_other_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            raw_root = root / "data" / "raw" / "snapshot_bad_numeric"
            scaffold_real_source_layout(raw_root)
            openalex_path = raw_root / "openalex" / "bad_numeric.jsonl"
            openalex_path.write_text(
                '{"doi":"10.4100/test.1","title":"Good row","abstract":"A","publication_date":"2021-01-01","type":"article","concepts":[{"display_name":"Biology","score":0.9}],"authorships":[],"referenced_works_count":12}\n'
                '{"doi":"10.4100/test.2","title":"Bad numeric","abstract":"B","publication_date":"2021-01-01","type":"article","concepts":[{"display_name":"Biology","score":0.9}],"authorships":[],"referenced_works_count":"not-a-number"}\n',
                encoding="utf-8",
            )
            register_snapshot(
                snapshot_id="snapshot_bad_numeric",
                raw_root=raw_root,
                root_dir=root,
                snapshot_date="2026-04-09",
            )
            ingest_snapshot("snapshot_bad_numeric", "openalex_bulk", root_dir=root)
            store = ManifestStore(root / "data" / "manifests" / "ingest.sqlite3")
            file_rows = store.list_files("snapshot_bad_numeric", "openalex_bulk")
            target_row = next(
                row for row in file_rows if row["relative_path"].endswith("bad_numeric.jsonl")
            )
            self.assertEqual(int(target_row["parsed_rows"]), 1)
            self.assertEqual(int(target_row["quarantined_rows"]), 1)
            summary_paths = materialize_canonical_snapshot(
                "snapshot_bad_numeric",
                root_dir=root,
                manifest=store,
            )
            summary = read_json(summary_paths["collection_summary"])
            self.assertIn(
                "openalex_bulk:bad_numeric",
                summary["quarantine_counts_by_error_code"],
            )

    def test_openalex_life_science_score_requires_specific_evidence(self):
        from life_science_integrity_benchmark.collectors import _extract_life_science_score

        broad_topic_only = {
            "primary_topic": {
                "subfield": {"display_name": "Biology"},
                "domain": {"display_name": "Life Sciences"},
            }
        }
        self.assertEqual(_extract_life_science_score(broad_topic_only), 0.0)

        scored_biology_topic = {
            "topics": [{"subfield": {"display_name": "Biology"}, "score": 0.81}]
        }
        self.assertEqual(_extract_life_science_score(scored_biology_topic), 0.81)

        specific_primary_topic = {
            "primary_topic": {
                "subfield": {"display_name": "Neuroscience"},
                "domain": {"display_name": "Life Sciences"},
            }
        }
        self.assertEqual(_extract_life_science_score(specific_primary_topic), 0.9)

    def test_openalex_early_scope_filter_preserves_allowlisted_dois(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            raw_root = root / "data" / "raw" / "snapshot_early_scope"
            (raw_root / "openalex").mkdir(parents=True)
            (raw_root / "official_notices").mkdir(parents=True)
            (raw_root / "pubmed").mkdir(parents=True)
            write_jsonl(
                raw_root / "openalex" / "works.jsonl.gz",
                [
                    {
                        "doi": "10.2300/scope.keep",
                        "title": "Scoped biomedical article",
                        "abstract": "A genomics article with specific OpenAlex evidence.",
                        "publication_date": "2024-01-15",
                        "type": "article",
                        "concepts": [{"display_name": "Genetics", "score": 0.86}],
                        "authorships": [],
                    },
                    {
                        "doi": "10.2300/scope.drop",
                        "title": "Broad life sciences only",
                        "abstract": "This row has only generic topic metadata.",
                        "publication_date": "2024-02-15",
                        "type": "article",
                        "primary_topic": {
                            "subfield": {"display_name": "Biology"},
                            "domain": {"display_name": "Life Sciences"},
                        },
                        "authorships": [],
                    },
                    {
                        "doi": "10.2300/scope.notice",
                        "title": "Weak topic row with official notice",
                        "abstract": "Notice-linked rows must survive early filtering.",
                        "publication_date": "2024-03-15",
                        "type": "article",
                        "primary_topic": {
                            "subfield": {"display_name": "Biology"},
                            "domain": {"display_name": "Life Sciences"},
                        },
                        "authorships": [],
                    },
                    {
                        "doi": "10.2300/scope.pubmed",
                        "title": "Weak topic row with PubMed DOI",
                        "abstract": "PubMed-linked rows must survive early filtering.",
                        "publication_date": "2024-04-15",
                        "type": "article",
                        "primary_topic": {
                            "subfield": {"display_name": "Biology"},
                            "domain": {"display_name": "Life Sciences"},
                        },
                        "authorships": [],
                    },
                ],
            )
            write_jsonl(
                raw_root / "official_notices" / "notices.jsonl.gz",
                [
                    {
                        "doi": "10.2300/scope.notice",
                        "notice_type": "retraction",
                        "notice_date": "2024-05-01",
                        "source_name": "Crossmark",
                        "source_url": "https://example.org/notices/scope.notice",
                    }
                ],
            )
            (raw_root / "pubmed" / "pubmed.xml").write_text(
                """<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation>
      <PMID>22222222</PMID>
      <Article>
        <ELocationID EIdType="doi">10.2300/scope.pubmed</ELocationID>
      </Article>
    </MedlineCitation>
    <PubmedData>
      <ArticleIdList><ArticleId IdType="doi">10.2300/scope.pubmed</ArticleId></ArticleIdList>
    </PubmedData>
  </PubmedArticle>
</PubmedArticleSet>
""",
                encoding="utf-8",
            )

            register_snapshot(
                snapshot_id="snapshot_early_scope",
                raw_root=raw_root,
                root_dir=root,
                snapshot_date="2026-04-09",
            )
            allowlist_path = root / "artifacts" / "openalex_scope_allowlist.txt"
            allowlist = build_openalex_scope_allowlist(
                snapshot_id="snapshot_early_scope",
                root_dir=root,
                output_path=allowlist_path,
            )
            self.assertEqual(allowlist["doi_count"], 2)
            self.assertEqual(
                allowlist_path.read_text(encoding="utf-8").splitlines(),
                ["10.2300/scope.notice", "10.2300/scope.pubmed"],
            )

            original_filter = os.environ.get("LSIB_OPENALEX_EARLY_SCOPE_FILTER")
            original_allowlist = os.environ.get("LSIB_OPENALEX_SCOPE_DOI_ALLOWLIST")
            os.environ["LSIB_OPENALEX_EARLY_SCOPE_FILTER"] = "1"
            os.environ["LSIB_OPENALEX_SCOPE_DOI_ALLOWLIST"] = str(allowlist_path)
            try:
                ingest_result = ingest_snapshot("snapshot_early_scope", "openalex_bulk", root_dir=root)
            finally:
                if original_filter is None:
                    os.environ.pop("LSIB_OPENALEX_EARLY_SCOPE_FILTER", None)
                else:
                    os.environ["LSIB_OPENALEX_EARLY_SCOPE_FILTER"] = original_filter
                if original_allowlist is None:
                    os.environ.pop("LSIB_OPENALEX_SCOPE_DOI_ALLOWLIST", None)
                else:
                    os.environ["LSIB_OPENALEX_SCOPE_DOI_ALLOWLIST"] = original_allowlist

            self.assertEqual(ingest_result["normalized_rows"], 3)
            self.assertEqual(ingest_result["quarantined_rows"], 0)
            self.assertEqual(ingest_result["scope_skipped_rows"], 1)
            store = ManifestStore(root / "data" / "manifests" / "ingest.sqlite3")
            scope_artifacts = store.list_artifacts(
                "snapshot_early_scope",
                "scope_skipped_openalex_bulk",
            )
            self.assertEqual(len(scope_artifacts), 1)
            self.assertEqual(int(scope_artifacts[0]["row_count"]), 1)

            ingest_snapshot("snapshot_early_scope", "local_notice_export", root_dir=root)
            paths = materialize_canonical_snapshot(
                "snapshot_early_scope",
                root_dir=root,
                manifest=store,
            )
            summary = read_json(paths["collection_summary"])
            self.assertEqual(summary["scope_skipped_rows_by_collector"]["openalex_bulk"], 1)
            canonical_articles = read_jsonl(paths["articles_dir"] / "part-00000.jsonl.gz")
            self.assertEqual(
                {row["doi"] for row in canonical_articles},
                {"10.2300/scope.keep", "10.2300/scope.notice", "10.2300/scope.pubmed"},
            )

    def test_pubmed_boolean_strings_are_coerced_before_materialize_join(self):
        from life_science_integrity_benchmark.collectors import PubMedIndexCollector
        from life_science_integrity_benchmark.materialize import _join_pubmed_metadata

        self.assertFalse(coerce_bool("0.0", default=True))
        self.assertTrue(coerce_bool("1.0", default=False))

        collector = PubMedIndexCollector()
        normalized = collector.normalize_record(
            {
                "doi": "10.4100/pubmed.bool",
                "pmid": "123",
                "is_pubmed_indexed": "false",
            },
            context={
                "file_id": "pubmed-file",
                "line_number": 2,
                "snapshot_id": "snapshot_bool",
            },
        )
        self.assertEqual(normalized["kind"], "normalized")
        self.assertFalse(normalized["row"]["is_pubmed_indexed"])

        article = {
            "doi": "10.4100/pubmed.bool",
            "is_pubmed_indexed": True,
            "source_lineage": [],
        }
        _join_pubmed_metadata(
            [article],
            [
                {
                    "doi": "10.4100/pubmed.bool",
                    "is_pubmed_indexed": "false",
                    "pmid": "123",
                    "mesh_terms": [],
                    "keywords": [],
                    "pubmed_publication_types": [],
                    "pubmed_journal_title": "",
                    "source_file_id": "pubmed-file",
                    "source_line_number": 2,
                }
            ],
        )
        self.assertFalse(article["is_pubmed_indexed"])

    def test_legacy_normalize_wrapper_emits_flat_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            raw_root = root / "raw"
            scaffold_real_source_layout(raw_root)
            normalized = normalize_real_source_exports(raw_root, root / "normalized")
            self.assertTrue(normalized["articles"].exists())
            self.assertTrue(normalized["official_notices"].exists())
            self.assertEqual(read_jsonl(normalized["external_signals"]), [])

    def test_experiment_report_generation_includes_ingest_summary(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            report_dir = Path(tmpdir)
            build_experiment_report(
                summary={
                    "snapshot_date": "2026-04-09",
                    "record_count": 16,
                    "auto_publish_count": 6,
                    "curated_review_count": 5,
                    "task_a_12m_eligible_count": 16,
                    "task_a_36m_eligible_count": 10,
                },
                splits={
                    "task_a_12m": {
                        "split_kind": "time",
                        "train_dois": ["a"],
                        "val_dois": ["b"],
                        "test_dois": ["c"],
                    }
                },
                leakage_report={
                    "passed": True,
                    "records_with_invalid_event_order": [],
                    "records_with_snapshot_violations": [],
                    "feature_cutoff_violations": [],
                },
                task_a_baselines={
                    "task_a_12m": [
                        {
                            "model_name": "metadata",
                            "backend_used": "logistic",
                            "metrics": {
                                "AUPRC": 0.5,
                                "AUPRC_ci_lower": 0.3,
                                "AUPRC_ci_upper": 0.7,
                                "Precision@1pct": 0.6,
                                "Precision@5pct": 0.5,
                                "Recall@1pct": 0.2,
                                "Recall@5pct": 0.4,
                                "ECE": 0.1,
                            },
                        }
                    ]
                },
                task_b_baseline={
                    "model_name": "task_b_rules",
                    "backend_used": "keyword",
                    "metrics": {
                        "notice_status_accuracy": 0.8,
                        "tag_macro_f1": 0.7,
                        "provenance_coverage": 0.9,
                    },
                },
                markdown_path=report_dir / "experiment_report.md",
                json_path=report_dir / "experiment_report.json",
                ingest_summary={
                    "snapshot_id": "snapshot_alpha",
                    "raw_file_counts_by_collector": {"openalex_bulk": 1},
                    "parsed_row_counts_by_collector": {"openalex_bulk": 10},
                    "quarantine_counts_by_error_code": {"openalex_bulk:bad_json": 1},
                    "duplicate_doi_count": 2,
                    "orphan_notice_count": 1,
                    "date_precision_distribution": {"day": 9, "year_imputed": 1},
                },
            )
            markdown = (report_dir / "experiment_report.md").read_text(encoding="utf-8")
            self.assertIn("Benchmark Experiment Report", markdown)
            self.assertIn("Ingest Summary", markdown)
            self.assertIn("Duplicate DOI count", markdown)
            self.assertIn("95% CI 0.3-0.7", markdown)
            self.assertIn("Precision@1pct=0.6", markdown)

    def test_results_v0_2_markdown_generation_includes_real_metrics(self):
        markdown = build_results_v0_2_markdown(
            summary={
                "snapshot_date": "2026-04-09",
                "record_count": 1234,
                "auto_publish_count": 1200,
                "curated_review_count": 34,
                "task_a_12m_eligible_count": 900,
                "task_a_36m_eligible_count": 850,
                "task_a_noisy_date_count": 17,
                "notice_status_counts": {
                    "none_known_at_snapshot": 1200,
                    "retracted": 20,
                    "editorial_notice": 14,
                },
                "subfield_counts": {"biology": 600, "biomedicine": 634},
            },
            leakage_report={
                "passed": True,
                "records_checked": 1234,
                "leaked_fields_found": [],
                "records_missing_feature_provenance": [],
                "records_with_invalid_event_order": [],
                "records_with_snapshot_violations": [],
                "feature_cutoff_violations": [],
            },
            task_a_baselines={
                "task_a_12m": [
                    {
                        "model_name": "metadata_logistic_baseline",
                        "metrics": {
                            "AUPRC": 0.6123,
                            "AUPRC_ci_lower": 0.5123,
                            "AUPRC_ci_upper": 0.7012,
                            "Precision@1pct": 0.4,
                            "Recall@1pct": 0.1,
                            "Precision@5pct": 0.3,
                            "Recall@5pct": 0.2,
                            "ECE": 0.0821,
                        },
                    }
                ],
                "task_a_36m": [
                    {
                        "model_name": "abstract_encoder_baseline",
                        "metrics": {
                            "AUPRC": 0.4555,
                            "AUPRC_ci_lower": 0.4011,
                            "AUPRC_ci_upper": 0.5002,
                            "Precision@1pct": 0.25,
                            "Recall@1pct": 0.08,
                            "Precision@5pct": 0.21,
                            "Recall@5pct": 0.19,
                            "ECE": 0.1022,
                        },
                    }
                ],
            },
            task_a_robustness={
                "task_a_12m": [
                    {
                        "model_name": "metadata_logistic_baseline",
                        "metrics": {"AUPRC": 0.6123},
                    }
                ],
                "task_a_12m_author_cluster_holdout": [
                    {
                        "model_name": "metadata_logistic_baseline",
                        "metrics": {"AUPRC": 0.401},
                    }
                ],
                "task_a_12m_venue_holdout": [
                    {
                        "model_name": "metadata_logistic_baseline",
                        "metrics": {"AUPRC": 0.55},
                    }
                ],
                "task_a_12m_publisher_holdout": [
                    {
                        "model_name": "metadata_logistic_baseline",
                        "metrics": {"AUPRC": 0.48},
                    }
                ],
                "task_a_36m": [
                    {
                        "model_name": "abstract_encoder_baseline",
                        "metrics": {"AUPRC": 0.4555},
                    }
                ],
            },
            task_b_baseline={
                "metrics": {
                    "notice_status_accuracy": 0.91,
                    "tag_macro_f1": 0.42,
                    "provenance_coverage": 0.88,
                }
            },
            run_root="/tmp/lsib-real-run",
        )
        self.assertIn("# Real-Data Results (v0.2)", markdown)
        self.assertIn("AUPRC 95% CI", markdown)
        self.assertIn("metadata_logistic", markdown)
        self.assertIn("0.512-0.701", markdown)
        self.assertIn("abstract_encoder (hashing)", markdown)
        self.assertIn("task_a_pr_curves.svg", markdown)
        self.assertIn("benchmark_v1.csv", markdown)
        self.assertIn("adjudication_queue.csv", markdown)
        self.assertIn("adjudication_protocol.md", markdown)
        self.assertIn("internal_curation_queue.json", markdown)
        self.assertIn("experiment_report.json", markdown)
        self.assertIn("biology=600", markdown)
        self.assertIn("| Notice-status accuracy | `0.910` |", markdown)
        self.assertIn("| `metadata_logistic` | 0.612 | 0.401 | 0.55 | 0.48 |", markdown)

    def test_cli_build_results_v0_2_writes_markdown(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            release_dir = root / "release"
            release_dir.mkdir()
            write_json(
                release_dir / "summary.json",
                {
                    "snapshot_date": "2026-04-09",
                    "record_count": 10,
                    "auto_publish_count": 8,
                    "curated_review_count": 2,
                    "task_a_12m_eligible_count": 7,
                    "task_a_36m_eligible_count": 6,
                    "task_a_noisy_date_count": 1,
                    "notice_status_counts": {"none_known_at_snapshot": 8, "retracted": 2},
                    "subfield_counts": {"biology": 10},
                },
            )
            write_json(
                release_dir / "leakage_report.json",
                {
                    "passed": True,
                    "records_checked": 10,
                    "leaked_fields_found": [],
                    "records_missing_feature_provenance": [],
                    "records_with_invalid_event_order": [],
                    "records_with_snapshot_violations": [],
                    "feature_cutoff_violations": [],
                },
            )
            write_json(
                release_dir / "task_a_baselines.json",
                {
                    "task_a_12m": [
                        {
                            "model_name": "metadata_logistic_baseline",
                            "metrics": {
                                "AUPRC": 0.5,
                                "AUPRC_ci_lower": 0.4,
                                "AUPRC_ci_upper": 0.6,
                                "Precision@1pct": 1.0,
                                "Recall@1pct": 0.5,
                                "Precision@5pct": 0.5,
                                "Recall@5pct": 0.5,
                                "ECE": 0.1,
                            },
                        }
                    ],
                    "task_a_36m": [],
                },
            )
            write_json(release_dir / "task_a_robustness.json", {})
            write_json(
                release_dir / "task_b_baseline.json",
                {
                    "metrics": {
                        "notice_status_accuracy": 0.8,
                        "tag_macro_f1": 0.7,
                        "provenance_coverage": 0.9,
                    }
                },
            )

            output_path = root / "results_v0.2.md"
            env = os.environ.copy()
            env["PYTHONPATH"] = str(self.repo_root / "src")
            subprocess.run(
                [
                    self.python_bin,
                    "-m",
                    "life_science_integrity_benchmark.cli",
                    "--root-dir",
                    str(self.repo_root),
                    "--release-dir",
                    str(release_dir),
                    "build-results-v0-2",
                    "--output-path",
                    str(output_path),
                    "--run-root",
                    "/tmp/lsib-real-run",
                ],
                cwd=self.repo_root,
                env=env,
                capture_output=True,
                text=True,
                check=True,
            )
            markdown = output_path.read_text(encoding="utf-8")
            self.assertIn("# Real-Data Results (v0.2)", markdown)
            self.assertIn("0.400-0.600", markdown)
            self.assertIn("/tmp/lsib-real-run", markdown)

    def test_update_readme_for_v0_2_rewrites_managed_blocks(self):
        template = """# LSIB

<!-- LSIB_STATUS_START -->
old status
<!-- LSIB_STATUS_END -->

<!-- LSIB_RELEASE_SNAPSHOT_START -->
old snapshot
<!-- LSIB_RELEASE_SNAPSHOT_END -->
"""
        updated = update_readme_for_v0_2(
            readme_text=template,
            summary={
                "snapshot_date": "2026-04-09",
                "record_count": 1234,
                "auto_publish_count": 1200,
                "curated_review_count": 34,
            },
            leakage_report={"passed": True},
            task_a_baselines={
                "task_a_12m": [
                    {
                        "model_name": "metadata_logistic_baseline",
                        "metrics": {
                            "AUPRC": 0.6123,
                            "AUPRC_ci_lower": 0.5123,
                            "AUPRC_ci_upper": 0.7012,
                        },
                    }
                ],
                "task_a_36m": [
                    {
                        "model_name": "abstract_encoder_baseline",
                        "metrics": {
                            "AUPRC": 0.4555,
                            "AUPRC_ci_lower": 0.4011,
                            "AUPRC_ci_upper": 0.5002,
                        },
                    }
                ],
            },
            task_b_baseline={"metrics": {"notice_status_accuracy": 0.91}},
            results_doc_path="docs/results_v0.2.md",
        )
        self.assertIn("Status: **v0.2 released.**", updated)
        self.assertIn("## Release Snapshot", updated)
        self.assertIn("1234", updated)
        self.assertIn("metadata_logistic (0.612, 95% CI 0.512-0.701)", updated)
        self.assertIn("[docs/results_v0.2.md](docs/results_v0.2.md)", updated)

    def test_update_readme_for_v0_2_ignores_missing_headline_auprc(self):
        template = """# LSIB

<!-- LSIB_STATUS_START -->
old status
<!-- LSIB_STATUS_END -->

<!-- LSIB_RELEASE_SNAPSHOT_START -->
old snapshot
<!-- LSIB_RELEASE_SNAPSHOT_END -->
"""
        updated = update_readme_for_v0_2(
            readme_text=template,
            summary={
                "snapshot_date": "2026-04-09",
                "record_count": 1234,
                "auto_publish_count": 1200,
                "curated_review_count": 34,
            },
            leakage_report={"passed": True},
            task_a_baselines={
                "task_a_12m": [
                    {
                        "model_name": "metadata_logistic_baseline",
                        "metrics": {
                            "AUPRC": None,
                            "AUPRC_ci_lower": None,
                            "AUPRC_ci_upper": None,
                        },
                    },
                    {
                        "model_name": "abstract_encoder_baseline",
                        "metrics": {
                            "AUPRC": 0.4555,
                            "AUPRC_ci_lower": 0.4011,
                            "AUPRC_ci_upper": 0.5002,
                        },
                    },
                ],
                "task_a_36m": [],
            },
            task_b_baseline={"metrics": {"notice_status_accuracy": 0.91}},
            results_doc_path="docs/results_v0.2.md",
        )

        self.assertIn("12m=abstract_encoder (hashing) (0.456, 95% CI 0.401-0.500)", updated)
        self.assertIn("Best Task A 36m AUPRC | `n/a`", updated)

    def test_cli_build_readme_v0_2_writes_updated_readme(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            release_dir = root / "release"
            release_dir.mkdir()
            write_json(
                release_dir / "summary.json",
                {
                    "snapshot_date": "2026-04-09",
                    "record_count": 10,
                    "auto_publish_count": 8,
                    "curated_review_count": 2,
                },
            )
            write_json(release_dir / "leakage_report.json", {"passed": True})
            write_json(
                release_dir / "task_a_baselines.json",
                {
                    "task_a_12m": [
                        {
                            "model_name": "metadata_logistic_baseline",
                            "metrics": {
                                "AUPRC": 0.5,
                                "AUPRC_ci_lower": 0.4,
                                "AUPRC_ci_upper": 0.6,
                            },
                        }
                    ],
                    "task_a_36m": [],
                },
            )
            write_json(
                release_dir / "task_b_baseline.json",
                {"metrics": {"notice_status_accuracy": 0.8}},
            )

            readme_path = root / "README.md"
            readme_path.write_text(
                "# LSIB\n\n"
                "<!-- LSIB_STATUS_START -->\n"
                "old status\n"
                "<!-- LSIB_STATUS_END -->\n\n"
                "<!-- LSIB_RELEASE_SNAPSHOT_START -->\n"
                "old snapshot\n"
                "<!-- LSIB_RELEASE_SNAPSHOT_END -->\n",
                encoding="utf-8",
            )

            env = os.environ.copy()
            env["PYTHONPATH"] = str(self.repo_root / "src")
            subprocess.run(
                [
                    self.python_bin,
                    "-m",
                    "life_science_integrity_benchmark.cli",
                    "--root-dir",
                    str(self.repo_root),
                    "--release-dir",
                    str(release_dir),
                    "build-readme-v0-2",
                    "--output-path",
                    str(readme_path),
                    "--results-doc-path",
                    "docs/results_v0.2.md",
                ],
                cwd=self.repo_root,
                env=env,
                capture_output=True,
                text=True,
                check=True,
            )
            updated = readme_path.read_text(encoding="utf-8")
            self.assertIn("Status: **v0.2 released.**", updated)
            self.assertIn("## Release Snapshot", updated)
            self.assertIn("Task B notice accuracy", updated)

    def test_finalize_open_data_release_from_local_reports_running_status(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            ssh_output = root / "ssh_output.txt"
            ssh_output.write_text(
                "job_id=2789241\n"
                "job_id_file=/remote/run/root/artifacts/open_data_release/job_id.txt\n"
                "job_status=RUNNING\n"
                "job_elapsed=1-23:27:43\n"
                "job_time_limit=5-00:00:00\n"
                "job_start=2026-04-21T12:51:17\n"
                "job_node=c0003\n"
                "resource_activity=observed\n"
                "sstat_ave_cpu=1-05:56:58\n"
                "sstat_max_rss=33549176K\n"
                "current_step=ingest_openalex\n"
                "current_step_mtime=2026-04-13 22:00:50\n"
                "current_step_age_seconds=169145\n"
                "log_path=/remote/run/root/logs/open-data-downstream-2789241.out\n"
                "log_mtime=2026-04-13 22:00:49\n"
                "log_age_seconds=169146\n",
                encoding="utf-8",
            )

            mock_ssh = root / "mock_ssh.sh"
            mock_ssh.write_text(
                "#!/bin/bash\n"
                "cat \"$MOCK_SSH_OUTPUT\"\n",
                encoding="utf-8",
            )
            mock_ssh.chmod(0o755)

            harvest_flag = root / "harvest_called.txt"
            mock_harvest = root / "mock_harvest.sh"
            mock_harvest.write_text(
                "#!/bin/bash\n"
                "set -euo pipefail\n"
                "touch \"$HARVEST_FLAG\"\n",
                encoding="utf-8",
            )
            mock_harvest.chmod(0o755)

            result = subprocess.run(
                [
                    "/bin/bash",
                    str(self.cayuga_script_dir / "finalize_open_data_release_from_local.sh"),
                    "cayuga-phobos",
                    "/remote/run/root",
                    str(root / "local"),
                    str(root / "results_v0.2.md"),
                ],
                cwd=self.repo_root,
                capture_output=True,
                text=True,
                env={
                    **os.environ,
                    "MOCK_SSH_OUTPUT": str(ssh_output),
                    "SSH_BIN": str(mock_ssh),
                    "HARVEST_SCRIPT": str(mock_harvest),
                    "HARVEST_FLAG": str(harvest_flag),
                },
                check=True,
            )
            self.assertIn("job_id_file=/remote/run/root/artifacts/open_data_release/job_id.txt", result.stdout)
            self.assertIn("job_status=RUNNING", result.stdout)
            self.assertIn("job_elapsed=1-23:27:43", result.stdout)
            self.assertIn("job_time_limit=5-00:00:00", result.stdout)
            self.assertIn("resource_activity=observed", result.stdout)
            self.assertIn("sstat_max_rss=33549176K", result.stdout)
            self.assertIn("current_step_stale=yes", result.stdout)
            self.assertIn("log_stale=yes", result.stdout)
            self.assertIn("stale_progress=yes", result.stdout)
            self.assertIn("stale_progress_context=resource_activity_observed", result.stdout)
            self.assertIn("progress_recommendation=inspect_runtime_or_wait", result.stdout)
            self.assertIn("release_ready=no", result.stdout)
            self.assertFalse(harvest_flag.exists())

    def test_finalize_open_data_release_from_local_prefers_failed_marker_over_completed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            remote_run_root = root / "remote_run"
            art_root = remote_run_root / "artifacts" / "open_data_release"
            art_root.mkdir(parents=True)
            (art_root / "job_id.txt").write_text("2789241\n", encoding="utf-8")
            (art_root / "COMPLETED").write_text("", encoding="utf-8")
            (art_root / "FAILED").write_text("", encoding="utf-8")
            (art_root / "failed_step.txt").write_text("build_report\n", encoding="utf-8")

            mock_ssh = root / "mock_ssh.sh"
            mock_ssh.write_text(
                "#!/bin/bash\n"
                "set -euo pipefail\n"
                "shift\n"
                "\"$@\"\n",
                encoding="utf-8",
            )
            mock_ssh.chmod(0o755)

            harvest_flag = root / "harvest_called.txt"
            mock_harvest = root / "mock_harvest.sh"
            mock_harvest.write_text(
                "#!/bin/bash\n"
                "set -euo pipefail\n"
                "touch \"$HARVEST_FLAG\"\n",
                encoding="utf-8",
            )
            mock_harvest.chmod(0o755)

            result = subprocess.run(
                [
                    "/bin/bash",
                    str(self.cayuga_script_dir / "finalize_open_data_release_from_local.sh"),
                    "mock-host",
                    str(remote_run_root),
                    str(root / "local"),
                    str(root / "results_v0.2.md"),
                ],
                cwd=self.repo_root,
                capture_output=True,
                text=True,
                env={
                    **os.environ,
                    "SSH_BIN": str(mock_ssh),
                    "HARVEST_SCRIPT": str(mock_harvest),
                    "HARVEST_FLAG": str(harvest_flag),
                    "PYTHON_BIN": self.python_bin,
                },
                check=True,
            )

            self.assertIn("job_status=FAILED_MARKER", result.stdout)
            self.assertIn("failed_step=build_report", result.stdout)
            self.assertIn("release_ready=no", result.stdout)
            self.assertIn("next_action=resubmit_recommended", result.stdout)
            self.assertFalse(harvest_flag.exists())

    def test_finalize_open_data_release_from_local_harvests_and_builds_results_doc(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            local_dest = root / "local"
            output_path = root / "results_v0.2.md"
            readme_path = root / "README.md"
            ssh_output = root / "ssh_output.txt"
            ssh_output.write_text(
                "job_id=2789241\n"
                "job_status=COMPLETED\n"
                "current_step=build-report\n"
                "current_step_mtime=2026-04-16 09:00:00\n",
                encoding="utf-8",
            )
            readme_path.write_text(
                "# LSIB\n\n"
                "<!-- LSIB_STATUS_START -->\n"
                "old status\n"
                "<!-- LSIB_STATUS_END -->\n\n"
                "<!-- LSIB_RELEASE_SNAPSHOT_START -->\n"
                "old snapshot\n"
                "<!-- LSIB_RELEASE_SNAPSHOT_END -->\n",
                encoding="utf-8",
            )

            mock_ssh = root / "mock_ssh.sh"
            mock_ssh.write_text(
                "#!/bin/bash\n"
                "cat \"$MOCK_SSH_OUTPUT\"\n",
                encoding="utf-8",
            )
            mock_ssh.chmod(0o755)

            seed_release = root / "seed_release"
            seed_release.mkdir()
            write_json(
                seed_release / "summary.json",
                {
                    "snapshot_date": "2026-04-09",
                    "record_count": 10,
                    "auto_publish_count": 8,
                    "curated_review_count": 2,
                    "task_a_12m_eligible_count": 7,
                    "task_a_36m_eligible_count": 6,
                    "task_a_noisy_date_count": 1,
                    "notice_status_counts": {"none_known_at_snapshot": 8, "retracted": 2},
                    "subfield_counts": {"biology": 10},
                },
            )
            write_json(
                seed_release / "leakage_report.json",
                {
                    "passed": True,
                    "records_checked": 10,
                    "leaked_fields_found": [],
                    "records_missing_feature_provenance": [],
                    "records_with_invalid_event_order": [],
                    "records_with_snapshot_violations": [],
                    "feature_cutoff_violations": [],
                },
            )
            write_json(
                seed_release / "task_a_baselines.json",
                {
                    "task_a_12m": [
                        {
                            "model_name": "metadata_logistic_baseline",
                            "metrics": {
                                "AUPRC": 0.5,
                                "AUPRC_ci_lower": 0.4,
                                "AUPRC_ci_upper": 0.6,
                                "Precision@1pct": 1.0,
                                "Recall@1pct": 0.5,
                                "Precision@5pct": 0.5,
                                "Recall@5pct": 0.5,
                                "ECE": 0.1,
                            },
                        }
                    ],
                    "task_a_36m": [],
                },
            )
            write_json(seed_release / "task_a_robustness.json", {})
            write_json(
                seed_release / "task_b_baseline.json",
                {
                    "metrics": {
                        "notice_status_accuracy": 0.8,
                        "tag_macro_f1": 0.7,
                        "provenance_coverage": 0.9,
                    }
                },
            )

            mock_harvest = root / "mock_harvest.sh"
            mock_harvest.write_text(
                "#!/bin/bash\n"
                "set -euo pipefail\n"
                "DEST=\"$3/artifacts/open_data_release\"\n"
                "mkdir -p \"$DEST\"\n"
                "cp \"$SEED_RELEASE\"/*.json \"$DEST/\"\n",
                encoding="utf-8",
            )
            mock_harvest.chmod(0o755)

            result = subprocess.run(
                [
                    "/bin/bash",
                    str(self.cayuga_script_dir / "finalize_open_data_release_from_local.sh"),
                    "cayuga-phobos",
                    "/remote/run/root",
                    str(local_dest),
                    str(output_path),
                ],
                cwd=self.repo_root,
                capture_output=True,
                text=True,
                env={
                    **os.environ,
                    "MOCK_SSH_OUTPUT": str(ssh_output),
                    "SSH_BIN": str(mock_ssh),
                    "HARVEST_SCRIPT": str(mock_harvest),
                    "PYTHON_BIN": "python3",
                    "SEED_RELEASE": str(seed_release),
                    "README_PATH_OVERRIDE": str(readme_path),
                },
                check=True,
            )
            markdown = output_path.read_text(encoding="utf-8")
            updated_readme = readme_path.read_text(encoding="utf-8")
            self.assertIn("release_ready=yes", result.stdout)
            self.assertIn("results_doc=%s" % output_path, result.stdout)
            self.assertIn("readme_doc=%s" % readme_path, result.stdout)
            self.assertIn("# Real-Data Results (v0.2)", markdown)
            self.assertIn("0.400-0.600", markdown)
            self.assertIn("/remote/run/root", markdown)
            self.assertIn("Status: **v0.2 released.**", updated_readme)

    def test_watch_open_data_release_from_local_stops_when_release_ready(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            mock_finalize = root / "mock_finalize.sh"
            mock_finalize.write_text(
                "#!/bin/bash\n"
                "echo 'job_id=2789241'\n"
                "echo 'job_status=COMPLETED'\n"
                "echo 'release_ready=yes'\n"
                "echo 'results_doc=/tmp/results_v0.2.md'\n",
                encoding="utf-8",
            )
            mock_finalize.chmod(0o755)

            result = subprocess.run(
                [
                    "/bin/bash",
                    str(self.cayuga_script_dir / "watch_open_data_release_from_local.sh"),
                    "cayuga-phobos",
                    "/remote/run/root",
                    "0",
                    "3",
                ],
                cwd=self.repo_root,
                capture_output=True,
                text=True,
                env={**os.environ, "FINALIZE_SCRIPT": str(mock_finalize)},
                check=True,
            )
            self.assertIn("poll=1", result.stdout)
            self.assertIn("release_ready=yes", result.stdout)
            self.assertIn("watch_status=COMPLETED", result.stdout)

    def test_watch_open_data_release_from_local_respects_max_polls(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            mock_finalize = root / "mock_finalize.sh"
            mock_finalize.write_text(
                "#!/bin/bash\n"
                "echo 'job_id=2789241'\n"
                "echo 'job_status=RUNNING'\n"
                "echo 'current_step=ingest_openalex'\n"
                "echo 'stale_progress=yes'\n"
                "echo 'release_ready=no'\n",
                encoding="utf-8",
            )
            mock_finalize.chmod(0o755)

            result = subprocess.run(
                [
                    "/bin/bash",
                    str(self.cayuga_script_dir / "watch_open_data_release_from_local.sh"),
                    "cayuga-phobos",
                    "/remote/run/root",
                    "0",
                    "2",
                ],
                cwd=self.repo_root,
                capture_output=True,
                text=True,
                env={**os.environ, "FINALIZE_SCRIPT": str(mock_finalize)},
                check=True,
            )
            self.assertIn("poll=1", result.stdout)
            self.assertIn("poll=2", result.stdout)
            self.assertIn("watch_warning=STALE_PROGRESS", result.stdout)
            self.assertIn("max_polls_reached=2", result.stdout)
            self.assertIn("watch_status=TIMED_OUT", result.stdout)

    def test_harvest_open_data_release_to_local_copies_and_prints_summary(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            remote_run_root = root / "remote_run"
            remote_release = remote_run_root / "artifacts" / "open_data_release"
            remote_site = remote_run_root / "artifacts" / "open_data_site"
            remote_release.mkdir(parents=True)
            remote_site.mkdir(parents=True)
            (remote_release / "COMPLETED").write_text("", encoding="utf-8")
            write_json(
                remote_release / "summary.json",
                {
                    "snapshot_date": "2026-04-09",
                    "record_count": 10,
                    "auto_publish_count": 8,
                    "curated_review_count": 2,
                    "task_a_12m_eligible_count": 7,
                    "task_a_36m_eligible_count": 6,
                    "task_a_noisy_date_count": 1,
                    "notice_status_counts": {"retracted": 2},
                    "subfield_counts": {"biology": 10},
                },
            )
            write_json(
                remote_release / "leakage_report.json",
                {
                    "passed": True,
                    "records_checked": 10,
                    "feature_cutoff_violations": [],
                    "leaked_fields_found": [],
                    "records_missing_feature_provenance": [],
                    "records_with_invalid_event_order": [],
                    "records_with_snapshot_violations": [],
                },
            )
            write_json(
                remote_release / "task_a_baselines.json",
                {
                    "task_a_12m": [
                        {
                            "model_name": "metadata_logistic_baseline",
                            "metrics": {
                                "AUPRC": 0.5,
                                "Recall@1pct": 0.5,
                                "Recall@5pct": 0.5,
                                "ECE": None,
                            },
                        }
                    ]
                },
            )
            write_json(
                remote_release / "task_b_baseline.json",
                {
                    "metrics": {
                        "notice_status_accuracy": 0.8,
                        "tag_macro_f1": None,
                        "provenance_coverage": 0.9,
                    }
                },
            )
            (remote_release / "benchmark_v1.jsonl").write_text("{}\n", encoding="utf-8")
            (remote_site / "index.html").write_text("<html></html>\n", encoding="utf-8")

            mock_bin = root / "mock_bin"
            mock_bin.mkdir()
            mock_ssh = mock_bin / "ssh"
            mock_ssh.write_text(
                "#!/bin/bash\n"
                "set -euo pipefail\n"
                "cmd=\"${@: -1}\"\n"
                "eval \"$cmd\"\n",
                encoding="utf-8",
            )
            mock_ssh.chmod(0o755)
            mock_rsync = mock_bin / "rsync"
            mock_rsync.write_text(
                "#!/bin/bash\n"
                "set -euo pipefail\n"
                "args=()\n"
                "for arg in \"$@\"; do\n"
                "  case \"$arg\" in\n"
                "    -*) ;;\n"
                "    *) args+=(\"$arg\") ;;\n"
                "  esac\n"
                "done\n"
                "src=\"${args[0]}\"\n"
                "dest=\"${args[1]}\"\n"
                "src_path=\"${src#*:}\"\n"
                "mkdir -p \"$dest\"\n"
                "cp -R \"$src_path\"/. \"$dest\"/\n",
                encoding="utf-8",
            )
            mock_rsync.chmod(0o755)

            local_dest = root / "local"
            result = subprocess.run(
                [
                    "/bin/bash",
                    str(self.cayuga_script_dir / "harvest_open_data_release_to_local.sh"),
                    "mock-host",
                    str(remote_run_root),
                    str(local_dest),
                ],
                cwd=self.repo_root,
                capture_output=True,
                text=True,
                env={
                    **os.environ,
                    "PATH": "%s:%s" % (mock_bin, os.environ.get("PATH", "")),
                    "PYTHON_BIN": self.python_bin,
                },
                check=True,
            )
            self.assertIn("== Release summary ==", result.stdout)
            self.assertIn("Snapshot date:", result.stdout)
            self.assertIn("Task A task_a_12m:", result.stdout)
            self.assertIn("ECE=n/a", result.stdout)
            self.assertIn("macroF1=n/a", result.stdout)
            self.assertIn("Files:", result.stdout)
            self.assertIn("summary.json", result.stdout)
            self.assertTrue((local_dest / "artifacts" / "open_data_release" / "summary.json").exists())
            self.assertTrue((local_dest / "artifacts" / "open_data_site" / "index.html").exists())

    def test_harvest_open_data_release_to_local_refuses_failed_marker(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            remote_run_root = root / "remote_run"
            remote_release = remote_run_root / "artifacts" / "open_data_release"
            remote_site = remote_run_root / "artifacts" / "open_data_site"
            remote_release.mkdir(parents=True)
            remote_site.mkdir(parents=True)
            (remote_release / "COMPLETED").write_text("", encoding="utf-8")
            (remote_release / "FAILED").write_text("", encoding="utf-8")
            (remote_release / "failed_step.txt").write_text("build_report\n", encoding="utf-8")

            mock_bin = root / "mock_bin"
            mock_bin.mkdir()
            mock_ssh = mock_bin / "ssh"
            mock_ssh.write_text(
                "#!/bin/bash\n"
                "set -euo pipefail\n"
                "cmd=\"${@: -1}\"\n"
                "eval \"$cmd\"\n",
                encoding="utf-8",
            )
            mock_ssh.chmod(0o755)
            mock_rsync = mock_bin / "rsync"
            mock_rsync.write_text(
                "#!/bin/bash\n"
                "touch \"$RSYNC_CALLED\"\n",
                encoding="utf-8",
            )
            mock_rsync.chmod(0o755)
            rsync_called = root / "rsync_called.txt"

            result = subprocess.run(
                [
                    "/bin/bash",
                    str(self.cayuga_script_dir / "harvest_open_data_release_to_local.sh"),
                    "mock-host",
                    str(remote_run_root),
                    str(root / "local"),
                ],
                cwd=self.repo_root,
                capture_output=True,
                text=True,
                env={
                    **os.environ,
                    "PATH": "%s:%s" % (mock_bin, os.environ.get("PATH", "")),
                    "PYTHON_BIN": self.python_bin,
                    "RSYNC_CALLED": str(rsync_called),
                },
                check=False,
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("FAILED marker found", result.stderr)
            self.assertIn("build_report", result.stderr)
            self.assertFalse(rsync_called.exists())

    def test_watch_real_ingest_prefers_failed_marker_over_completed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            run_root = Path(tmpdir)
            art_root = run_root / "artifacts" / "real_release"
            art_root.mkdir(parents=True)
            (art_root / "job_id.txt").write_text("12345\n", encoding="utf-8")
            (art_root / "COMPLETED").write_text("", encoding="utf-8")
            (art_root / "FAILED").write_text("", encoding="utf-8")
            (art_root / "failed_step.txt").write_text("ingest_openalex\n", encoding="utf-8")

            result = subprocess.run(
                [
                    "/bin/bash",
                    str(self.cayuga_script_dir / "watch_real_ingest.sh"),
                    str(run_root),
                    "0",
                    "1",
                ],
                cwd=self.repo_root,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(result.returncode, 1)
            self.assertIn("failed_step=ingest_openalex", result.stdout)
            self.assertIn("real_ingest_status=FAILED", result.stdout)

    def test_real_ingest_template_uses_node_local_runtime_root(self):
        template = (
            self.cayuga_script_dir / "templates" / "real_ingest_template.sbatch.in"
        ).read_text(encoding="utf-8")
        self.assertIn('LOCAL_RUNTIME_BASE="${SLURM_TMPDIR:-${TMPDIR:-/tmp}}"', template)
        self.assertIn(
            'RUNTIME_ROOT="$LOCAL_RUNTIME_BASE/lsib_real_runtime_${SLURM_JOB_ID:-$$}"',
            template,
        )
        self.assertIn(
            'CANONICAL_SOURCE_DIR="$RUNTIME_ROOT/data/normalized/$SNAPSHOT_ID/canonical"',
            template,
        )
        self.assertIn(
            'rm -f "$ART_RELEASE/COMPLETED" "$ART_RELEASE/FAILED" "$ART_RELEASE/current_step.txt" "$ART_RELEASE/failed_step.txt"',
            template,
        )
        self.assertIn('--root-dir "$RUNTIME_ROOT"', template)

    def test_submit_real_ingest_clears_stale_markers_before_recording_job_id(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            run_root = Path(tmpdir)
            job_dir = run_root / "jobs"
            art_root = run_root / "artifacts" / "real_release"
            raw_root = run_root / "raw" / "real_snapshot"
            job_dir.mkdir(parents=True)
            art_root.mkdir(parents=True)
            for label in ("openalex", "official_notices", "pubmed"):
                target = raw_root / label
                target.mkdir(parents=True, exist_ok=True)
                (target / "dummy.jsonl").write_text("{}\n", encoding="utf-8")
            (job_dir / "real_ingest_template.sbatch").write_text("#!/bin/bash\n", encoding="utf-8")
            (art_root / "COMPLETED").write_text("", encoding="utf-8")
            (art_root / "FAILED").write_text("", encoding="utf-8")
            (art_root / "current_step.txt").write_text("old_step\n", encoding="utf-8")
            (art_root / "failed_step.txt").write_text("old_failed_step\n", encoding="utf-8")

            mock_bin = run_root / "mock_bin"
            mock_bin.mkdir()
            mock_sbatch = mock_bin / "sbatch"
            mock_sbatch.write_text(
                "#!/bin/bash\n"
                "echo 'Submitted batch job 424242'\n",
                encoding="utf-8",
            )
            mock_sbatch.chmod(0o755)

            result = subprocess.run(
                [
                    "/bin/bash",
                    str(self.cayuga_script_dir / "submit_real_ingest.sh"),
                    str(run_root),
                ],
                cwd=self.repo_root,
                capture_output=True,
                text=True,
                env={
                    **os.environ,
                    "PATH": "%s:%s" % (mock_bin, os.environ.get("PATH", "")),
                },
                check=True,
            )
            self.assertIn("real_snapshot_ready=yes", result.stdout)
            self.assertIn("real_ingest_job_id=424242", result.stdout)
            self.assertEqual((art_root / "job_id.txt").read_text(encoding="utf-8").strip(), "424242")
            self.assertFalse((art_root / "COMPLETED").exists())
            self.assertFalse((art_root / "FAILED").exists())
            self.assertFalse((art_root / "current_step.txt").exists())
            self.assertFalse((art_root / "failed_step.txt").exists())

    def test_submit_open_data_downstream_only_clears_stale_markers_before_recording_job_id(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            run_root = Path(tmpdir)
            job_dir = run_root / "jobs"
            art_root = run_root / "artifacts" / "open_data_release"
            raw_root = run_root / "raw" / "public_open_data_snapshot"
            template_path = (
                run_root
                / "repo"
                / "scripts"
                / "cayuga"
                / "templates"
                / "open_data_downstream_only.sbatch.in"
            )
            job_dir.mkdir(parents=True)
            art_root.mkdir(parents=True)
            template_path.parent.mkdir(parents=True)
            template_path.write_text("#!/bin/bash\n# __RUN_ROOT__\n", encoding="utf-8")
            for label in ("openalex", "official_notices", "pubmed"):
                target = raw_root / label
                target.mkdir(parents=True, exist_ok=True)
                (target / "dummy.jsonl").write_text("{}\n", encoding="utf-8")
            (art_root / "COMPLETED").write_text("", encoding="utf-8")
            (art_root / "FAILED").write_text("", encoding="utf-8")
            (art_root / "current_step.txt").write_text("old_step\n", encoding="utf-8")
            (art_root / "failed_step.txt").write_text("old_failed_step\n", encoding="utf-8")

            mock_bin = run_root / "mock_bin"
            mock_bin.mkdir()
            mock_sbatch = mock_bin / "sbatch"
            mock_sbatch.write_text(
                "#!/bin/bash\n"
                "echo 'Submitted batch job 525252'\n",
                encoding="utf-8",
            )
            mock_sbatch.chmod(0o755)

            result = subprocess.run(
                [
                    "/bin/bash",
                    str(self.cayuga_script_dir / "submit_open_data_downstream_only.sh"),
                    str(run_root),
                ],
                cwd=self.repo_root,
                capture_output=True,
                text=True,
                env={
                    **os.environ,
                    "PATH": "%s:%s" % (mock_bin, os.environ.get("PATH", "")),
                },
                check=True,
            )
            self.assertIn("local_raw_snapshot_ready=yes", result.stdout)
            self.assertIn("open_data_downstream_only_job_id=525252", result.stdout)
            self.assertEqual((art_root / "job_id.txt").read_text(encoding="utf-8").strip(), "525252")
            self.assertFalse((art_root / "COMPLETED").exists())
            self.assertFalse((art_root / "FAILED").exists())
            self.assertFalse((art_root / "current_step.txt").exists())
            self.assertFalse((art_root / "failed_step.txt").exists())

    def test_submit_open_data_finalize_clears_stale_markers_before_recording_job_id(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            run_root = Path(tmpdir)
            job_dir = run_root / "jobs"
            art_root = run_root / "artifacts" / "open_data_release"
            raw_root = run_root / "raw" / "public_open_data_snapshot"
            template_path = (
                run_root
                / "repo"
                / "scripts"
                / "cayuga"
                / "templates"
                / "open_data_finalize_template.sbatch.in"
            )
            job_dir.mkdir(parents=True)
            art_root.mkdir(parents=True)
            template_path.parent.mkdir(parents=True)
            template_path.write_text("#!/bin/bash\n# __RUN_ROOT__\n", encoding="utf-8")
            for label in ("openalex", "official_notices", "pubmed"):
                target = raw_root / label
                target.mkdir(parents=True, exist_ok=True)
                (target / "dummy.jsonl").write_text("{}\n", encoding="utf-8")
            (art_root / "COMPLETED").write_text("", encoding="utf-8")
            (art_root / "FAILED").write_text("", encoding="utf-8")
            (art_root / "current_step.txt").write_text("old_step\n", encoding="utf-8")
            (art_root / "failed_step.txt").write_text("old_failed_step\n", encoding="utf-8")

            mock_bin = run_root / "mock_bin"
            mock_bin.mkdir()
            mock_sbatch = mock_bin / "sbatch"
            mock_sbatch.write_text(
                "#!/bin/bash\n"
                "echo 'Submitted batch job 626262'\n",
                encoding="utf-8",
            )
            mock_sbatch.chmod(0o755)

            result = subprocess.run(
                [
                    "/bin/bash",
                    str(self.cayuga_script_dir / "submit_open_data_finalize.sh"),
                    str(run_root),
                ],
                cwd=self.repo_root,
                capture_output=True,
                text=True,
                env={
                    **os.environ,
                    "PATH": "%s:%s" % (mock_bin, os.environ.get("PATH", "")),
                },
                check=True,
            )
            self.assertIn("local_raw_snapshot_ready=yes", result.stdout)
            self.assertIn("open_data_finalize_job_id=626262", result.stdout)
            self.assertEqual((art_root / "job_id.txt").read_text(encoding="utf-8").strip(), "626262")
            self.assertFalse((art_root / "COMPLETED").exists())
            self.assertFalse((art_root / "FAILED").exists())
            self.assertFalse((art_root / "current_step.txt").exists())
            self.assertFalse((art_root / "failed_step.txt").exists())

    def test_submit_public_vendor_collection_clears_stale_markers_before_recording_job_id(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            run_root = Path(tmpdir)
            job_dir = run_root / "jobs"
            art_root = run_root / "artifacts" / "public_vendor_collection"
            template_path = (
                run_root
                / "repo"
                / "scripts"
                / "cayuga"
                / "templates"
                / "public_vendor_collection.sbatch.in"
            )
            job_dir.mkdir(parents=True)
            art_root.mkdir(parents=True)
            template_path.parent.mkdir(parents=True)
            template_path.write_text("#!/bin/bash\n# __RUN_ROOT__\n", encoding="utf-8")
            (art_root / "COMPLETED").write_text("", encoding="utf-8")
            (art_root / "FAILED").write_text("", encoding="utf-8")
            (art_root / "current_step.txt").write_text("old_step\n", encoding="utf-8")
            (art_root / "failed_step.txt").write_text("old_failed_step\n", encoding="utf-8")
            (art_root / "summary.txt").write_text("old_summary\n", encoding="utf-8")

            mock_bin = run_root / "mock_bin"
            mock_bin.mkdir()
            mock_sbatch = mock_bin / "sbatch"
            mock_sbatch.write_text(
                "#!/bin/bash\n"
                "echo 'Submitted batch job 727272'\n",
                encoding="utf-8",
            )
            mock_sbatch.chmod(0o755)

            result = subprocess.run(
                [
                    "/bin/bash",
                    str(self.cayuga_script_dir / "submit_public_vendor_collection.sh"),
                    str(run_root),
                ],
                cwd=self.repo_root,
                capture_output=True,
                text=True,
                env={
                    **os.environ,
                    "PATH": "%s:%s" % (mock_bin, os.environ.get("PATH", "")),
                },
                check=True,
            )
            self.assertIn("public_vendor_collection_job_id=727272", result.stdout)
            self.assertEqual((art_root / "job_id.txt").read_text(encoding="utf-8").strip(), "727272")
            self.assertFalse((art_root / "COMPLETED").exists())
            self.assertFalse((art_root / "FAILED").exists())
            self.assertFalse((art_root / "current_step.txt").exists())
            self.assertFalse((art_root / "failed_step.txt").exists())
            self.assertFalse((art_root / "summary.txt").exists())

    def test_submit_public_vendor_collection_clears_stale_markers_before_failed_sbatch(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            run_root = Path(tmpdir)
            job_dir = run_root / "jobs"
            art_root = run_root / "artifacts" / "public_vendor_collection"
            template_path = (
                run_root
                / "repo"
                / "scripts"
                / "cayuga"
                / "templates"
                / "public_vendor_collection.sbatch.in"
            )
            job_dir.mkdir(parents=True)
            art_root.mkdir(parents=True)
            template_path.parent.mkdir(parents=True)
            template_path.write_text("#!/bin/bash\n# __RUN_ROOT__\n", encoding="utf-8")
            for stale_name in (
                "COMPLETED",
                "FAILED",
                "current_step.txt",
                "failed_step.txt",
                "summary.txt",
                "job_id.txt",
            ):
                (art_root / stale_name).write_text("stale\n", encoding="utf-8")

            mock_bin = run_root / "mock_bin"
            mock_bin.mkdir()
            mock_sbatch = mock_bin / "sbatch"
            mock_sbatch.write_text(
                "#!/bin/bash\n"
                "echo 'sbatch unavailable' >&2\n"
                "exit 1\n",
                encoding="utf-8",
            )
            mock_sbatch.chmod(0o755)

            result = subprocess.run(
                [
                    "/bin/bash",
                    str(self.cayuga_script_dir / "submit_public_vendor_collection.sh"),
                    str(run_root),
                ],
                cwd=self.repo_root,
                capture_output=True,
                text=True,
                env={
                    **os.environ,
                    "PATH": "%s:%s" % (mock_bin, os.environ.get("PATH", "")),
                },
                check=False,
            )
            self.assertNotEqual(result.returncode, 0)
            for stale_name in (
                "COMPLETED",
                "FAILED",
                "current_step.txt",
                "failed_step.txt",
                "summary.txt",
                "job_id.txt",
            ):
                self.assertFalse((art_root / stale_name).exists())

    def test_watch_open_data_finalize_prefers_failed_marker_over_completed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            run_root = Path(tmpdir)
            art_root = run_root / "artifacts" / "open_data_release"
            art_root.mkdir(parents=True)
            (art_root / "job_id.txt").write_text("12345\n", encoding="utf-8")
            (art_root / "COMPLETED").write_text("", encoding="utf-8")
            (art_root / "FAILED").write_text("", encoding="utf-8")
            (art_root / "failed_step.txt").write_text("build_report\n", encoding="utf-8")

            result = subprocess.run(
                [
                    "/bin/bash",
                    str(self.cayuga_script_dir / "watch_open_data_finalize.sh"),
                    str(run_root),
                    "0",
                    "1",
                ],
                cwd=self.repo_root,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(result.returncode, 1)
            self.assertIn("failed_step=build_report", result.stdout)
            self.assertIn("open_data_finalize_status=FAILED", result.stdout)

    def test_watch_public_vendor_collection_prefers_failed_marker_over_completed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            run_root = Path(tmpdir)
            art_root = run_root / "artifacts" / "public_vendor_collection"
            art_root.mkdir(parents=True)
            (art_root / "job_id.txt").write_text("12345\n", encoding="utf-8")
            (art_root / "COMPLETED").write_text("", encoding="utf-8")
            (art_root / "FAILED").write_text("", encoding="utf-8")
            (art_root / "failed_step.txt").write_text("fetch_pubmed\n", encoding="utf-8")

            result = subprocess.run(
                [
                    "/bin/bash",
                    str(self.cayuga_script_dir / "watch_public_vendor_collection.sh"),
                    str(run_root),
                    "0",
                    "1",
                ],
                cwd=self.repo_root,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(result.returncode, 1)
            self.assertIn("failed_step=fetch_pubmed", result.stdout)
            self.assertIn("public_vendor_collection_status=FAILED", result.stdout)

    def test_collect_morning_status_reports_effective_artifact_statuses(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            run_root = Path(tmpdir)
            artifacts_root = run_root / "artifacts"
            logs_root = run_root / "logs"
            logs_root.mkdir(parents=True)
            (logs_root / "sample.out").write_text("hello\nworld\n", encoding="utf-8")

            preflight = artifacts_root / "preflight"
            sample_stress = artifacts_root / "sample_stress"
            real_release = artifacts_root / "real_release"
            public_vendor = artifacts_root / "public_vendor_collection"
            open_data = artifacts_root / "open_data_release"
            for path in (preflight, sample_stress, real_release, public_vendor, open_data):
                path.mkdir(parents=True)

            (preflight / "job_id.txt").write_text("111\n", encoding="utf-8")
            (preflight / "COMPLETED").write_text("", encoding="utf-8")

            (sample_stress / "job_id.txt").write_text("222\n", encoding="utf-8")

            (real_release / "job_id.txt").write_text("333\n", encoding="utf-8")
            (real_release / "current_step.txt").write_text("ingest_openalex 17/2127 raw_records=123\n", encoding="utf-8")

            (public_vendor / "job_id.txt").write_text("444\n", encoding="utf-8")
            (public_vendor / "COMPLETED").write_text("", encoding="utf-8")
            (public_vendor / "FAILED").write_text("", encoding="utf-8")
            (public_vendor / "failed_step.txt").write_text("fetch_pubmed\n", encoding="utf-8")
            (public_vendor / "summary.txt").write_text("rows=10\n", encoding="utf-8")

            (open_data / "job_id.txt").write_text("555\n", encoding="utf-8")
            (open_data / "COMPLETED").write_text("", encoding="utf-8")

            (sample_stress / "checksums.tsv").write_text(
                "sha1\talpha\tbeta\tgamma\nsha2\talpha\tbeta\tgamma\n",
                encoding="utf-8",
            )

            result = subprocess.run(
                [
                    "/bin/bash",
                    str(self.cayuga_script_dir / "collect_morning_status.sh"),
                    str(run_root),
                ],
                cwd=self.repo_root,
                capture_output=True,
                text=True,
                check=True,
            )
            self.assertIn("preflight_job_id=111", result.stdout)
            self.assertIn("public_vendor_collection_job_id=444", result.stdout)
            self.assertIn("open_data_release_job_id=555", result.stdout)
            self.assertIn("artifact_status\tpreflight\tCOMPLETED", result.stdout)
            self.assertIn("artifact_status\tsample_stress\tJOB_RECORDED", result.stdout)
            self.assertIn("artifact_status\treal_release\tIN_PROGRESS", result.stdout)
            self.assertIn(
                "current_step\treal_release\tingest_openalex 17/2127 raw_records=123",
                result.stdout,
            )
            self.assertIn("artifact_status\tpublic_vendor_collection\tFAILED", result.stdout)
            self.assertIn("failed_step\tpublic_vendor_collection\tfetch_pubmed", result.stdout)
            self.assertIn(
                "summary_file\tpublic_vendor_collection\t%s"
                % (public_vendor / "summary.txt"),
                result.stdout,
            )
            self.assertIn("artifact_status\topen_data_release\tCOMPLETED", result.stdout)
            self.assertIn("checksum_rows=2", result.stdout)
            self.assertIn("checksum_unique_tuples=1", result.stdout)
            report_path = artifacts_root / "morning_status_report.txt"
            self.assertTrue(report_path.exists())
            self.assertIn("artifact_status\tpublic_vendor_collection\tFAILED", report_path.read_text(encoding="utf-8"))

    def test_mixed_source_classification_override_and_staging_scripts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            mixed_root = root / "mixed_sources"
            raw_root = root / "raw_snapshot"
            nested_dir = mixed_root / "nested"
            nested_dir.mkdir(parents=True)

            write_jsonl(
                mixed_root / "openalex-works.jsonl",
                [
                    {
                        "ids": {"doi": "https://doi.org/10.5000/openalex.1"},
                        "display_name": "OpenAlex-like row",
                        "abstract_inverted_index": {"cell": [0], "atlas": [1]},
                        "publication_year": 2024,
                        "type_crossref": "journal-article",
                        "authorships": [],
                        "concepts": [{"display_name": "Biology", "score": 0.9}],
                    }
                ],
            )
            write_csv(
                mixed_root / "crossref_retractions.csv",
                rows=[
                    {
                        "doi": "10.5000/openalex.1",
                        "notice_type": "retraction",
                        "notice_date": "2025-01-01",
                        "source_name": "Crossref",
                        "source_url": "https://example.org/retraction/1",
                    }
                ],
                fieldnames=[
                    "doi",
                    "notice_type",
                    "notice_date",
                    "source_name",
                    "source_url",
                ],
            )
            (mixed_root / "pubmed_baseline.xml").write_text(
                """<PubmedArticleSet><PubmedArticle><MedlineCitation><PMID>123</PMID></MedlineCitation></PubmedArticle></PubmedArticleSet>""",
                encoding="utf-8",
            )
            write_csv(
                nested_dir / "ambiguous.csv",
                rows=[{"doi": "10.5000/ambiguous.1", "value": "mystery"}],
                fieldnames=["doi", "value"],
            )

            classification_path = mixed_root / "source_classification.tsv"
            self.run_cayuga_script(
                "classify_mixed_sources.sh",
                str(mixed_root),
                str(classification_path),
            )

            with classification_path.open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle, delimiter="\t"))
            by_path = {row["relative_path"]: row for row in rows}

            self.assertEqual(by_path["openalex-works.jsonl"]["bucket"], "openalex")
            self.assertEqual(
                by_path["crossref_retractions.csv"]["bucket"], "official_notices"
            )
            self.assertEqual(by_path["pubmed_baseline.xml"]["bucket"], "pubmed")
            self.assertEqual(by_path["nested/ambiguous.csv"]["bucket"], "unknown")
            self.assertNotIn("source_classification.tsv", by_path)

            review = self.run_cayuga_script(
                "review_unknown_classification.sh",
                str(mixed_root),
                str(classification_path),
            )
            self.assertIn("relative_path=nested/ambiguous.csv", review.stdout)
            self.assertIn("sample_begin", review.stdout)

            override_path = mixed_root / "classification_overrides.tsv"
            self.run_cayuga_script(
                "write_unknown_override_template.sh",
                str(classification_path),
                str(override_path),
            )
            override_path.write_text(
                "relative_path\tbucket\treason\n"
                "nested/ambiguous.csv\tofficial_notices\tmanual_notice_override\n",
                encoding="utf-8",
            )

            merged_path = mixed_root / "source_classification.merged.tsv"
            self.run_cayuga_script(
                "apply_classification_overrides.sh",
                str(classification_path),
                str(override_path),
                str(merged_path),
            )
            with merged_path.open("r", encoding="utf-8", newline="") as handle:
                merged_rows = list(csv.DictReader(handle, delimiter="\t"))
            merged_by_path = {row["relative_path"]: row for row in merged_rows}
            self.assertEqual(
                merged_by_path["nested/ambiguous.csv"]["bucket"], "official_notices"
            )
            self.assertNotIn("classification_overrides.tsv", merged_by_path)

            staged = self.run_cayuga_script(
                "stage_mixed_sources_into_raw_snapshot.sh",
                str(mixed_root),
                str(raw_root),
                "copy",
                str(classification_path),
                str(override_path),
            )
            self.assertIn("local_raw_snapshot_ready=yes", staged.stdout)
            self.assertTrue((raw_root / "openalex" / "openalex-works.jsonl").exists())
            self.assertTrue(
                (raw_root / "official_notices" / "crossref_retractions.csv").exists()
            )
            self.assertTrue(
                (raw_root / "official_notices" / "nested__ambiguous.csv").exists()
            )
            self.assertTrue((raw_root / "pubmed" / "pubmed_baseline.xml").exists())

            readiness = self.run_cayuga_script(
                "check_local_raw_snapshot.sh",
                str(raw_root),
            )
            self.assertIn("local_raw_snapshot_ready=yes", readiness.stdout)

    def test_local_real_snapshot_pipeline_uses_isolated_runtime_root(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            raw_root = root / "raw_snapshot"
            work_root = root / "work"
            snapshot_id = "isolated_pipeline_%s" % root.name.replace("-", "_")

            self.run_cayuga_script("scaffold_local_raw_snapshot.sh", str(raw_root))
            write_jsonl(
                raw_root / "openalex" / "works.jsonl.gz",
                [
                    {
                        "doi": "10.7000/pipeline.1",
                        "title": "Pipeline article",
                        "abstract": "Proteomics benchmark article",
                        "publication_date": "2022-04-03",
                        "type": "article",
                        "concepts": [{"display_name": "Biology", "score": 0.91}],
                        "authorships": [],
                    }
                ],
            )
            write_csv(
                raw_root / "official_notices" / "notices.csv.gz",
                rows=[
                    {
                        "doi": "10.7000/pipeline.1",
                        "notice_type": "retraction",
                        "notice_date": "2023-01-01",
                        "source_name": "Crossref",
                        "source_url": "https://example.org/pipeline/retraction",
                    }
                ],
                fieldnames=[
                    "doi",
                    "notice_type",
                    "notice_date",
                    "source_name",
                    "source_url",
                ],
            )
            (raw_root / "pubmed" / "pubmed.xml").write_text(
                """<PubmedArticleSet><PubmedArticle><MedlineCitation><PMID>999</PMID><Article><ELocationID EIdType="doi">10.7000/pipeline.1</ELocationID></Article></MedlineCitation></PubmedArticle></PubmedArticleSet>""",
                encoding="utf-8",
            )

            result = self.run_cayuga_script(
                "run_local_real_snapshot_pipeline.sh",
                str(raw_root),
                str(work_root),
                snapshot_id,
            )

            runtime_root = work_root / "runtime_root"
            canonical_root = runtime_root / "data" / "normalized" / snapshot_id / "canonical"
            release_dir = work_root / "release"
            repo_side_root = self.repo_root / "data" / "normalized" / snapshot_id

            self.assertIn(f"runtime_root={runtime_root}", result.stdout)
            self.assertTrue(canonical_root.exists())
            self.assertTrue((canonical_root / "articles" / "part-00000.jsonl.gz").exists())
            self.assertTrue((release_dir / "benchmark_v1.jsonl").exists())
            self.assertTrue((release_dir / "splits.json").exists())
            self.assertTrue((release_dir / "leakage_report.json").exists())
            self.assertFalse(repo_side_root.exists())

    def test_openalex_plain_gz_and_sidecars_are_ingest_ready(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            raw_root = root / "raw_snapshot"
            self.run_cayuga_script("scaffold_local_raw_snapshot.sh", str(raw_root))

            write_jsonl(
                raw_root / "openalex" / "works_part_000.gz",
                [
                    {
                        "doi": "10.8100/openalex.gz.1",
                        "display_name": "Gzip OpenAlex row",
                        "abstract": "Life science benchmark row",
                        "publication_date": "2024-03-03",
                        "type": "article",
                        "concepts": [{"display_name": "Biology", "score": 0.93}],
                        "authorships": [],
                    }
                ],
            )
            (raw_root / "openalex" / "manifest").write_text("ok\n", encoding="utf-8")
            (raw_root / "openalex" / "LICENSE.txt").write_text("license\n", encoding="utf-8")
            (raw_root / "openalex" / "RELEASE_NOTES.txt").write_text("notes\n", encoding="utf-8")
            (raw_root / "openalex" / "source_versions.json").write_text("{}", encoding="utf-8")
            (raw_root / "openalex" / "sha256_manifest.tsv").write_text("x\n", encoding="utf-8")

            write_csv(
                raw_root / "official_notices" / "notices.csv.gz",
                rows=[
                    {
                        "doi": "10.8100/openalex.gz.1",
                        "notice_type": "retraction",
                        "notice_date": "2025-01-01",
                        "source_name": "Crossref",
                        "source_url": "https://example.org/retraction",
                    }
                ],
                fieldnames=[
                    "doi",
                    "notice_type",
                    "notice_date",
                    "source_name",
                    "source_url",
                ],
            )
            (raw_root / "official_notices" / "source_versions.json").write_text(
                "{}", encoding="utf-8"
            )
            (raw_root / "official_notices" / "sha256_manifest.tsv").write_text(
                "x\n", encoding="utf-8"
            )

            (raw_root / "pubmed" / "source_versions.json").write_text("{}", encoding="utf-8")
            (raw_root / "pubmed" / "sha256_manifest.tsv").write_text("x\n", encoding="utf-8")
            (raw_root / "pubmed" / "pubmed.xml").write_text(
                """<PubmedArticleSet><PubmedArticle><MedlineCitation><PMID>42</PMID><Article><ELocationID EIdType="doi">10.8100/openalex.gz.1</ELocationID></Article></MedlineCitation></PubmedArticle></PubmedArticleSet>""",
                encoding="utf-8",
            )

            readiness = self.run_cayuga_script("check_local_raw_snapshot.sh", str(raw_root))
            self.assertIn("local_raw_snapshot_ready=yes", readiness.stdout)

            register_snapshot(
                snapshot_id="snapshot_openalex_gz",
                raw_root=raw_root,
                root_dir=root,
                snapshot_date="2026-04-09",
            )
            ingest_result = ingest_snapshot("snapshot_openalex_gz", "openalex_bulk", root_dir=root)
            self.assertEqual(ingest_result["processed_files"], 1)

    def test_vendor_archive_extract_and_stage_pipeline(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            vendor_root = root / "vendor_archive"
            raw_root = root / "raw_snapshot"

            openalex_dir = vendor_root / "openalex" / "2026-03" / "data" / "works" / "updated_date=2026-03-31"
            openalex_dir.mkdir(parents=True, exist_ok=True)
            write_jsonl(
                openalex_dir / "000_part_000.gz",
                [
                    {
                        "doi": "10.9100/vendor.1",
                        "title": "Vendor staged article",
                        "abstract": "Proteomics article",
                        "publication_date": "2025-10-01",
                        "type": "article",
                        "concepts": [{"display_name": "Biology", "score": 0.92}],
                        "authorships": [],
                    }
                ],
            )
            openalex_batch = vendor_root / "openalex" / "2026-03"
            (openalex_batch / "manifest").write_text("manifest\n", encoding="utf-8")
            (openalex_batch / "LICENSE.txt").write_text("license\n", encoding="utf-8")
            (openalex_batch / "RELEASE_NOTES.txt").write_text("notes\n", encoding="utf-8")
            (openalex_batch / "source_versions.json").write_text("{}", encoding="utf-8")
            (openalex_batch / "sha256_manifest.tsv").write_text("x\n", encoding="utf-8")

            crossref_dir = vendor_root / "crossref" / "2026-03"
            crossref_dir.mkdir(parents=True, exist_ok=True)
            crossref_payload = (
                json.dumps(
                    {
                        "DOI": "10.9999/notice.1",
                        "type": "retraction",
                        "relation": {
                            "is-retraction-of": [
                                {"id": "10.9100/vendor.1", "id-type": "doi"}
                            ]
                        },
                        "created": {"date-parts": [[2026, 3, 20]]},
                        "URL": "https://doi.org/10.9999/notice.1",
                    }
                )
                + "\n"
                + json.dumps(
                    {
                        "DOI": "10.9999/notice.future",
                        "type": "retraction",
                        "relation": {
                            "is-retraction-of": [
                                {"id": "10.9100/vendor.1", "id-type": "doi"}
                            ]
                        },
                        "created": {"date-parts": [[2026, 4, 2]]},
                        "URL": "https://doi.org/10.9999/notice.future",
                    }
                )
                + "\n"
            ).encode("utf-8")
            with tarfile.open(crossref_dir / "all.json.tar.gz", "w:gz") as archive:
                info = tarfile.TarInfo("part-000.json")
                info.size = len(crossref_payload)
                archive.addfile(info, io.BytesIO(crossref_payload))
            (crossref_dir / "source_versions.json").write_text("{}", encoding="utf-8")
            (crossref_dir / "sha256_manifest.tsv").write_text("x\n", encoding="utf-8")

            rw_dir = vendor_root / "retraction_watch" / "2026-03-31"
            rw_dir.mkdir(parents=True, exist_ok=True)
            write_csv(
                rw_dir / "retraction_watch.csv",
                rows=[
                    {
                        "OriginalPaperDOI": "10.9100/vendor.1",
                        "RetractionNature": "Correction",
                        "RetractionDate": "2026-03-05",
                        "URLS": "https://retraction.example/1",
                    },
                    {
                        "OriginalPaperDOI": "10.9100/vendor.1",
                        "RetractionNature": "Retraction",
                        "RetractionDate": "2026-04-06",
                        "URLS": "https://retraction.example/future",
                    },
                ],
                fieldnames=["OriginalPaperDOI", "RetractionNature", "RetractionDate", "URLS"],
            )
            (rw_dir / "source_versions.json").write_text("{}", encoding="utf-8")
            (rw_dir / "sha256_manifest.tsv").write_text("x\n", encoding="utf-8")

            pubmed_baseline_dir = vendor_root / "pubmed" / "baseline" / "2026"
            pubmed_baseline_dir.mkdir(parents=True, exist_ok=True)
            with gzip.open(pubmed_baseline_dir / "pubmed26n0001.xml.gz", "wt", encoding="utf-8") as handle:
                handle.write("<PubmedArticleSet/>")
            (pubmed_baseline_dir / "source_versions.json").write_text("{}", encoding="utf-8")
            (pubmed_baseline_dir / "sha256_manifest.tsv").write_text("x\n", encoding="utf-8")

            pubmed_update_dir = vendor_root / "pubmed" / "updatefiles" / "2026-03"
            pubmed_update_dir.mkdir(parents=True, exist_ok=True)
            with gzip.open(pubmed_update_dir / "pubmed26n1001.xml.gz", "wt", encoding="utf-8") as handle:
                handle.write("<PubmedArticleSet/>")
            (pubmed_update_dir / "source_versions.json").write_text("{}", encoding="utf-8")
            (pubmed_update_dir / "sha256_manifest.tsv").write_text("x\n", encoding="utf-8")

            vendor_validation = validate_vendor_archive(vendor_root, "2026-03-freeze")
            self.assertTrue(vendor_validation["passed"])

            crossref_export = root / "crossref_updates_2026-03.csv.gz"
            crossref_result = extract_crossref_official_notices(
                crossref_dir / "all.json.tar.gz",
                crossref_export,
                snapshot_label="2026-03-freeze",
            )
            self.assertEqual(crossref_result["row_count"], 1)
            with gzip.open(crossref_export, "rt", encoding="utf-8", newline="") as handle:
                crossref_rows = list(csv.DictReader(handle))
            self.assertEqual(crossref_rows[0]["doi"], "10.9100/vendor.1")
            self.assertEqual(crossref_rows[0]["notice_type"], "retraction")

            rw_export = root / "retraction_watch_2026-03-31.csv.gz"
            rw_result = extract_retraction_watch_csv(
                rw_dir / "retraction_watch.csv",
                rw_export,
                snapshot_label="2026-03-freeze",
            )
            self.assertEqual(rw_result["row_count"], 1)
            with gzip.open(rw_export, "rt", encoding="utf-8", newline="") as handle:
                rw_rows = list(csv.DictReader(handle))
            self.assertEqual(rw_rows[0]["notice_type"], "major_correction")

            stage_result = stage_vendor_archive_to_raw_snapshot(
                vendor_root=vendor_root,
                raw_root=raw_root,
                snapshot_label="2026-03-freeze",
                mode="symlink",
            )
            self.assertEqual(stage_result["crossref_notice_rows"], 1)
            self.assertEqual(stage_result["retraction_watch_rows"], 1)
            self.assertTrue((raw_root / "openalex" / "data" / "works" / "updated_date=2026-03-31" / "000_part_000.gz").exists())
            self.assertTrue(
                (raw_root / "openalex" / "data" / "works" / "updated_date=2026-03-31" / "000_part_000.gz").is_symlink()
            )
            self.assertTrue((raw_root / "openalex" / "source_versions.json").exists())
            self.assertTrue((raw_root / "official_notices" / "crossref_updates_2026-03.csv.gz").exists())
            self.assertTrue((raw_root / "official_notices" / "retraction_watch_2026-03-31.csv.gz").exists())
            self.assertTrue((raw_root / "official_notices" / "source_versions.json").exists())
            self.assertTrue((raw_root / "pubmed" / "baseline" / "pubmed26n0001.xml.gz").exists())
            self.assertTrue((raw_root / "pubmed" / "baseline" / "pubmed26n0001.xml.gz").is_symlink())
            self.assertTrue((raw_root / "pubmed" / "updatefiles" / "pubmed26n1001.xml.gz").exists())
            readiness = self.run_cayuga_script("check_local_raw_snapshot.sh", str(raw_root))
            self.assertIn("local_raw_snapshot_ready=yes", readiness.stdout)

    def test_vendor_archive_shell_pipeline_runs_end_to_end(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            vendor_root = root / "vendor_archive"
            raw_root = root / "raw_snapshot"
            work_root = root / "work"
            snapshot_label = "2026-03-freeze"
            snapshot_id = "vendor_shell_snapshot"

            openalex_dir = vendor_root / "openalex" / "2026-03" / "data" / "works" / "updated_date=2026-03-31"
            openalex_dir.mkdir(parents=True, exist_ok=True)
            write_jsonl(
                openalex_dir / "000_part_000.gz",
                [
                    {
                        "doi": "10.9200/vendor.shell.1",
                        "title": "Vendor shell article",
                        "abstract": "Shell pipeline article",
                        "publication_date": "2025-11-01",
                        "type": "article",
                        "concepts": [{"display_name": "Biology", "score": 0.91}],
                        "authorships": [],
                    }
                ],
            )
            openalex_batch = vendor_root / "openalex" / "2026-03"
            (openalex_batch / "manifest").write_text("manifest\n", encoding="utf-8")
            (openalex_batch / "LICENSE.txt").write_text("license\n", encoding="utf-8")
            (openalex_batch / "RELEASE_NOTES.txt").write_text("notes\n", encoding="utf-8")
            (openalex_batch / "source_versions.json").write_text("{}", encoding="utf-8")
            (openalex_batch / "sha256_manifest.tsv").write_text("x\n", encoding="utf-8")

            crossref_dir = vendor_root / "crossref" / "2026-03"
            crossref_dir.mkdir(parents=True, exist_ok=True)
            crossref_payload = (
                json.dumps(
                    {
                        "DOI": "10.9999/notice.shell",
                        "type": "retraction",
                        "relation": {
                            "is-retraction-of": [
                                {"id": "10.9200/vendor.shell.1", "id-type": "doi"}
                            ]
                        },
                        "created": {"date-parts": [[2026, 3, 25]]},
                        "URL": "https://doi.org/10.9999/notice.shell",
                    }
                )
                + "\n"
            ).encode("utf-8")
            with tarfile.open(crossref_dir / "all.json.tar.gz", "w:gz") as archive:
                info = tarfile.TarInfo("part-000.json")
                info.size = len(crossref_payload)
                archive.addfile(info, io.BytesIO(crossref_payload))
            (crossref_dir / "source_versions.json").write_text("{}", encoding="utf-8")
            (crossref_dir / "sha256_manifest.tsv").write_text("x\n", encoding="utf-8")

            rw_dir = vendor_root / "retraction_watch" / "2026-03-31"
            rw_dir.mkdir(parents=True, exist_ok=True)
            write_csv(
                rw_dir / "retraction_watch.csv",
                rows=[
                    {
                        "OriginalPaperDOI": "10.9200/vendor.shell.1",
                        "RetractionNature": "Retraction",
                        "RetractionDate": "2026-03-20",
                        "URLS": "https://retraction.example/shell",
                    }
                ],
                fieldnames=["OriginalPaperDOI", "RetractionNature", "RetractionDate", "URLS"],
            )
            (rw_dir / "source_versions.json").write_text("{}", encoding="utf-8")
            (rw_dir / "sha256_manifest.tsv").write_text("x\n", encoding="utf-8")

            pubmed_baseline_dir = vendor_root / "pubmed" / "baseline" / "2026"
            pubmed_baseline_dir.mkdir(parents=True, exist_ok=True)
            with gzip.open(pubmed_baseline_dir / "pubmed26n0001.xml.gz", "wt", encoding="utf-8") as handle:
                handle.write(
                    "<PubmedArticleSet><PubmedArticle><MedlineCitation><PMID>88</PMID><Article><ELocationID EIdType=\"doi\">10.9200/vendor.shell.1</ELocationID></Article></MedlineCitation></PubmedArticle></PubmedArticleSet>"
                )
            (pubmed_baseline_dir / "source_versions.json").write_text("{}", encoding="utf-8")
            (pubmed_baseline_dir / "sha256_manifest.tsv").write_text("x\n", encoding="utf-8")

            pubmed_update_dir = vendor_root / "pubmed" / "updatefiles" / "2026-03"
            pubmed_update_dir.mkdir(parents=True, exist_ok=True)
            with gzip.open(pubmed_update_dir / "pubmed26n1001.xml.gz", "wt", encoding="utf-8") as handle:
                handle.write("<PubmedArticleSet/>")
            (pubmed_update_dir / "source_versions.json").write_text("{}", encoding="utf-8")
            (pubmed_update_dir / "sha256_manifest.tsv").write_text("x\n", encoding="utf-8")

            result = self.run_raw_snapshot_script(
                "run_vendor_archive_pipeline.sh",
                str(vendor_root),
                str(raw_root),
                str(work_root),
                snapshot_label,
                "copy",
                snapshot_id,
            )
            self.assertIn(f"snapshot_id={snapshot_id}", result.stdout)
            self.assertTrue((work_root / "release" / "benchmark_v1.jsonl").exists())
            self.assertTrue((work_root / "release" / "splits.json").exists())
            self.assertTrue((work_root / "release" / "leakage_report.json").exists())
            self.assertTrue((raw_root / "official_notices" / "crossref_updates_2026-03.csv.gz").exists())

    def test_vendor_archive_open_data_only_pipeline_without_crossref(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            vendor_root = root / "vendor_archive"
            raw_root = root / "raw_snapshot"
            work_root = root / "work"
            snapshot_label = "2026-03-freeze"
            snapshot_id = "vendor_open_data_snapshot"

            openalex_dir = vendor_root / "openalex" / "2026-03" / "data" / "works" / "updated_date=2026-03-31"
            openalex_dir.mkdir(parents=True, exist_ok=True)
            write_jsonl(
                openalex_dir / "000_part_000.gz",
                [
                    {
                        "doi": "10.9300/vendor.free.1",
                        "title": "Vendor free article",
                        "abstract": "Open data only article",
                        "publication_date": "2025-12-01",
                        "type": "article",
                        "concepts": [{"display_name": "Biology", "score": 0.95}],
                        "authorships": [],
                    }
                ],
            )
            openalex_batch = vendor_root / "openalex" / "2026-03"
            (openalex_batch / "manifest").write_text("manifest\n", encoding="utf-8")
            (openalex_batch / "LICENSE.txt").write_text("license\n", encoding="utf-8")
            (openalex_batch / "RELEASE_NOTES.txt").write_text("notes\n", encoding="utf-8")
            (openalex_batch / "source_versions.json").write_text("{}", encoding="utf-8")
            (openalex_batch / "sha256_manifest.tsv").write_text("x\n", encoding="utf-8")

            rw_dir = vendor_root / "retraction_watch" / "2026-03-31"
            rw_dir.mkdir(parents=True, exist_ok=True)
            write_csv(
                rw_dir / "retraction_watch.csv",
                rows=[
                    {
                        "OriginalPaperDOI": "10.9300/vendor.free.1",
                        "RetractionNature": "Retraction",
                        "RetractionDate": "2026-03-20",
                        "URLS": "https://retraction.example/free",
                    }
                ],
                fieldnames=["OriginalPaperDOI", "RetractionNature", "RetractionDate", "URLS"],
            )
            (rw_dir / "source_versions.json").write_text("{}", encoding="utf-8")
            (rw_dir / "sha256_manifest.tsv").write_text("x\n", encoding="utf-8")

            pubmed_baseline_dir = vendor_root / "pubmed" / "baseline" / "2026"
            pubmed_baseline_dir.mkdir(parents=True, exist_ok=True)
            with gzip.open(pubmed_baseline_dir / "pubmed26n0001.xml.gz", "wt", encoding="utf-8") as handle:
                handle.write(
                    "<PubmedArticleSet><PubmedArticle><MedlineCitation><PMID>77</PMID><Article><ELocationID EIdType=\"doi\">10.9300/vendor.free.1</ELocationID></Article></MedlineCitation></PubmedArticle></PubmedArticleSet>"
                )
            (pubmed_baseline_dir / "source_versions.json").write_text("{}", encoding="utf-8")
            (pubmed_baseline_dir / "sha256_manifest.tsv").write_text("x\n", encoding="utf-8")

            pubmed_update_dir = vendor_root / "pubmed" / "updatefiles" / "2026-03"
            pubmed_update_dir.mkdir(parents=True, exist_ok=True)
            with gzip.open(pubmed_update_dir / "pubmed26n1001.xml.gz", "wt", encoding="utf-8") as handle:
                handle.write("<PubmedArticleSet/>")
            (pubmed_update_dir / "source_versions.json").write_text("{}", encoding="utf-8")
            (pubmed_update_dir / "sha256_manifest.tsv").write_text("x\n", encoding="utf-8")

            vendor_validation = validate_vendor_archive(
                vendor_root,
                "2026-03-freeze",
                allow_missing_crossref=True,
            )
            self.assertTrue(vendor_validation["passed"])

            stage_result = stage_vendor_archive_to_raw_snapshot(
                vendor_root=vendor_root,
                raw_root=raw_root,
                snapshot_label="2026-03-freeze",
                mode="symlink",
                allow_missing_crossref=True,
            )
            self.assertEqual(stage_result["crossref_notice_rows"], 0)
            self.assertTrue(stage_result["crossref_skipped"])
            self.assertTrue((raw_root / "official_notices" / "retraction_watch_2026-03-31.csv.gz").exists())
            self.assertTrue(
                (raw_root / "openalex" / "data" / "works" / "updated_date=2026-03-31" / "000_part_000.gz").is_symlink()
            )
            self.assertTrue((raw_root / "pubmed" / "baseline" / "pubmed26n0001.xml.gz").is_symlink())

            result = self.run_raw_snapshot_script(
                "run_vendor_archive_pipeline.sh",
                str(vendor_root),
                str(raw_root),
                str(work_root),
                snapshot_label,
                "symlink",
                snapshot_id,
                env={**dict(os.environ), "ALLOW_MISSING_CROSSREF": "1"},
            )
            self.assertIn(f"snapshot_id={snapshot_id}", result.stdout)
            self.assertTrue((work_root / "release" / "benchmark_v1.jsonl").exists())
            self.assertFalse((raw_root / "official_notices" / "crossref_updates_2026-03.csv.gz").exists())
            self.assertTrue((raw_root / "official_notices" / "retraction_watch_2026-03-31.csv.gz").exists())
            self.assertTrue(
                (raw_root / "openalex" / "data" / "works" / "updated_date=2026-03-31" / "000_part_000.gz").is_symlink()
            )

    def test_retraction_watch_collection_checks_out_commit_at_freeze_date(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source_repo = root / "retraction_watch_repo"
            source_repo.mkdir()

            subprocess.run(["git", "init"], cwd=source_repo, capture_output=True, text=True, check=True)
            subprocess.run(["git", "config", "user.email", "test@example.org"], cwd=source_repo, check=True)
            subprocess.run(["git", "config", "user.name", "Test User"], cwd=source_repo, check=True)

            fieldnames = ["OriginalPaperDOI", "RetractionNature", "RetractionDate", "URLS"]
            write_csv(
                source_repo / "retraction_watch.csv",
                rows=[
                    {
                        "OriginalPaperDOI": "10.9400/rw.old",
                        "RetractionNature": "Retraction",
                        "RetractionDate": "2026-03-20",
                        "URLS": "https://retraction.example/old",
                    }
                ],
                fieldnames=fieldnames,
            )
            commit_env = {
                **os.environ,
                "GIT_AUTHOR_DATE": "2026-03-15T12:00:00+0000",
                "GIT_COMMITTER_DATE": "2026-03-15T12:00:00+0000",
            }
            subprocess.run(["git", "add", "retraction_watch.csv"], cwd=source_repo, check=True)
            subprocess.run(
                ["git", "commit", "-m", "pre-freeze retraction watch export"],
                cwd=source_repo,
                env=commit_env,
                capture_output=True,
                text=True,
                check=True,
            )
            old_sha = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=source_repo,
                capture_output=True,
                text=True,
                check=True,
            ).stdout.strip()

            write_csv(
                source_repo / "retraction_watch.csv",
                rows=[
                    {
                        "OriginalPaperDOI": "10.9400/rw.old",
                        "RetractionNature": "Retraction",
                        "RetractionDate": "2026-03-20",
                        "URLS": "https://retraction.example/old",
                    },
                    {
                        "OriginalPaperDOI": "10.9400/rw.backfill",
                        "RetractionNature": "Retraction",
                        "RetractionDate": "2026-03-25",
                        "URLS": "https://retraction.example/backfill",
                    },
                ],
                fieldnames=fieldnames,
            )
            future_env = {
                **os.environ,
                "GIT_AUTHOR_DATE": "2026-04-15T12:00:00+0000",
                "GIT_COMMITTER_DATE": "2026-04-15T12:00:00+0000",
            }
            subprocess.run(["git", "add", "retraction_watch.csv"], cwd=source_repo, check=True)
            subprocess.run(
                ["git", "commit", "-m", "post-freeze backfill"],
                cwd=source_repo,
                env=future_env,
                capture_output=True,
                text=True,
                check=True,
            )

            vendor_root = root / "vendor_archive"
            result = self.run_raw_snapshot_script(
                "collect_retraction_watch_csv.sh",
                str(vendor_root),
                "2026-03-31",
                env={**os.environ, "RETRACTION_WATCH_REPO_URL": str(source_repo)},
            )
            self.assertIn("retraction_watch/2026-03-31", result.stdout)
            collected_csv = vendor_root / "retraction_watch" / "2026-03-31" / "retraction_watch.csv"
            collected_text = collected_csv.read_text(encoding="utf-8")
            self.assertIn("10.9400/rw.old", collected_text)
            self.assertNotIn("10.9400/rw.backfill", collected_text)
            source_versions = read_json(
                vendor_root / "retraction_watch" / "2026-03-31" / "source_versions.json"
            )
            self.assertEqual(source_versions["batches"][0]["git_sha"], old_sha)

    def test_openalex_collection_prunes_partitions_after_freeze_end(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            vendor_root = root / "vendor_archive"
            mock_bin = root / "mock_bin"
            mock_bin.mkdir()
            mock_aws = mock_bin / "aws"
            mock_aws.write_text(
                "#!/bin/bash\n"
                "set -euo pipefail\n"
                "if [ \"$1\" != \"s3\" ]; then exit 2; fi\n"
                "if [ \"$2\" = \"sync\" ]; then\n"
                "  dest=\"$4\"\n"
                "  mkdir -p \"$dest/updated_date=2026-03-31\" \"$dest/updated_date=2026-04-01\"\n"
                "  printf 'past\\n' > \"$dest/updated_date=2026-03-31/part.gz\"\n"
                "  printf 'future\\n' > \"$dest/updated_date=2026-04-01/part.gz\"\n"
                "elif [ \"$2\" = \"cp\" ]; then\n"
                "  target=\"$4\"\n"
                "  mkdir -p \"$(dirname \"$target\")\"\n"
                "  printf 'sidecar\\n' > \"$target\"\n"
                "else\n"
                "  exit 2\n"
                "fi\n",
                encoding="utf-8",
            )
            mock_aws.chmod(0o755)

            self.run_raw_snapshot_script(
                "collect_openalex_snapshot.sh",
                str(vendor_root),
                "2026-03-freeze",
                "works-only",
                env={**dict(os.environ), "PATH": "%s:%s" % (mock_bin, os.environ.get("PATH", ""))},
            )

            target_dir = vendor_root / "openalex" / "2026-03"
            self.assertTrue(
                (target_dir / "data" / "works" / "updated_date=2026-03-31" / "part.gz").exists()
            )
            self.assertFalse(
                (target_dir / "data" / "works" / "updated_date=2026-04-01").exists()
            )
            source_versions = read_json(target_dir / "source_versions.json")
            self.assertEqual(source_versions["freeze_end"], "2026-03-31")
            self.assertEqual(source_versions["batches"][0]["pruned_future_partitions"], 1)
            manifest = (target_dir / "sha256_manifest.tsv").read_text(encoding="utf-8")
            self.assertNotIn("2026-04-01", manifest)

    def test_pubmed_collection_filters_updatefiles_to_freeze_end(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            vendor_root = root / "vendor_archive"
            mock_bin = root / "mock_bin"
            mock_bin.mkdir()
            baseline_listing = root / "baseline.html"
            update_listing = root / "updatefiles.html"
            baseline_listing.write_text(
                '<a href="pubmed26n0001.xml.gz">pubmed26n0001.xml.gz</a> 2025-12-15 10:00\n'
                '<a href="pubmed27n0001.xml.gz">pubmed27n0001.xml.gz</a> 2026-12-15 10:00\n'
                '<a href="README.txt">README.txt</a> 2025-12-15 10:00\n',
                encoding="utf-8",
            )
            update_listing.write_text(
                '<a href="pubmed26n1001.xml.gz">pubmed26n1001.xml.gz</a> 2026-03-31 10:00\n'
                '<a href="pubmed26n1002.xml.gz">pubmed26n1002.xml.gz</a> 2026-04-01 10:00\n'
                '<a href="pubmed27n1001.xml.gz">pubmed27n1001.xml.gz</a> 2026-03-31 10:00\n'
                '<a href="README.txt">README.txt</a> 2026-03-31 10:00\n',
                encoding="utf-8",
            )
            mock_curl = mock_bin / "curl"
            mock_curl.write_text(
                "#!/bin/bash\n"
                "set -euo pipefail\n"
                "url=\"${@: -1}\"\n"
                "case \"$url\" in\n"
                "  */baseline/) cat \"%s\" ;;\n"
                "  */updatefiles/) cat \"%s\" ;;\n"
                "  *) exit 2 ;;\n"
                "esac\n" % (baseline_listing, update_listing),
                encoding="utf-8",
            )
            mock_curl.chmod(0o755)
            mock_wget = mock_bin / "wget"
            mock_wget.write_text(
                "#!/bin/bash\n"
                "set -euo pipefail\n"
                "output=\"\"\n"
                "while [ \"$#\" -gt 0 ]; do\n"
                "  case \"$1\" in\n"
                "    -O|-qO)\n"
                "      output=\"$2\"\n"
                "      shift 2\n"
                "      ;;\n"
                "    *)\n"
                "      shift\n"
                "      ;;\n"
                "  esac\n"
                "done\n"
                "mkdir -p \"$(dirname \"$output\")\"\n"
                "printf '<PubmedArticleSet/>\\n' > \"$output\"\n",
                encoding="utf-8",
            )
            mock_wget.chmod(0o755)

            self.run_raw_snapshot_script(
                "collect_pubmed_baseline_updatefiles.sh",
                str(vendor_root),
                "2026-03-freeze",
                env={
                    **dict(os.environ),
                    "CURL_BIN": str(mock_curl),
                    "WGET_BIN": str(mock_wget),
                },
            )

            baseline_dir = vendor_root / "pubmed" / "baseline" / "2026"
            update_dir = vendor_root / "pubmed" / "updatefiles" / "2026-03"
            self.assertTrue((baseline_dir / "pubmed26n0001.xml.gz").exists())
            self.assertFalse((baseline_dir / "pubmed27n0001.xml.gz").exists())
            self.assertTrue((update_dir / "pubmed26n1001.xml.gz").exists())
            self.assertFalse((update_dir / "pubmed26n1002.xml.gz").exists())
            self.assertFalse((update_dir / "pubmed27n1001.xml.gz").exists())
            source_versions = read_json(update_dir / "source_versions.json")
            self.assertEqual(source_versions["freeze_end"], "2026-03-31")
            self.assertEqual(source_versions["batches"][1]["downloaded_xml_files"], 1)
            self.assertEqual(
                source_versions["batches"][1]["filtered_by_listing_mtime_on_or_before"],
                "2026-03-31",
            )
            manifest = (update_dir / "sha256_manifest.tsv").read_text(encoding="utf-8")
            self.assertIn("pubmed26n1001.xml.gz", manifest)
            self.assertNotIn("pubmed26n1002.xml.gz", manifest)

    def test_vendor_staging_does_not_fall_forward_to_future_retraction_watch_dump(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            vendor_root = root / "vendor_archive"
            raw_root = root / "raw_snapshot"

            openalex_dir = vendor_root / "openalex" / "2026-03" / "data" / "works"
            openalex_dir.mkdir(parents=True, exist_ok=True)
            write_jsonl(
                openalex_dir / "000_part_000.gz",
                [
                    {
                        "doi": "10.9500/vendor.future-rw",
                        "title": "Future Retraction Watch guard article",
                        "abstract": "Open data only article",
                        "publication_date": "2025-12-01",
                        "type": "article",
                        "concepts": [{"display_name": "Biology", "score": 0.95}],
                        "authorships": [],
                    }
                ],
            )
            openalex_batch = vendor_root / "openalex" / "2026-03"
            (openalex_batch / "source_versions.json").write_text("{}", encoding="utf-8")
            (openalex_batch / "sha256_manifest.tsv").write_text("x\n", encoding="utf-8")

            future_rw = vendor_root / "retraction_watch" / "2026-04-30"
            future_rw.mkdir(parents=True, exist_ok=True)
            write_csv(
                future_rw / "retraction_watch.csv",
                rows=[
                    {
                        "OriginalPaperDOI": "10.9500/vendor.future-rw",
                        "RetractionNature": "Retraction",
                        "RetractionDate": "2026-03-20",
                        "URLS": "https://retraction.example/future-rw",
                    }
                ],
                fieldnames=["OriginalPaperDOI", "RetractionNature", "RetractionDate", "URLS"],
            )
            (future_rw / "source_versions.json").write_text("{}", encoding="utf-8")
            (future_rw / "sha256_manifest.tsv").write_text("x\n", encoding="utf-8")

            pubmed_baseline_dir = vendor_root / "pubmed" / "baseline" / "2026"
            pubmed_baseline_dir.mkdir(parents=True, exist_ok=True)
            with gzip.open(pubmed_baseline_dir / "pubmed26n0001.xml.gz", "wt", encoding="utf-8") as handle:
                handle.write("<PubmedArticleSet/>")
            (pubmed_baseline_dir / "source_versions.json").write_text("{}", encoding="utf-8")
            (pubmed_baseline_dir / "sha256_manifest.tsv").write_text("x\n", encoding="utf-8")

            pubmed_update_dir = vendor_root / "pubmed" / "updatefiles" / "2026-03"
            pubmed_update_dir.mkdir(parents=True, exist_ok=True)
            with gzip.open(pubmed_update_dir / "pubmed26n1001.xml.gz", "wt", encoding="utf-8") as handle:
                handle.write("<PubmedArticleSet/>")
            (pubmed_update_dir / "source_versions.json").write_text("{}", encoding="utf-8")
            (pubmed_update_dir / "sha256_manifest.tsv").write_text("x\n", encoding="utf-8")

            vendor_validation = validate_vendor_archive(
                vendor_root,
                "2026-03-freeze",
                allow_missing_crossref=True,
            )
            self.assertFalse(vendor_validation["passed"])
            self.assertTrue(
                any(
                    check["label"] == "retraction_watch"
                    and check["reason"] == "no dated directory found at or before freeze_end"
                    for check in vendor_validation["checks"]
                )
            )

            with self.assertRaisesRegex(FileNotFoundError, "Retraction Watch"):
                stage_vendor_archive_to_raw_snapshot(
                    vendor_root=vendor_root,
                    raw_root=raw_root,
                    snapshot_label="2026-03-freeze",
                    mode="copy",
                    allow_missing_crossref=True,
                )
            self.assertFalse(
                (raw_root / "official_notices" / "retraction_watch_2026-04-30.csv.gz").exists()
            )

    def test_collection_runtime_check_script_supports_binary_overrides(self):
        python_bin = subprocess.run(
            [self.python_bin, "-c", "import sys; print(sys.executable)"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        result = subprocess.run(
            ["/bin/bash", str(self.raw_snapshot_script_dir / "check_collection_runtime.sh")],
            cwd=self.repo_root,
            capture_output=True,
            text=True,
            env={
                **dict(os.environ),
                "AWS_BIN": "/bin/echo",
                "CURL_BIN": "/bin/echo",
                "WGET_BIN": "/bin/echo",
                "GIT_BIN": "/bin/echo",
                "PYTHON_BIN": python_bin,
                "CROSSREF_PLUS_TOKEN": "dummy-token",
            },
            check=True,
        )
        self.assertIn("collection_runtime_ready=yes", result.stdout)
        self.assertIn("crossref_plus_token=present", result.stdout)

    def test_collection_runtime_check_supports_env_file(self):
        python_bin = subprocess.run(
            [self.python_bin, "-c", "import sys; print(sys.executable)"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        with tempfile.TemporaryDirectory() as tmp_dir:
            env_file = Path(tmp_dir) / "raw_snapshot.env"
            env_file.write_text(
                "\n".join(
                    [
                        'AWS_BIN="/bin/echo"',
                        'CURL_BIN="/bin/echo"',
                        'WGET_BIN="/bin/echo"',
                        'GIT_BIN="/bin/echo"',
                        f'PYTHON_BIN="{python_bin}"',
                        'CROSSREF_PLUS_TOKEN="dummy-token"',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            result = subprocess.run(
                ["/bin/bash", str(self.raw_snapshot_script_dir / "check_collection_runtime.sh")],
                cwd=self.repo_root,
                capture_output=True,
                text=True,
                env={
                    **dict(os.environ),
                    "RAW_SNAPSHOT_ENV_FILE": str(env_file),
                },
                check=True,
            )
        self.assertIn("collection_runtime_ready=yes", result.stdout)
        self.assertIn("crossref_plus_token=present", result.stdout)

    def test_collection_runtime_check_open_data_only_does_not_require_token(self):
        python_bin = subprocess.run(
            [self.python_bin, "-c", "import sys; print(sys.executable)"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        result = subprocess.run(
            ["/bin/bash", str(self.raw_snapshot_script_dir / "check_collection_runtime.sh")],
            cwd=self.repo_root,
            capture_output=True,
            text=True,
            env={
                **dict(os.environ),
                "AWS_BIN": "/bin/echo",
                "CURL_BIN": "/bin/echo",
                "WGET_BIN": "/bin/echo",
                "GIT_BIN": "/bin/echo",
                "PYTHON_BIN": python_bin,
                "CROSSREF_SOURCE_MODE": "skip",
            },
            check=True,
        )
        self.assertIn("collection_runtime_ready=yes", result.stdout)
        self.assertIn("crossref_source_mode=skip", result.stdout)
        self.assertIn("crossref_plus_token=not_required", result.stdout)


    def test_metadata_baseline_exposes_feature_importance(self):
        """Metadata logistic run must include a non-empty feature_importance list
        whose entries are dicts with ``feature`` (str) and ``weight`` (float) keys,
        sorted descending by absolute weight."""
        from life_science_integrity_benchmark.baselines import (
            run_task_a_baselines,
            split_records_for_manifest,
        )
        from life_science_integrity_benchmark.splits import build_split_manifests

        manifests = build_split_manifests(self.records)
        train, _, test = split_records_for_manifest(self.records, manifests["task_a_12m"])
        runs = run_task_a_baselines(train, test, horizon="12m")
        metadata_run = next(r for r in runs if r.model_name == "metadata_logistic_baseline")

        fi = metadata_run.metrics.get("feature_importance", [])
        self.assertIsInstance(fi, list)
        self.assertGreater(len(fi), 0, "feature_importance must not be empty")
        for entry in fi:
            self.assertIn("feature", entry)
            self.assertIn("weight", entry)
            self.assertIsInstance(entry["feature"], str)
            self.assertIsInstance(entry["weight"], float)
        # Sorted descending by abs weight
        weights = [abs(e["weight"]) for e in fi]
        self.assertEqual(weights, sorted(weights, reverse=True))

    def test_all_baselines_include_calibration_curve(self):
        """Every Task A baseline run must include a ``calibration_curve`` list of
        bin dicts.  Each non-empty bin must have the required keys and values in
        [0, 1]."""
        from life_science_integrity_benchmark.baselines import (
            run_task_a_baselines,
            split_records_for_manifest,
        )
        from life_science_integrity_benchmark.splits import build_split_manifests

        manifests = build_split_manifests(self.records)
        train, _, test = split_records_for_manifest(self.records, manifests["task_a_12m"])
        runs = run_task_a_baselines(train, test, horizon="12m")

        self.assertEqual(len(runs), 3)
        for run in runs:
            curve = run.metrics.get("calibration_curve")
            self.assertIsInstance(curve, list, "calibration_curve missing for %s" % run.model_name)
            for bin_entry in curve:
                for key in ("bin_lower", "bin_upper", "mean_predicted", "fraction_positive", "count"):
                    self.assertIn(key, bin_entry)
                self.assertGreaterEqual(bin_entry["fraction_positive"], 0.0)
                self.assertLessEqual(bin_entry["fraction_positive"], 1.0)
                self.assertGreaterEqual(bin_entry["mean_predicted"], 0.0)
                self.assertLessEqual(bin_entry["mean_predicted"], 1.0)

    def test_all_baselines_include_threshold_scan_and_precision_metrics(self):
        from life_science_integrity_benchmark.baselines import (
            run_task_a_baselines,
            split_records_for_manifest,
        )
        from life_science_integrity_benchmark.splits import build_split_manifests

        manifests = build_split_manifests(self.records)
        train, _, test = split_records_for_manifest(self.records, manifests["task_a_12m"])
        runs = run_task_a_baselines(train, test, horizon="12m")

        self.assertEqual(len(runs), 3)
        expected_thresholds = {
            "top_0.5pct",
            "top_1pct",
            "top_2pct",
            "top_5pct",
            "top_10pct",
        }
        for run in runs:
            metrics = run.metrics
            self.assertIn("Precision@1pct", metrics)
            self.assertIn("Precision@5pct", metrics)
            threshold_scan = metrics.get("precision_recall_at_thresholds")
            self.assertIsInstance(
                threshold_scan,
                dict,
                "precision_recall_at_thresholds missing for %s" % run.model_name,
            )
            self.assertEqual(set(threshold_scan.keys()), expected_thresholds)
            self.assertEqual(
                threshold_scan["top_1pct"]["precision"], metrics["Precision@1pct"]
            )
            self.assertEqual(
                threshold_scan["top_1pct"]["recall"], metrics["Recall@1pct"]
            )
            self.assertEqual(
                threshold_scan["top_5pct"]["precision"], metrics["Precision@5pct"]
            )
            self.assertEqual(
                threshold_scan["top_5pct"]["recall"], metrics["Recall@5pct"]
            )
            for point in threshold_scan.values():
                self.assertIn("k", point)
                self.assertIn("precision", point)
                self.assertIn("recall", point)
                self.assertGreaterEqual(point["k"], 1)
                self.assertGreaterEqual(point["precision"], 0.0)
                self.assertLessEqual(point["precision"], 1.0)
                self.assertGreaterEqual(point["recall"], 0.0)
                self.assertLessEqual(point["recall"], 1.0)

    def test_bootstrap_ci_is_reproducible_and_bounded(self):
        from life_science_integrity_benchmark.evaluation import (
            average_precision,
            bootstrap_ci,
        )

        labels = [1, 0, 1, 0, 1, 0]
        probs = [0.91, 0.83, 0.72, 0.41, 0.35, 0.08]

        first = bootstrap_ci(labels, probs, average_precision, n_bootstrap=200)
        second = bootstrap_ci(labels, probs, average_precision, n_bootstrap=200)

        self.assertEqual(first, second)
        lower, upper = first
        self.assertGreaterEqual(lower, 0.0)
        self.assertLessEqual(lower, upper)
        self.assertLessEqual(upper, 1.0)
        self.assertGreater(upper - lower, 0.0)

    def test_evaluation_metrics_reject_mismatched_lengths(self):
        from life_science_integrity_benchmark.evaluation import (
            accuracy,
            average_precision,
            bootstrap_ci,
            calibration_curve_data,
            expected_calibration_error,
            precision_at_k,
            recall_at_k,
        )

        with self.assertRaises(ValueError):
            average_precision([1, 0], [0.9])
        with self.assertRaises(ValueError):
            precision_at_k([1, 0], [0.9], 1)
        with self.assertRaises(ValueError):
            recall_at_k([1, 0], [0.9], 1)
        with self.assertRaises(ValueError):
            expected_calibration_error([1, 0], [0.9])
        with self.assertRaises(ValueError):
            calibration_curve_data([1, 0], [0.9])
        with self.assertRaises(ValueError):
            bootstrap_ci([1, 0], [0.9], average_precision)
        with self.assertRaises(ValueError):
            accuracy(["a"], [])
        with self.assertRaises(ValueError):
            expected_calibration_error([1], [0.9], bins=0)
        with self.assertRaises(ValueError):
            calibration_curve_data([1], [0.9], n_bins=0)

    def test_model_primitives_reject_misaligned_feature_shapes(self):
        from life_science_integrity_benchmark.models import (
            LogisticRegressionModel,
            concat_features,
        )

        model = LogisticRegressionModel()
        with self.assertRaisesRegex(ValueError, "features and labels"):
            model.fit([[1.0], [0.0]], [1])
        with self.assertRaisesRegex(ValueError, "same width"):
            model.fit([[1.0], [0.0, 1.0]], [1, 0])

        model.fit([[0.0], [1.0]], [0, 1])
        with self.assertRaisesRegex(ValueError, "feature width"):
            model.predict_proba([[0.0, 1.0]])
        with self.assertRaisesRegex(ValueError, "same number of rows"):
            concat_features([[1.0]], [[2.0], [3.0]])

    def test_all_baselines_include_auprc_confidence_intervals(self):
        from life_science_integrity_benchmark.baselines import (
            run_task_a_baselines,
            split_records_for_manifest,
        )
        from life_science_integrity_benchmark.splits import build_split_manifests

        manifests = build_split_manifests(self.records)
        train, _, test = split_records_for_manifest(self.records, manifests["task_a_12m"])
        runs = run_task_a_baselines(train, test, horizon="12m")

        self.assertEqual(len(runs), 3)
        for run in runs:
            metrics = run.metrics
            self.assertIn("AUPRC_ci_lower", metrics)
            self.assertIn("AUPRC_ci_upper", metrics)
            self.assertGreaterEqual(metrics["AUPRC_ci_lower"], 0.0)
            self.assertLessEqual(metrics["AUPRC_ci_lower"], metrics["AUPRC_ci_upper"])
            self.assertLessEqual(metrics["AUPRC_ci_upper"], 1.0)
            self.assertGreaterEqual(metrics["AUPRC"], metrics["AUPRC_ci_lower"])
            self.assertLessEqual(metrics["AUPRC"], metrics["AUPRC_ci_upper"])

    def test_calibration_svg_is_valid_xml(self):
        """``build_calibration_svg`` must return well-formed XML with the expected
        panel structure (2 horizons × 3 models)."""
        import xml.etree.ElementTree as ET
        from life_science_integrity_benchmark.reporting import build_calibration_svg

        baselines = {
            "task_a_12m": [
                {
                    "model_name": "metadata_logistic_baseline",
                    "metrics": {
                        "calibration_curve": [
                            {"bin_lower": 0.0, "bin_upper": 0.2, "mean_predicted": 0.1,
                             "fraction_positive": 0.05, "count": 4},
                            {"bin_lower": 0.4, "bin_upper": 0.6, "mean_predicted": 0.5,
                             "fraction_positive": 0.6, "count": 3},
                            {"bin_lower": 0.8, "bin_upper": 1.0, "mean_predicted": 0.9,
                             "fraction_positive": 0.9, "count": 2},
                        ]
                    },
                }
            ],
            "task_a_36m": [],
        }
        svg_text = build_calibration_svg(baselines)
        self.assertIn("<svg", svg_text)
        # Must parse as valid XML
        root = ET.fromstring(svg_text)
        self.assertEqual(root.tag, "{http://www.w3.org/2000/svg}svg")
        # At least one polyline rendered for the populated 12m panel
        ns = {"svg": "http://www.w3.org/2000/svg"}
        polylines = root.findall(".//svg:polyline", ns)
        self.assertGreater(len(polylines), 0)

    def test_train_task_a_writes_pr_curve_svg(self):
        import xml.etree.ElementTree as ET

        from life_science_integrity_benchmark.cli import _train_task_a

        manifests = build_split_manifests(self.records)
        with tempfile.TemporaryDirectory() as tmpdir:
            release_dir = Path(tmpdir)
            _train_task_a(self.records, manifests, release_dir, text_backend="hashing")
            pr_svg_path = release_dir / "task_a_pr_curves.svg"
            self.assertTrue(pr_svg_path.exists())
            svg_text = pr_svg_path.read_text(encoding="utf-8")
            self.assertIn("<svg", svg_text)
            root = ET.fromstring(svg_text)
            self.assertEqual(root.tag, "{http://www.w3.org/2000/svg}svg")


if __name__ == "__main__":
    unittest.main()
