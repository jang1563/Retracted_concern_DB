import csv
import gzip
import os
import subprocess
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from life_science_integrity_benchmark.audit import build_leakage_report
from life_science_integrity_benchmark.dataset import (
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
    ingest_snapshot,
    normalize_real_source_exports,
    register_snapshot,
    scaffold_real_source_layout,
)
from life_science_integrity_benchmark.manifest import ManifestStore, SnapshotModifiedError
from life_science_integrity_benchmark.materialize import materialize_canonical_snapshot
from life_science_integrity_benchmark.reporting import build_experiment_report
from life_science_integrity_benchmark.sample_data import SAMPLE_ARTICLES, SAMPLE_NOTICES, SAMPLE_SIGNALS
from life_science_integrity_benchmark.site import build_site, export_internal_curation_queue
from life_science_integrity_benchmark.splits import build_split_manifests
from life_science_integrity_benchmark.utils import read_json, read_jsonl, write_csv, write_jsonl
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
        holdout = manifests["task_a_12m_author_cluster_holdout"]
        by_doi = {record.doi: record for record in self.records + [noisy_record]}
        held_out_clusters = {by_doi[doi].author_cluster for doi in holdout.test_dois}
        overlap = {
            by_doi[doi].author_cluster
            for doi in holdout.train_dois + holdout.val_dois
            if by_doi[doi].author_cluster in held_out_clusters
        }
        self.assertEqual(overlap, set())
        self.assertIn("task_a_12m_noisy_date", manifests)
        self.assertIn("10.5555/lsib.2021.noisy", manifests["task_a_12m_noisy_date"].train_dois + manifests["task_a_12m_noisy_date"].val_dois + manifests["task_a_12m_noisy_date"].test_dois)

    def test_task_a_robustness_runs_on_every_grouped_holdout(self):
        from life_science_integrity_benchmark.baselines import run_task_a_robustness

        manifests = build_split_manifests(self.records)
        result = run_task_a_robustness(self.records, manifests, text_backend="hashing")

        # Primary time splits and the three grouped holdouts (per horizon)
        # must be present, and noisy-date splits must NOT be in the
        # robustness pass (they have their own analysis split).
        expected_present = {
            "task_a_12m",
            "task_a_12m_author_cluster_holdout",
            "task_a_12m_venue_holdout",
            "task_a_12m_publisher_holdout",
            "task_a_36m",
            "task_a_36m_author_cluster_holdout",
            "task_a_36m_venue_holdout",
            "task_a_36m_publisher_holdout",
        }
        self.assertTrue(expected_present.issubset(result.keys()))
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

    def test_collection_runtime_check_script_supports_binary_overrides(self):
        python_bin = subprocess.run(
            ["/bin/sh", "-lc", "command -v python3"],
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
            ["/bin/sh", "-lc", "command -v python3"],
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
            ["/bin/sh", "-lc", "command -v python3"],
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


if __name__ == "__main__":
    unittest.main()
