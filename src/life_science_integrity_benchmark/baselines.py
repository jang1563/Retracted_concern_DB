"""Baseline runners for Task A ranking and Task B evidence aggregation."""

from dataclasses import dataclass
from typing import Dict, List, Sequence, Tuple

from .evaluation import (
    accuracy,
    average_precision,
    bootstrap_ci,
    calibration_curve_data,
    expected_calibration_error,
    macro_f1,
    precision_at_k,
    provenance_coverage,
    recall_at_k,
)
from .models import (
    LogisticRegressionModel,
    MetadataVectorizer,
    OptionalTransformerEncoder,
    TextVectorizer,
    concat_features,
)
from .types import BaselineRun, BenchmarkRecord, SourceProvenance


TASK_B_TAG_UNIVERSE = [
    "retraction",
    "expression_of_concern",
    "major_correction",
    "community_flag",
    "image_issue",
    "paper_mill_signal",
    "tortured_phrase",
    "metadata_anomaly",
]


@dataclass
class TaskAView:
    doi: str
    title: str
    abstract: str
    venue: str
    publisher: str
    publication_year: int
    subfield: str
    is_pubmed_indexed: bool
    openalex_life_science_score: float
    references_count: int
    author_history_signal_count: int
    journal_history_signal_count: int
    oa_status: str
    authors: List[str]
    institutions: List[str]


def run_task_a_baselines(
    train_records: List[BenchmarkRecord],
    test_records: List[BenchmarkRecord],
    horizon: str = "12m",
    text_backend: str = "hashing",
    transformer_model_name: str = "allenai/scibert_scivocab_uncased",
) -> List[BaselineRun]:
    label_attr = (
        "any_signal_or_notice_within_12m"
        if horizon == "12m"
        else "any_signal_or_notice_within_36m"
    )
    y_train = [1 if getattr(record, label_attr) else 0 for record in train_records]
    y_test = [1 if getattr(record, label_attr) else 0 for record in test_records]

    if not train_records or not test_records:
        return []

    train_views = [_materialize_task_a_view(record) for record in train_records]
    test_views = [_materialize_task_a_view(record) for record in test_records]

    metadata_vectorizer = MetadataVectorizer()
    metadata_train = metadata_vectorizer.fit_transform(train_views)
    metadata_test = metadata_vectorizer.transform(test_views)

    metadata_model = LogisticRegressionModel()
    metadata_model.fit(metadata_train, y_train)
    metadata_probs = metadata_model.predict_proba(metadata_test)

    feature_importance: list = []
    feat_names = metadata_vectorizer.feature_names()
    if metadata_model.weights and feat_names:
        paired = sorted(
            zip(feat_names, metadata_model.weights),
            key=lambda kv: abs(kv[1]),
            reverse=True,
        )
        feature_importance = [
            {"feature": n, "weight": round(w, 6)} for n, w in paired[:20]
        ]

    text_train_input = [_text_input(view) for view in train_views]
    text_test_input = [_text_input(view) for view in test_views]
    text_backend_used = text_backend

    if text_backend == "transformers":
        try:
            encoder = OptionalTransformerEncoder(transformer_model_name)
            text_train = encoder.encode(text_train_input)
            text_test = encoder.encode(text_test_input)
        except RuntimeError:
            text_backend_used = "hashing_fallback"
            vectorizer = TextVectorizer()
            text_train = vectorizer.fit_transform(text_train_input)
            text_test = vectorizer.transform(text_test_input)
    else:
        vectorizer = TextVectorizer()
        text_train = vectorizer.fit_transform(text_train_input)
        text_test = vectorizer.transform(text_test_input)

    text_model = LogisticRegressionModel()
    text_model.fit(text_train, y_train)
    text_probs = text_model.predict_proba(text_test)

    fusion_train = concat_features(metadata_train, text_train)
    fusion_test = concat_features(metadata_test, text_test)
    fusion_model = LogisticRegressionModel()
    fusion_model.fit(fusion_train, y_train)
    fusion_probs = fusion_model.predict_proba(fusion_test)

    metadata_metrics = _ranking_metrics(y_test, metadata_probs, test_records)
    metadata_metrics["feature_importance"] = feature_importance

    return [
        BaselineRun(
            model_name="metadata_logistic_baseline",
            task_name="task_a_" + horizon,
            backend_used="logistic",
            metrics=metadata_metrics,
        ),
        BaselineRun(
            model_name="abstract_encoder_baseline",
            task_name="task_a_" + horizon,
            backend_used=text_backend_used,
            metrics=_ranking_metrics(y_test, text_probs, test_records),
        ),
        BaselineRun(
            model_name="metadata_text_fusion_baseline",
            task_name="task_a_" + horizon,
            backend_used="logistic+fusion",
            metrics=_ranking_metrics(y_test, fusion_probs, test_records),
        ),
    ]


def run_task_a_robustness(
    records: List[BenchmarkRecord],
    manifests,
    text_backend: str = "hashing",
    transformer_model_name: str = "allenai/scibert_scivocab_uncased",
) -> Dict[str, List[BaselineRun]]:
    """Run Task A baselines across the primary time split AND every grouped
    holdout manifest that split construction deemed usable, for both horizons.

    Returns a mapping from manifest name (e.g. ``task_a_12m`` or
    ``task_a_36m_venue_holdout``) to the list of baseline runs produced on
    that split. This is how robustness to distributional shift across valid
    authorship-cluster, venue, and publisher holdouts gets measured
    empirically rather than just claimed in the README.
    """
    runs_by_split: Dict[str, List[BaselineRun]] = {}
    for manifest_name in sorted(manifests):
        if not manifest_name.startswith("task_a_"):
            continue
        if manifest_name.endswith("_noisy_date"):
            continue
        manifest = manifests[manifest_name]
        train_records, _, test_records = split_records_for_manifest(records, manifest)
        if not train_records or not test_records:
            runs_by_split[manifest_name] = []
            continue
        horizon = "12m" if "12m" in manifest_name else "36m"
        runs = run_task_a_baselines(
            train_records=train_records,
            test_records=test_records,
            horizon=horizon,
            text_backend=text_backend,
            transformer_model_name=transformer_model_name,
        )
        for run in runs:
            run.task_name = manifest_name
        runs_by_split[manifest_name] = runs
    return runs_by_split


def run_task_b_baseline(records: List[BenchmarkRecord]) -> BaselineRun:
    gold_notice = [record.notice_status for record in records]
    gold_tags = [sorted(set(record.core_tags + record.extension_tags)) for record in records]

    pred_notice = []
    pred_tags = []
    for record in records:
        evidence_text = " ".join(
            "%s %s %s"
            % (entry.source_name, entry.event_kind, entry.summary)
            for entry in record.provenance
        ).lower()
        predicted_tags = _predict_task_b_tags(record.provenance, evidence_text)
        predicted_notice = _predict_notice_status(predicted_tags)
        pred_notice.append(predicted_notice)
        pred_tags.append(predicted_tags)

    return BaselineRun(
        model_name="task_b_evidence_keyword_baseline",
        task_name="task_b",
        backend_used="keyword_rules_over_provenance",
        metrics={
            "notice_status_accuracy": round(accuracy(gold_notice, pred_notice), 4),
            "tag_macro_f1": round(macro_f1(gold_tags, pred_tags, TASK_B_TAG_UNIVERSE), 4),
            "provenance_coverage": round(provenance_coverage(records), 4),
        },
    )


def split_records_for_manifest(
    records: List[BenchmarkRecord], manifest
) -> Tuple[List[BenchmarkRecord], List[BenchmarkRecord], List[BenchmarkRecord]]:
    by_doi = {record.doi: record for record in records}
    return (
        [by_doi[doi] for doi in manifest.train_dois if doi in by_doi],
        [by_doi[doi] for doi in manifest.val_dois if doi in by_doi],
        [by_doi[doi] for doi in manifest.test_dois if doi in by_doi],
    )


def _materialize_task_a_view(record: BenchmarkRecord) -> TaskAView:
    return TaskAView(
        doi=record.doi,
        title=record.title,
        abstract=record.abstract,
        venue=record.venue,
        publisher=record.publisher,
        publication_year=record.publication_year,
        subfield=record.subfield,
        is_pubmed_indexed=record.is_pubmed_indexed,
        openalex_life_science_score=record.openalex_life_science_score,
        references_count=record.references_count,
        author_history_signal_count=record.author_history_signal_count,
        journal_history_signal_count=record.journal_history_signal_count,
        oa_status=record.oa_status,
        authors=list(record.authors),
        institutions=list(record.institutions),
    )


def _text_input(record) -> str:
    return "%s %s" % (record.title, record.abstract)


def _predict_task_b_tags(
    provenance: List[SourceProvenance], evidence_text: str
) -> List[str]:
    tags = set()
    for entry in provenance:
        normalized = "%s %s %s" % (
            entry.source_name.lower(),
            entry.observed_label.lower(),
            entry.summary.lower(),
        )
        if "retraction" in normalized:
            tags.add("retraction")
        if "expression_of_concern" in normalized or "expression of concern" in normalized:
            tags.add("expression_of_concern")
        if "major_correction" in normalized or "major correction" in normalized:
            tags.add("major_correction")
        if "community" in normalized:
            tags.add("community_flag")
        if "image" in normalized or "figure" in normalized or "duplication" in normalized:
            tags.add("image_issue")
        if "paper mill" in normalized or "template family" in normalized:
            tags.add("paper_mill_signal")
        if "tortured phrase" in normalized:
            tags.add("tortured_phrase")
        if "metadata" in normalized or "authorship" in normalized or "revision timing" in normalized:
            tags.add("metadata_anomaly")
    return sorted(tags)


def _predict_notice_status(tags: List[str]) -> str:
    if "retraction" in tags:
        return "retracted"
    if any(tag in tags for tag in ("expression_of_concern", "major_correction")):
        return "editorial_notice"
    return "none_known_at_snapshot"


def _ranking_metrics(
    labels: Sequence[int], probs: Sequence[float], records: List[BenchmarkRecord]
) -> Dict[str, object]:
    record_count = max(1, len(records))
    top_1pct = max(1, int(round(record_count * 0.01)))
    top_5pct = max(1, int(round(record_count * 0.05)))
    auprc = average_precision(labels, probs)
    auprc_ci_lower, auprc_ci_upper = bootstrap_ci(labels, probs, average_precision)
    threshold_scan = {}
    for threshold_pct in (0.5, 1, 2, 5, 10):
        threshold_key = "top_%spct" % ("%g" % threshold_pct)
        top_k = max(1, int(round(record_count * (threshold_pct / 100.0))))
        threshold_scan[threshold_key] = {
            "k": min(top_k, len(probs)),
            "precision": round(precision_at_k(labels, probs, top_k), 4),
            "recall": round(recall_at_k(labels, probs, top_k), 4),
        }
    metrics = {
        "AUPRC": round(auprc, 4),
        "AUPRC_ci_lower": round(auprc_ci_lower, 4),
        "AUPRC_ci_upper": round(auprc_ci_upper, 4),
        "Precision@1pct": round(precision_at_k(labels, probs, top_1pct), 4),
        "Precision@5pct": round(precision_at_k(labels, probs, top_5pct), 4),
        "Recall@1pct": round(recall_at_k(labels, probs, top_1pct), 4),
        "Recall@5pct": round(recall_at_k(labels, probs, top_5pct), 4),
        "ECE": round(expected_calibration_error(labels, probs), 4),
        "calibration_curve": calibration_curve_data(labels, probs),
        "precision_recall_at_thresholds": threshold_scan,
    }
    subfield_breakdown = {}
    for subfield in sorted({record.subfield for record in records}):
        indices = [index for index, record in enumerate(records) if record.subfield == subfield]
        if not indices:
            continue
        sub_labels = [labels[index] for index in indices]
        sub_probs = [probs[index] for index in indices]
        subfield_breakdown[subfield] = round(average_precision(sub_labels, sub_probs), 4)
    metrics["AUPRC_by_subfield"] = subfield_breakdown
    return metrics
