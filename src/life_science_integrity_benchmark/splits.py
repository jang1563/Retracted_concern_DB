"""Time-aware split generation with explicit grouped holdout manifests."""

from typing import Dict, List, Optional

from .constants import GROUP_HOLDOUT_FIELDS
from .types import BenchmarkRecord, SplitManifest


def build_split_manifests(records: List[BenchmarkRecord]) -> Dict[str, SplitManifest]:
    manifests: Dict[str, SplitManifest] = {}
    task_to_records = {
        "task_a_12m": [record for record in records if record.eligible_for_task_a_12m],
        "task_a_36m": [record for record in records if record.eligible_for_task_a_36m],
        "task_b": records,
    }
    noisy_task_to_records = {
        "task_a_12m_noisy_date": [
            record
            for record in records
            if record.task_a_date_bucket == "noisy_date" and record.publication_year <= 2024
        ],
        "task_a_36m_noisy_date": [
            record
            for record in records
            if record.task_a_date_bucket == "noisy_date" and record.publication_year <= 2022
        ],
    }

    for task_name, task_records in task_to_records.items():
        manifests[task_name] = _build_time_manifest(task_name, task_records)
        for group_field in GROUP_HOLDOUT_FIELDS:
            manifest = _build_group_holdout_manifest(task_name, task_records, group_field)
            if manifest is not None:
                manifests[manifest.task_name] = manifest
    for task_name, task_records in noisy_task_to_records.items():
        manifests[task_name] = _build_time_manifest(task_name, task_records)
    return manifests


def _build_time_manifest(task_name: str, records: List[BenchmarkRecord]) -> SplitManifest:
    years = sorted({record.publication_year for record in records})
    if not years:
        return SplitManifest(
            task_name=task_name,
            split_kind="time",
            train_dois=[],
            val_dois=[],
            test_dois=[],
        )
    train_years, val_years, test_years = _partition_years(years)
    return SplitManifest(
        task_name=task_name,
        split_kind="time",
        train_dois=[record.doi for record in records if record.publication_year in train_years],
        val_dois=[record.doi for record in records if record.publication_year in val_years],
        test_dois=[record.doi for record in records if record.publication_year in test_years],
    )


def _build_group_holdout_manifest(
    base_task_name: str, records: List[BenchmarkRecord], group_field: str
) -> Optional[SplitManifest]:
    holdout_value = _select_holdout_value(records, group_field, base_task_name)
    if holdout_value is None:
        return None

    holdout_records = [record for record in records if getattr(record, group_field) == holdout_value]
    remaining = [record for record in records if getattr(record, group_field) != holdout_value]
    if len(holdout_records) < 1 or len(remaining) < 3:
        return None

    years = sorted({record.publication_year for record in remaining})
    train_years, val_years, _ = _partition_years(years)
    train_dois = [record.doi for record in remaining if record.publication_year in train_years]
    val_dois = [record.doi for record in remaining if record.publication_year in val_years]
    if not train_dois or not val_dois:
        return None

    return SplitManifest(
        task_name="%s_%s_holdout" % (base_task_name, group_field),
        split_kind="group_holdout",
        train_dois=train_dois,
        val_dois=val_dois,
        test_dois=[record.doi for record in holdout_records],
        group_field=group_field,
        holdout_values=[holdout_value],
    )


def _select_holdout_value(
    records: List[BenchmarkRecord], group_field: str, task_name: str
) -> Optional[str]:
    for record in sorted(records, key=lambda item: item.publication_date, reverse=True):
        value = getattr(record, group_field)
        holdout_records = [
            candidate for candidate in records if getattr(candidate, group_field) == value
        ]
        holdout_count = len(holdout_records)
        remaining_count = len(records) - holdout_count
        if holdout_count < 2 or remaining_count < 3:
            continue
        if task_name.startswith("task_a_") and not _has_task_a_label_diversity(
            holdout_records, task_name
        ):
            continue
        if holdout_count >= 1 and remaining_count >= 3:
            return value
    return None


def _has_task_a_label_diversity(records: List[BenchmarkRecord], task_name: str) -> bool:
    label_attr = (
        "any_signal_or_notice_within_12m"
        if "12m" in task_name
        else "any_signal_or_notice_within_36m"
    )
    labels = {bool(getattr(record, label_attr)) for record in records}
    return len(labels) > 1


def _partition_years(years: List[int]):
    if len(years) == 0:
        return [], [], []
    if len(years) == 1:
        return years, [], []
    if len(years) == 2:
        return [years[0]], [], [years[1]]
    train_cut = max(1, int(len(years) * 0.6))
    val_cut = max(train_cut + 1, int(len(years) * 0.8))
    if val_cut >= len(years):
        val_cut = len(years) - 1
    train_years = years[:train_cut]
    val_years = years[train_cut:val_cut]
    test_years = years[val_cut:]
    if not val_years:
        val_years = [train_years[-1]]
    if not test_years:
        test_years = [years[-1]]
    return train_years, val_years, test_years
