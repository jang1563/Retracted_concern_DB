"""Metrics for ranking, calibration, and multilabel evaluation."""

import random
from typing import Callable, Dict, List, Sequence, Tuple


def average_precision(labels: Sequence[int], scores: Sequence[float]) -> float:
    _validate_aligned_lengths(labels, scores)
    paired = sorted(zip(scores, labels), key=lambda item: item[0], reverse=True)
    positives = sum(labels)
    if positives == 0:
        return 0.0
    running_hits = 0
    running_sum = 0.0
    for index, (_, label) in enumerate(paired, start=1):
        if label:
            running_hits += 1
            running_sum += running_hits / index
    return running_sum / positives


def recall_at_k(labels: Sequence[int], scores: Sequence[float], k: int) -> float:
    _validate_aligned_lengths(labels, scores)
    paired = sorted(zip(scores, labels), key=lambda item: item[0], reverse=True)[:k]
    positives = sum(labels)
    if positives == 0:
        return 0.0
    return sum(label for _, label in paired) / positives


def precision_at_k(labels: Sequence[int], scores: Sequence[float], k: int) -> float:
    _validate_aligned_lengths(labels, scores)
    if k <= 0:
        return 0.0
    paired = sorted(zip(scores, labels), key=lambda item: item[0], reverse=True)[:k]
    if not paired:
        return 0.0
    return sum(label for _, label in paired) / len(paired)


def bootstrap_ci(
    labels: Sequence[int],
    probs: Sequence[float],
    metric_fn: Callable[[Sequence[int], Sequence[float]], float],
    n_bootstrap: int = 1000,
    alpha: float = 0.05,
) -> Tuple[float, float]:
    """Estimate a percentile bootstrap confidence interval for a ranking metric.

    A fixed RNG seed keeps the benchmark outputs reproducible across runs and
    test environments.
    """
    if len(labels) != len(probs):
        raise ValueError("labels and probs must have the same length")
    if not labels or not probs:
        return 0.0, 0.0

    if n_bootstrap <= 0:
        point_estimate = metric_fn(labels, probs)
        return point_estimate, point_estimate

    rng = random.Random(0)
    sample_size = len(labels)
    draws = []
    for _ in range(n_bootstrap):
        indices = [rng.randrange(sample_size) for _ in range(sample_size)]
        sampled_labels = [labels[index] for index in indices]
        sampled_probs = [probs[index] for index in indices]
        draws.append(metric_fn(sampled_labels, sampled_probs))

    draws.sort()
    lower_index = max(0, min(len(draws) - 1, int((alpha / 2) * len(draws))))
    upper_index = max(0, min(len(draws) - 1, int((1 - (alpha / 2)) * len(draws)) - 1))
    return draws[lower_index], draws[upper_index]


def expected_calibration_error(labels: Sequence[int], probs: Sequence[float], bins: int = 10) -> float:
    _validate_aligned_lengths(labels, probs)
    if bins <= 0:
        raise ValueError("bins must be positive")
    bucket_totals = [0] * bins
    bucket_labels = [0.0] * bins
    bucket_probs = [0.0] * bins
    for label, prob in zip(labels, probs):
        index = min(bins - 1, int(prob * bins))
        bucket_totals[index] += 1
        bucket_labels[index] += label
        bucket_probs[index] += prob
    total = len(labels) or 1
    ece = 0.0
    for total_count, label_sum, prob_sum in zip(bucket_totals, bucket_labels, bucket_probs):
        if not total_count:
            continue
        accuracy = label_sum / total_count
        confidence = prob_sum / total_count
        ece += abs(accuracy - confidence) * (total_count / total)
    return ece


def accuracy(labels: Sequence[str], preds: Sequence[str]) -> float:
    _validate_aligned_lengths(labels, preds)
    if not labels:
        return 0.0
    matches = sum(1 for left, right in zip(labels, preds) if left == right)
    return matches / len(labels)


def macro_f1(label_rows: Sequence[Sequence[str]], pred_rows: Sequence[Sequence[str]], universe: Sequence[str]) -> float:
    _validate_aligned_lengths(label_rows, pred_rows)
    per_tag = []
    for tag in universe:
        tp = fp = fn = 0
        for labels, preds in zip(label_rows, pred_rows):
            labels_set = set(labels)
            preds_set = set(preds)
            if tag in labels_set and tag in preds_set:
                tp += 1
            elif tag not in labels_set and tag in preds_set:
                fp += 1
            elif tag in labels_set and tag not in preds_set:
                fn += 1
        precision = tp / (tp + fp) if (tp + fp) else 0.0
        recall = tp / (tp + fn) if (tp + fn) else 0.0
        if precision + recall == 0:
            per_tag.append(0.0)
        else:
            per_tag.append((2 * precision * recall) / (precision + recall))
    return sum(per_tag) / len(per_tag) if per_tag else 0.0


def provenance_coverage(records) -> float:
    if not records:
        return 0.0
    covered = sum(1 for record in records if record.source_names and record.source_urls)
    return covered / len(records)


def calibration_curve_data(
    labels: Sequence[int], probs: Sequence[float], n_bins: int = 5
) -> List[Dict[str, object]]:
    """Return per-bin statistics for a reliability diagram.

    Each bin covers an equal-width interval of predicted probability.  Empty
    bins are omitted so callers can detect degenerate cases (e.g. all
    predictions in one bin).
    """
    _validate_aligned_lengths(labels, probs)
    if n_bins <= 0:
        raise ValueError("n_bins must be positive")
    bins = []
    for i in range(n_bins):
        lo = i / n_bins
        hi = (i + 1) / n_bins
        in_bin = [
            (p, l)
            for p, l in zip(probs, labels)
            if lo <= p < hi or (i == n_bins - 1 and p == 1.0)
        ]
        if not in_bin:
            continue
        bin_probs = [p for p, _ in in_bin]
        bin_labels = [l for _, l in in_bin]
        bins.append(
            {
                "bin_lower": round(lo, 4),
                "bin_upper": round(hi, 4),
                "mean_predicted": round(sum(bin_probs) / len(bin_probs), 4),
                "fraction_positive": round(sum(bin_labels) / len(bin_labels), 4),
                "count": len(bin_probs),
            }
        )
    return bins


def grouped_slice_counts(records) -> Dict[str, Dict[str, int]]:
    output: Dict[str, Dict[str, int]] = {"subfield": {}, "publisher": {}}
    for record in records:
        output["subfield"][record.subfield] = output["subfield"].get(record.subfield, 0) + 1
        output["publisher"][record.publisher] = output["publisher"].get(record.publisher, 0) + 1
    return output


def _validate_aligned_lengths(left: Sequence, right: Sequence) -> None:
    if len(left) != len(right):
        raise ValueError("metric inputs must have the same length")
