"""Small offline-friendly modeling primitives and optional adapters."""

import math
from collections import Counter
from typing import List, Sequence

from .utils import dot, sigmoid, tokenize


class MetadataVectorizer:
    """A compact deterministic vectorizer for pre-publication metadata."""

    def __init__(self):
        self.category_to_index = {}
        self.numeric_fields = [
            "publication_year",
            "openalex_life_science_score",
            "references_count",
            "author_history_signal_count",
            "journal_history_signal_count",
            "is_pubmed_indexed",
        ]

    def fit(self, records) -> None:
        for record in records:
            for feature in self._categorical_features(record):
                if feature not in self.category_to_index:
                    self.category_to_index[feature] = len(self.category_to_index)

    def transform(self, records) -> List[List[float]]:
        vectors = []
        width = len(self.category_to_index) + len(self.numeric_fields)
        for record in records:
            vector = [0.0] * width
            for feature in self._categorical_features(record):
                if feature in self.category_to_index:
                    vector[self.category_to_index[feature]] = 1.0
            offset = len(self.category_to_index)
            vector[offset + 0] = (record.publication_year - 2000) / 24.0
            vector[offset + 1] = record.openalex_life_science_score
            vector[offset + 2] = record.references_count / 100.0
            vector[offset + 3] = record.author_history_signal_count / 10.0
            vector[offset + 4] = record.journal_history_signal_count / 10.0
            vector[offset + 5] = 1.0 if record.is_pubmed_indexed else 0.0
            vectors.append(vector)
        return vectors

    def fit_transform(self, records) -> List[List[float]]:
        self.fit(records)
        return self.transform(records)

    def feature_names(self) -> List[str]:
        """Return feature names in the same order as ``transform()`` output.

        Categorical features (venue, publisher, subfield, oa_status) come
        first in the order they were first seen during ``fit()``, followed by
        the fixed numeric features.
        """
        names: List[str] = [""] * (len(self.category_to_index) + len(self.numeric_fields))
        for name, idx in self.category_to_index.items():
            names[idx] = name
        offset = len(self.category_to_index)
        for i, field in enumerate(self.numeric_fields):
            names[offset + i] = field
        return names

    def _categorical_features(self, record) -> List[str]:
        return [
            "venue=" + record.venue,
            "publisher=" + record.publisher,
            "subfield=" + record.subfield,
            "oa_status=" + record.oa_status,
        ]


class TextVectorizer:
    """A bag-of-words encoder with capped vocabulary for title and abstract text."""

    def __init__(self, vocab_size: int = 128):
        self.vocab_size = vocab_size
        self.vocabulary = {}
        self.idf = {}

    def fit(self, texts: Sequence[str]) -> None:
        document_frequency = Counter()
        for text in texts:
            document_frequency.update(set(tokenize(text)))
        most_common = document_frequency.most_common(self.vocab_size)
        self.vocabulary = {token: index for index, (token, _) in enumerate(most_common)}
        total_docs = max(1, len(texts))
        self.idf = {
            token: math.log(1 + total_docs / (1 + document_frequency[token])) + 1.0
            for token in self.vocabulary
        }

    def transform(self, texts: Sequence[str]) -> List[List[float]]:
        output = []
        for text in texts:
            vector = [0.0] * len(self.vocabulary)
            counts = Counter(tokenize(text))
            token_total = max(1, sum(counts.values()))
            for token, count in counts.items():
                if token not in self.vocabulary:
                    continue
                index = self.vocabulary[token]
                tf = count / token_total
                vector[index] = tf * self.idf[token]
            output.append(vector)
        return output

    def fit_transform(self, texts: Sequence[str]) -> List[List[float]]:
        self.fit(texts)
        return self.transform(texts)


class LogisticRegressionModel:
    """Simple dense logistic regression trained with batch gradient descent."""

    def __init__(self, learning_rate: float = 0.25, epochs: int = 400, l2: float = 0.001):
        self.learning_rate = learning_rate
        self.epochs = epochs
        self.l2 = l2
        self.weights: List[float] = []
        self.bias = 0.0

    def fit(self, features: List[List[float]], labels: Sequence[int]) -> None:
        if not features:
            self.weights = []
            self.bias = 0.0
            return
        width = len(features[0])
        self.weights = [0.0] * width
        self.bias = 0.0
        for _ in range(self.epochs):
            grad_w = [0.0] * width
            grad_b = 0.0
            for vector, label in zip(features, labels):
                prob = sigmoid(dot(self.weights, vector) + self.bias)
                error = prob - label
                for index, value in enumerate(vector):
                    grad_w[index] += error * value
                grad_b += error
            scale = 1.0 / max(1, len(features))
            for index in range(width):
                grad_w[index] = grad_w[index] * scale + self.l2 * self.weights[index]
                self.weights[index] -= self.learning_rate * grad_w[index]
            self.bias -= self.learning_rate * grad_b * scale

    def predict_proba(self, features: List[List[float]]) -> List[float]:
        return [sigmoid(dot(self.weights, vector) + self.bias) for vector in features]


class OptionalTransformerEncoder:
    """Optional local-only transformer encoder hook for abstracts."""

    def __init__(self, model_name: str, local_files_only: bool = True):
        self.model_name = model_name
        self.local_files_only = local_files_only

    def encode(self, texts: Sequence[str]) -> List[List[float]]:
        try:
            import torch
            from transformers import AutoModel, AutoTokenizer
        except Exception as exc:
            raise RuntimeError(
                "Transformer backend requested but transformers/torch are unavailable."
            ) from exc

        tokenizer = AutoTokenizer.from_pretrained(
            self.model_name, local_files_only=self.local_files_only
        )
        model = AutoModel.from_pretrained(
            self.model_name, local_files_only=self.local_files_only
        )
        model.eval()
        embeddings = []
        for text in texts:
            encoded = tokenizer(
                text,
                return_tensors="pt",
                truncation=True,
                max_length=256,
                padding=True,
            )
            with torch.no_grad():
                outputs = model(**encoded)
                pooled = outputs.last_hidden_state.mean(dim=1).squeeze(0)
                embeddings.append(pooled.tolist())
        return embeddings


def concat_features(left: List[List[float]], right: List[List[float]]) -> List[List[float]]:
    return [a + b for a, b in zip(left, right)]
