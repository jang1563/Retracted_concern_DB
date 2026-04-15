"""SQLite-backed ingest manifest for frozen local snapshots."""

import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from .constants import (
    MANIFEST_DB_PATH,
    NOTICE_COLLECTOR,
    OPENALEX_COLLECTOR,
    PARSER_BUNDLE_VERSION,
    PUBMED_COLLECTOR,
)
from .utils import discover_files, hash_file_sha256


DEFAULT_COLLECTOR_LAYOUT = {
    OPENALEX_COLLECTOR: ("openalex", (".jsonl", ".jsonl.gz", ".gz")),
    NOTICE_COLLECTOR: (
        "official_notices",
        (".jsonl", ".jsonl.gz", ".csv", ".csv.gz"),
    ),
    PUBMED_COLLECTOR: (
        "pubmed",
        (".jsonl", ".jsonl.gz", ".csv", ".csv.gz", ".xml", ".xml.gz"),
    ),
}


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class SnapshotModifiedError(RuntimeError):
    """Raised when a registered snapshot no longer matches its original file hashes."""


class ManifestStore:
    """Thin wrapper around a single SQLite manifest DB."""

    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = Path(db_path or MANIFEST_DB_PATH)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    def _connect(self):
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    @contextmanager
    def _transact(self):
        """Context manager that opens a connection, yields it, commits on
        success (or rolls back on exception), and always closes the connection.

        Use this everywhere instead of bare ``with self._connect() as conn:``
        so that SQLite file descriptors are released promptly and Python's
        ResourceWarning is suppressed in tests.
        """
        connection = self._connect()
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _ensure_schema(self) -> None:
        with self._transact() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS snapshots (
                    snapshot_id TEXT PRIMARY KEY,
                    source_family TEXT NOT NULL,
                    snapshot_label TEXT NOT NULL,
                    raw_root TEXT NOT NULL,
                    snapshot_date TEXT NOT NULL,
                    parser_bundle_version TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS files (
                    file_id TEXT PRIMARY KEY,
                    snapshot_id TEXT NOT NULL,
                    collector_name TEXT NOT NULL,
                    relative_path TEXT NOT NULL,
                    content_sha256 TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    discovered_at TEXT NOT NULL,
                    parse_status TEXT NOT NULL,
                    parsed_rows INTEGER NOT NULL,
                    quarantined_rows INTEGER NOT NULL,
                    error_count INTEGER NOT NULL,
                    UNIQUE(snapshot_id, collector_name, relative_path)
                );

                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    snapshot_id TEXT NOT NULL,
                    stage_name TEXT NOT NULL,
                    collector_name TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    status TEXT NOT NULL,
                    parser_version TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS artifacts (
                    artifact_id TEXT PRIMARY KEY,
                    snapshot_id TEXT NOT NULL,
                    artifact_kind TEXT NOT NULL,
                    relative_path TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(snapshot_id, artifact_kind, relative_path)
                );

                CREATE TABLE IF NOT EXISTS row_errors (
                    error_id TEXT PRIMARY KEY,
                    file_id TEXT NOT NULL,
                    line_number INTEGER NOT NULL,
                    error_code TEXT NOT NULL,
                    error_message TEXT NOT NULL,
                    raw_excerpt TEXT NOT NULL
                );
                """
            )

    def register_snapshot(
        self,
        snapshot_id: str,
        raw_root: Path,
        source_family: str,
        snapshot_date: str,
        snapshot_label: Optional[str] = None,
        parser_bundle_version: str = PARSER_BUNDLE_VERSION,
        collector_layout: Optional[Dict[str, tuple]] = None,
    ) -> Dict[str, object]:
        raw_root = Path(raw_root).resolve()
        collector_layout = collector_layout or DEFAULT_COLLECTOR_LAYOUT
        discovered_files = []
        for collector_name, (relative_root, suffixes) in collector_layout.items():
            collector_root = raw_root / relative_root
            if not collector_root.exists():
                continue
            for path in discover_files(collector_root, suffixes):
                relative_path = str(path.relative_to(raw_root))
                discovered_files.append(
                    {
                        "collector_name": collector_name,
                        "absolute_path": path,
                        "relative_path": relative_path,
                        "content_sha256": hash_file_sha256(path),
                        "size_bytes": path.stat().st_size,
                    }
                )
        discovered_files = sorted(
            discovered_files,
            key=lambda row: (row["collector_name"], row["relative_path"]),
        )

        now = utcnow_iso()
        with self._transact() as connection:
            existing = connection.execute(
                "SELECT * FROM snapshots WHERE snapshot_id = ?", (snapshot_id,)
            ).fetchone()
            if existing:
                self._assert_snapshot_identity(
                    connection,
                    snapshot_id=snapshot_id,
                    raw_root=raw_root,
                    source_family=source_family,
                    snapshot_date=snapshot_date,
                    parser_bundle_version=parser_bundle_version,
                    discovered_files=discovered_files,
                )
                return {
                    "snapshot_id": snapshot_id,
                    "raw_root": raw_root,
                    "registered_files": len(discovered_files),
                    "status": existing["status"],
                }

            connection.execute(
                """
                INSERT INTO snapshots (
                    snapshot_id, source_family, snapshot_label, raw_root, snapshot_date,
                    parser_bundle_version, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot_id,
                    source_family,
                    snapshot_label or snapshot_id,
                    str(raw_root),
                    snapshot_date,
                    parser_bundle_version,
                    "registered",
                    now,
                ),
            )
            for file_row in discovered_files:
                connection.execute(
                    """
                    INSERT INTO files (
                        file_id, snapshot_id, collector_name, relative_path, content_sha256,
                        size_bytes, discovered_at, parse_status, parsed_rows, quarantined_rows,
                        error_count
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        _stable_file_id(
                            snapshot_id,
                            file_row["collector_name"],
                            file_row["relative_path"],
                        ),
                        snapshot_id,
                        file_row["collector_name"],
                        file_row["relative_path"],
                        file_row["content_sha256"],
                        file_row["size_bytes"],
                        now,
                        "registered",
                        0,
                        0,
                        0,
                    ),
                )
        return {
            "snapshot_id": snapshot_id,
            "raw_root": raw_root,
            "registered_files": len(discovered_files),
            "status": "registered",
        }

    def get_snapshot(self, snapshot_id: str) -> sqlite3.Row:
        with self._transact() as connection:
            row = connection.execute(
                "SELECT * FROM snapshots WHERE snapshot_id = ?", (snapshot_id,)
            ).fetchone()
        if row is None:
            raise KeyError("Unknown snapshot_id: %s" % snapshot_id)
        return row

    def list_files(self, snapshot_id: str, collector_name: Optional[str] = None) -> List[sqlite3.Row]:
        query = "SELECT * FROM files WHERE snapshot_id = ?"
        params: List[object] = [snapshot_id]
        if collector_name is not None:
            query += " AND collector_name = ?"
            params.append(collector_name)
        query += " ORDER BY collector_name, relative_path"
        with self._transact() as connection:
            return connection.execute(query, tuple(params)).fetchall()

    def assert_snapshot_frozen(self, snapshot_id: str) -> None:
        snapshot = self.get_snapshot(snapshot_id)
        raw_root = Path(snapshot["raw_root"])
        with self._transact() as connection:
            rows = connection.execute(
                "SELECT relative_path, content_sha256 FROM files WHERE snapshot_id = ?",
                (snapshot_id,),
            ).fetchall()
        for row in rows:
            path = raw_root / row["relative_path"]
            if not path.exists():
                raise SnapshotModifiedError(
                    "snapshot modified: missing file %s" % row["relative_path"]
                )
            current_sha = hash_file_sha256(path)
            if current_sha != row["content_sha256"]:
                raise SnapshotModifiedError(
                    "snapshot modified: checksum mismatch for %s" % row["relative_path"]
                )

    def start_run(self, snapshot_id: str, stage_name: str, collector_name: str) -> str:
        run_id = uuid.uuid4().hex
        with self._transact() as connection:
            connection.execute(
                """
                INSERT INTO runs (
                    run_id, snapshot_id, stage_name, collector_name, started_at,
                    finished_at, status, parser_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    snapshot_id,
                    stage_name,
                    collector_name,
                    utcnow_iso(),
                    None,
                    "running",
                    PARSER_BUNDLE_VERSION,
                ),
            )
        return run_id

    def finish_run(self, run_id: str, status: str) -> None:
        with self._transact() as connection:
            connection.execute(
                """
                UPDATE runs
                SET finished_at = ?, status = ?
                WHERE run_id = ?
                """,
                (utcnow_iso(), status, run_id),
            )

    def update_file_parse_result(
        self,
        file_id: str,
        parse_status: str,
        parsed_rows: int,
        quarantined_rows: int,
        error_count: int,
    ) -> None:
        with self._transact() as connection:
            connection.execute(
                """
                UPDATE files
                SET parse_status = ?, parsed_rows = ?, quarantined_rows = ?, error_count = ?
                WHERE file_id = ?
                """,
                (parse_status, parsed_rows, quarantined_rows, error_count, file_id),
            )

    def replace_row_errors(self, file_id: str, errors: Iterable[dict]) -> None:
        with self._transact() as connection:
            connection.execute("DELETE FROM row_errors WHERE file_id = ?", (file_id,))
            for index, error in enumerate(errors):
                connection.execute(
                    """
                    INSERT INTO row_errors (
                        error_id, file_id, line_number, error_code, error_message, raw_excerpt
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "%s-%04d" % (file_id, index),
                        file_id,
                        int(error.get("line_number", 0)),
                        error.get("error_code", "unknown_error"),
                        error.get("error_message", ""),
                        error.get("raw_excerpt", ""),
                    ),
                )

    def upsert_artifact(
        self, snapshot_id: str, artifact_kind: str, relative_path: str, row_count: int
    ) -> None:
        with self._transact() as connection:
            connection.execute(
                """
                INSERT INTO artifacts (
                    artifact_id, snapshot_id, artifact_kind, relative_path, row_count, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(snapshot_id, artifact_kind, relative_path)
                DO UPDATE SET row_count = excluded.row_count, created_at = excluded.created_at
                """,
                (
                    uuid.uuid4().hex,
                    snapshot_id,
                    artifact_kind,
                    relative_path,
                    row_count,
                    utcnow_iso(),
                ),
            )

    def list_artifacts(
        self, snapshot_id: str, artifact_kind: Optional[str] = None
    ) -> List[sqlite3.Row]:
        query = "SELECT * FROM artifacts WHERE snapshot_id = ?"
        params: List[object] = [snapshot_id]
        if artifact_kind is not None:
            query += " AND artifact_kind = ?"
            params.append(artifact_kind)
        query += " ORDER BY artifact_kind, relative_path"
        with self._transact() as connection:
            return connection.execute(query, tuple(params)).fetchall()

    def _assert_snapshot_identity(
        self,
        connection,
        snapshot_id: str,
        raw_root: Path,
        source_family: str,
        snapshot_date: str,
        parser_bundle_version: str,
        discovered_files: List[dict],
    ) -> None:
        existing = connection.execute(
            "SELECT * FROM snapshots WHERE snapshot_id = ?",
            (snapshot_id,),
        ).fetchone()
        if existing["raw_root"] != str(raw_root):
            raise SnapshotModifiedError("snapshot modified: raw_root changed")
        if existing["source_family"] != source_family:
            raise SnapshotModifiedError("snapshot modified: source_family changed")
        if existing["snapshot_date"] != snapshot_date:
            raise SnapshotModifiedError("snapshot modified: snapshot_date changed")
        if existing["parser_bundle_version"] != parser_bundle_version:
            raise SnapshotModifiedError("snapshot modified: parser version changed")
        registered_files = connection.execute(
            """
            SELECT collector_name, relative_path, content_sha256
            FROM files
            WHERE snapshot_id = ?
            ORDER BY collector_name, relative_path
            """,
            (snapshot_id,),
        ).fetchall()
        current = [
            (
                row["collector_name"],
                row["relative_path"],
                row["content_sha256"],
            )
            for row in discovered_files
        ]
        stored = [
            (
                row["collector_name"],
                row["relative_path"],
                row["content_sha256"],
            )
            for row in registered_files
        ]
        if current != stored:
            raise SnapshotModifiedError("snapshot modified: registered file set changed")


def _stable_file_id(snapshot_id: str, collector_name: str, relative_path: str) -> str:
    digest = uuid.uuid5(
        uuid.NAMESPACE_URL,
        "%s|%s|%s" % (snapshot_id, collector_name, relative_path),
    )
    return digest.hex
