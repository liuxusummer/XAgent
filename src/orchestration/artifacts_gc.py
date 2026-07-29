"""Conservative, recoverable garbage collection for local Artifacts.

Reachability is derived only from complete ``ArtifactRef`` objects present in
committed Domain Events or their current projections.  Digest-shaped strings
are never treated as references.

Collection is intentionally not an unlink operation.  Old unreachable content
is atomically moved to a managed quarantine on the same filesystem and can be
verified and restored.  Expired regular temporary files left by a crashed
local write follow a separate verified quarantine path.  For canonical
references committed through ``DurableRunStore`` Domain Events, a durable
per-object claim linearizes reference registration against quarantine.  A
grace period and second scan remain conservative filters, but arbitrary
external registration outside that Event transaction has no such guarantee.
"""

from __future__ import annotations

import fcntl
import hashlib
import math
import os
import re
import stat
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterator

from .artifacts import ArtifactRef, LocalArtifactStore
from .store import DurableRunStore

MIN_ARTIFACT_GC_GRACE_SECONDS = 60.0
DEFAULT_ARTIFACT_GC_GRACE_SECONDS = 24 * 60 * 60.0
DEFAULT_ARTIFACT_GC_LIMIT = 100
MAX_ARTIFACT_GC_LIMIT = 1_000
MAX_MANAGED_ARTIFACT_FILES = 1_000_000
MAX_REACHABILITY_RUNS = 100_000
MAX_REACHABILITY_RECORDS = 2_000_000

_ARTIFACT_FIELDS = frozenset(ArtifactRef.__dataclass_fields__)
_LOWER_HEX_PAIR = re.compile(r"^[0-9a-f]{2}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_TEMP_ARTIFACT = re.compile(
    r"^\.([0-9a-f]{64})\.([a-z0-9_]{6,64})\.tmp$"
)
_QUARANTINE_ID = re.compile(r"^q([0-9]{20})_([0-9a-f]{32})$")
_QUARANTINE_RELATIVE_ROOT = Path(".artifact-quarantine") / "v1"
_TEMP_QUARANTINE_RELATIVE_ROOT = (
    Path(".artifact-quarantine") / "temp-v1"
)
_GC_LOCK_RELATIVE_PATH = Path(".artifact-quarantine") / "gc-v1.lock"


class ArtifactGCError(RuntimeError):
    """Base error for local Artifact lifecycle operations."""


class ArtifactGCIntegrityError(ArtifactGCError):
    """A managed tree or quarantine entry failed closed validation."""


class ArtifactGCRestoreError(ArtifactGCError):
    """A quarantined Artifact could not be safely restored."""


@dataclass(frozen=True, slots=True)
class QuarantinedArtifact:
    quarantine_id: str
    sha256: str
    size: int
    quarantined_at: float

    def __post_init__(self) -> None:
        quarantined_at = _validate_quarantine_identity(
            self.quarantine_id,
            self.quarantined_at,
        )
        if not isinstance(self.sha256, str) or not _SHA256.fullmatch(self.sha256):
            raise ArtifactGCIntegrityError("quarantine digest is invalid")
        if isinstance(self.size, bool) or not isinstance(self.size, int) or self.size < 0:
            raise ArtifactGCIntegrityError("quarantine size is invalid")
        object.__setattr__(self, "quarantined_at", quarantined_at)

    def to_dict(self) -> dict[str, str | int | float]:
        return {
            "quarantine_id": self.quarantine_id,
            "sha256": self.sha256,
            "size": self.size,
            "quarantined_at": self.quarantined_at,
        }


@dataclass(frozen=True, slots=True)
class QuarantinedTemporaryArtifact:
    quarantine_id: str
    intended_sha256: str
    content_sha256: str
    temporary_name: str
    size: int
    quarantined_at: float

    def __post_init__(self) -> None:
        quarantined_at = _validate_quarantine_identity(
            self.quarantine_id,
            self.quarantined_at,
        )
        if (
            not isinstance(self.intended_sha256, str)
            or not _SHA256.fullmatch(self.intended_sha256)
            or not isinstance(self.content_sha256, str)
            or not _SHA256.fullmatch(self.content_sha256)
        ):
            raise ArtifactGCIntegrityError(
                "temporary quarantine digest is invalid"
            )
        match = (
            _TEMP_ARTIFACT.fullmatch(self.temporary_name)
            if isinstance(self.temporary_name, str)
            else None
        )
        if match is None or match.group(1) != self.intended_sha256:
            raise ArtifactGCIntegrityError(
                "temporary quarantine identity is invalid"
            )
        if (
            isinstance(self.size, bool)
            or not isinstance(self.size, int)
            or self.size < 0
        ):
            raise ArtifactGCIntegrityError(
                "temporary quarantine size is invalid"
            )
        object.__setattr__(self, "quarantined_at", quarantined_at)

    def to_dict(self) -> dict[str, str | int | float]:
        return {
            "quarantine_id": self.quarantine_id,
            "intended_sha256": self.intended_sha256,
            "content_sha256": self.content_sha256,
            "temporary_name": self.temporary_name,
            "size": self.size,
            "quarantined_at": self.quarantined_at,
        }


@dataclass(frozen=True, slots=True)
class ArtifactGCReport:
    dry_run: bool
    grace_seconds: float
    limit: int
    scanned_files: int
    scanned_temporary_files: int
    reachable_digests: int
    eligible_orphans: int
    eligible_temporary_files: int
    retained_reachable: int
    retained_recent: int
    retained_recent_temporary_files: int
    skipped_integrity: int
    second_scan_performed: bool
    quarantined: tuple[QuarantinedArtifact, ...] = ()
    quarantined_temporary: tuple[
        QuarantinedTemporaryArtifact,
        ...,
    ] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "dry_run": self.dry_run,
            "grace_seconds": self.grace_seconds,
            "limit": self.limit,
            "scanned_files": self.scanned_files,
            "scanned_temporary_files": self.scanned_temporary_files,
            "reachable_digests": self.reachable_digests,
            "eligible_orphans": self.eligible_orphans,
            "eligible_temporary_files": self.eligible_temporary_files,
            "retained_reachable": self.retained_reachable,
            "retained_recent": self.retained_recent,
            "retained_recent_temporary_files": (
                self.retained_recent_temporary_files
            ),
            "skipped_integrity": self.skipped_integrity,
            "second_scan_performed": self.second_scan_performed,
            "quarantined": [entry.to_dict() for entry in self.quarantined],
            "quarantined_temporary": [
                entry.to_dict() for entry in self.quarantined_temporary
            ],
        }


@dataclass(frozen=True, slots=True)
class _StoredArtifact:
    sha256: str
    size: int
    modified_at: float
    device: int
    inode: int
    modified_ns: int
    links: int


@dataclass(frozen=True, slots=True)
class _StoredTemporaryArtifact:
    intended_sha256: str
    temporary_name: str
    size: int
    modified_at: float
    device: int
    inode: int
    modified_ns: int
    links: int


class LocalArtifactGarbageCollector:
    """Scan committed reachability and quarantine old local orphans."""

    def __init__(
        self,
        artifact_store: LocalArtifactStore,
        run_store: DurableRunStore,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if not isinstance(artifact_store, LocalArtifactStore):
            raise TypeError("artifact_store must be a LocalArtifactStore")
        if not isinstance(run_store, DurableRunStore):
            raise TypeError("run_store must be a DurableRunStore")
        if not callable(clock):
            raise TypeError("clock must be callable")
        self.artifact_store = artifact_store
        self.run_store = run_store
        self._clock = clock

    def collect(
        self,
        *,
        dry_run: bool = True,
        grace_seconds: float = DEFAULT_ARTIFACT_GC_GRACE_SECONDS,
        limit: int = DEFAULT_ARTIFACT_GC_LIMIT,
    ) -> ArtifactGCReport:
        """Find old orphans and optionally move them into quarantine."""

        with self._exclusive_gc_lock():
            self._recover_interrupted_claims()
            return self._collect_locked(
                dry_run=dry_run,
                grace_seconds=grace_seconds,
                limit=limit,
            )

    def _collect_locked(
        self,
        *,
        dry_run: bool,
        grace_seconds: float,
        limit: int,
    ) -> ArtifactGCReport:
        if not isinstance(dry_run, bool):
            raise ValueError("dry_run must be boolean")
        grace = _grace_seconds(grace_seconds)
        bounded_limit = _limit(limit)
        now = _timestamp(self._clock(), "clock")
        reachable = self.scan_reachable_digests()
        candidates, temporary_candidates = self._scan_managed_artifacts()
        retained_reachable = 0
        retained_recent = 0
        retained_recent_temporary = 0
        skipped_integrity = 0
        eligible: list[_StoredArtifact] = []
        eligible_temporary: list[
            tuple[_StoredTemporaryArtifact, str]
        ] = []
        for candidate in candidates:
            if candidate.sha256 in reachable:
                retained_reachable += 1
                continue
            if now - candidate.modified_at < grace:
                retained_recent += 1
                continue
            if not self._candidate_is_valid(candidate):
                skipped_integrity += 1
                continue
            eligible.append(candidate)
        for candidate in temporary_candidates:
            if now - candidate.modified_at < grace:
                retained_recent_temporary += 1
                continue
            content_digest = self._temporary_content_digest(candidate)
            if content_digest is None:
                skipped_integrity += 1
                continue
            eligible_temporary.append((candidate, content_digest))
        selected: list[
            tuple[
                str,
                _StoredArtifact
                | tuple[_StoredTemporaryArtifact, str],
            ]
        ] = [
            ("artifact", candidate) for candidate in eligible
        ]
        selected.extend(
            ("temporary", candidate)
            for candidate in eligible_temporary
        )
        selected.sort(
            key=lambda item: (
                (
                    item[1][0].modified_at
                    if isinstance(item[1], tuple)
                    else item[1].modified_at
                ),
                (
                    item[1][0].temporary_name
                    if isinstance(item[1], tuple)
                    else item[1].sha256
                ),
            )
        )
        selected = selected[:bounded_limit]
        if dry_run or not selected:
            return ArtifactGCReport(
                dry_run=dry_run,
                grace_seconds=grace,
                limit=bounded_limit,
                scanned_files=len(candidates),
                scanned_temporary_files=len(temporary_candidates),
                reachable_digests=len(reachable),
                eligible_orphans=len(eligible),
                eligible_temporary_files=len(eligible_temporary),
                retained_reachable=retained_reachable,
                retained_recent=retained_recent,
                retained_recent_temporary_files=(
                    retained_recent_temporary
                ),
                skipped_integrity=skipped_integrity,
                second_scan_performed=False,
            )

        # The second snapshot remains a conservative filter.  The durable
        # per-object claim below is the linearization point for references
        # committed through DurableRunStore Event transactions.
        reachable_again = self.scan_reachable_digests()
        quarantined: list[QuarantinedArtifact] = []
        quarantined_temporary: list[
            QuarantinedTemporaryArtifact
        ] = []
        for candidate_kind, selected_candidate in selected:
            if candidate_kind == "artifact":
                assert isinstance(selected_candidate, _StoredArtifact)
                if selected_candidate.sha256 in reachable_again:
                    retained_reachable += 1
                    continue
                if not self._candidate_is_valid(selected_candidate):
                    skipped_integrity += 1
                    continue
                entry = self._new_quarantine_entry(
                    selected_candidate,
                    now=now,
                )
                if not self.run_store.claim_artifact_gc_candidate(
                    selected_candidate.sha256,
                    quarantine_id=entry.quarantine_id,
                    size=selected_candidate.size,
                    claimed_at=entry.quarantined_at,
                ):
                    retained_reachable += 1
                    continue
                moved = False
                try:
                    self._fault(
                        "artifact_gc.after_claim",
                        selected_candidate.sha256,
                    )
                    quarantined_entry = self._quarantine(
                        selected_candidate,
                        now=now,
                        entry=entry,
                    )
                    moved = True
                    self._fault(
                        "artifact_gc.after_move",
                        selected_candidate.sha256,
                    )
                    self.run_store.mark_artifact_gc_quarantined(
                        selected_candidate.sha256,
                        quarantine_id=entry.quarantine_id,
                    )
                except Exception as exc:
                    if (
                        isinstance(exc, ArtifactGCIntegrityError)
                        and "state was not confirmed" in str(exc)
                    ):
                        raise
                    try:
                        if moved:
                            self._rollback_quarantine_move(
                                selected_candidate,
                                entry,
                            )
                        self.run_store.release_artifact_gc_claim(
                            selected_candidate.sha256,
                            quarantine_id=entry.quarantine_id,
                            expected_state="moving",
                        )
                    except Exception as rollback_exc:
                        raise ArtifactGCIntegrityError(
                            "Artifact GC failed; recoverable state was not confirmed"
                        ) from rollback_exc
                    raise ArtifactGCIntegrityError(
                        "Artifact GC claim was rolled back"
                    ) from exc
                quarantined.append(quarantined_entry)
                continue
            assert isinstance(selected_candidate, tuple)
            temporary, original_content_digest = selected_candidate
            content_digest = self._temporary_content_digest(temporary)
            if (
                content_digest is None
                or content_digest != original_content_digest
            ):
                skipped_integrity += 1
                continue
            quarantined_temporary.append(
                self._quarantine_temporary(
                    temporary,
                    content_digest,
                    now=now,
                )
            )
        return ArtifactGCReport(
            dry_run=False,
            grace_seconds=grace,
            limit=bounded_limit,
            scanned_files=len(candidates),
            scanned_temporary_files=len(temporary_candidates),
            reachable_digests=len(reachable_again),
            eligible_orphans=len(eligible),
            eligible_temporary_files=len(eligible_temporary),
            retained_reachable=retained_reachable,
            retained_recent=retained_recent,
            retained_recent_temporary_files=retained_recent_temporary,
            skipped_integrity=skipped_integrity,
            second_scan_performed=True,
            quarantined=tuple(quarantined),
            quarantined_temporary=tuple(quarantined_temporary),
        )

    def scan_reachable_digests(self) -> frozenset[str]:
        """Return local digests from complete committed ``ArtifactRef`` values."""

        reachable: set[str] = set()
        run_count = 0
        record_count = 0
        offset = 0
        while True:
            runs = self.run_store.list_runs(limit=1_000, offset=offset)
            if not runs:
                break
            run_count += len(runs)
            record_count += len(runs)
            if run_count > MAX_REACHABILITY_RUNS:
                raise ArtifactGCIntegrityError(
                    "reachability run bound was exceeded"
                )
            for run in runs:
                self._collect_refs(run.to_dict(), reachable)
                nodes = self.run_store.list_nodes(run.run_id)
                attempts = self.run_store.list_attempts(run.run_id)
                record_count += len(nodes) + len(attempts)
                if record_count > MAX_REACHABILITY_RECORDS:
                    raise ArtifactGCIntegrityError(
                        "reachability record bound was exceeded"
                    )
                for record in (*nodes, *attempts):
                    self._collect_refs(record.to_dict(), reachable)
                after_sequence = 0
                while True:
                    events = self.run_store.list_events(
                        run.run_id,
                        after_seq=after_sequence,
                        limit=1_000,
                    )
                    if not events:
                        break
                    record_count += len(events)
                    if record_count > MAX_REACHABILITY_RECORDS:
                        raise ArtifactGCIntegrityError(
                            "reachability record bound was exceeded"
                        )
                    for event in events:
                        self._collect_refs(event.to_dict(), reachable)
                    after_sequence = events[-1].seq
                    if len(events) < 1_000:
                        break
            if record_count > MAX_REACHABILITY_RECORDS:
                raise ArtifactGCIntegrityError(
                    "reachability record bound was exceeded"
                )
            offset += len(runs)
            if len(runs) < 1_000:
                break
        return frozenset(reachable)

    def list_quarantined(
        self,
        *,
        limit: int = MAX_ARTIFACT_GC_LIMIT,
    ) -> tuple[QuarantinedArtifact, ...]:
        """List verified quarantine entries without exposing filesystem paths."""

        with self._exclusive_gc_lock():
            self._recover_interrupted_claims()
            return self._list_quarantined_locked(limit=limit)

    def _list_quarantined_locked(
        self,
        *,
        limit: int,
    ) -> tuple[QuarantinedArtifact, ...]:
        bounded_limit = _limit(limit)
        root = self.artifact_store.root / _QUARANTINE_RELATIVE_ROOT
        quarantine_root = self.artifact_store.root / ".artifact-quarantine"
        if not self._entry_exists(quarantine_root):
            return ()
        self._require_safe_directory(self.artifact_store.root)
        self._require_safe_directory(quarantine_root)
        if not self._entry_exists(root):
            return ()
        self._require_safe_directory(root)
        entries: list[QuarantinedArtifact] = []
        for prefix in self._directory_entries(root):
            if not _LOWER_HEX_PAIR.fullmatch(prefix.name):
                raise ArtifactGCIntegrityError(
                    "quarantine tree failed safety validation"
                )
            self._require_directory_entry(prefix)
            for digest_entry in self._directory_entries(Path(prefix.path)):
                digest = digest_entry.name
                if (
                    not _SHA256.fullmatch(digest)
                    or digest[:2] != prefix.name
                ):
                    raise ArtifactGCIntegrityError(
                        "quarantine tree failed safety validation"
                    )
                self._require_directory_entry(digest_entry)
                for file_entry in self._directory_entries(
                    Path(digest_entry.path)
                ):
                    match = _QUARANTINE_ID.fullmatch(file_entry.name)
                    stored = self._regular_entry_stat(file_entry)
                    if match is None:
                        raise ArtifactGCIntegrityError(
                            "quarantine tree failed safety validation"
                        )
                    quarantined_at = int(match.group(1)) / 1_000_000_000
                    candidate = _StoredArtifact(
                        digest,
                        stored.st_size,
                        stored.st_mtime,
                        stored.st_dev,
                        stored.st_ino,
                        stored.st_mtime_ns,
                        stored.st_nlink,
                    )
                    if not self._path_matches_candidate(
                        Path(file_entry.path),
                        candidate,
                    ):
                        raise ArtifactGCIntegrityError(
                            "quarantine entry failed integrity validation"
                        )
                    entries.append(
                        QuarantinedArtifact(
                            file_entry.name,
                            digest,
                            stored.st_size,
                            quarantined_at,
                        )
                    )
                    if len(entries) > MAX_ARTIFACT_GC_LIMIT:
                        raise ArtifactGCIntegrityError(
                            "quarantine listing bound was exceeded"
                        )
        entries.sort(key=lambda item: (item.quarantined_at, item.quarantine_id))
        return tuple(entries[:bounded_limit])

    def list_quarantined_temporary(
        self,
        *,
        limit: int = MAX_ARTIFACT_GC_LIMIT,
    ) -> tuple[QuarantinedTemporaryArtifact, ...]:
        """List verified crashed-write temporary files in quarantine."""

        with self._exclusive_gc_lock():
            self._recover_interrupted_claims()
            return self._list_quarantined_temporary_locked(limit=limit)

    def _list_quarantined_temporary_locked(
        self,
        *,
        limit: int,
    ) -> tuple[QuarantinedTemporaryArtifact, ...]:
        bounded_limit = _limit(limit)
        quarantine_root = self.artifact_store.root / ".artifact-quarantine"
        root = (
            self.artifact_store.root
            / _TEMP_QUARANTINE_RELATIVE_ROOT
        )
        if not self._entry_exists(quarantine_root):
            return ()
        self._require_safe_directory(self.artifact_store.root)
        self._require_safe_directory(quarantine_root)
        if not self._entry_exists(root):
            return ()
        self._require_safe_directory(root)
        entries: list[QuarantinedTemporaryArtifact] = []
        for prefix in self._directory_entries(root):
            if not _LOWER_HEX_PAIR.fullmatch(prefix.name):
                raise ArtifactGCIntegrityError(
                    "temporary quarantine tree failed safety validation"
                )
            self._require_directory_entry(prefix)
            for digest_entry in self._directory_entries(Path(prefix.path)):
                intended_digest = digest_entry.name
                if (
                    not _SHA256.fullmatch(intended_digest)
                    or intended_digest[:2] != prefix.name
                ):
                    raise ArtifactGCIntegrityError(
                        "temporary quarantine tree failed safety validation"
                    )
                self._require_directory_entry(digest_entry)
                for identity_entry in self._directory_entries(
                    Path(digest_entry.path)
                ):
                    match = _QUARANTINE_ID.fullmatch(identity_entry.name)
                    if match is None:
                        raise ArtifactGCIntegrityError(
                            "temporary quarantine tree failed safety validation"
                        )
                    self._require_directory_entry(identity_entry)
                    files = self._directory_entries(Path(identity_entry.path))
                    if not files:
                        continue
                    if len(files) != 1:
                        raise ArtifactGCIntegrityError(
                            "temporary quarantine tree failed safety validation"
                        )
                    file_entry = files[0]
                    temporary_match = _TEMP_ARTIFACT.fullmatch(
                        file_entry.name
                    )
                    stored = self._regular_entry_stat(file_entry)
                    if (
                        temporary_match is None
                        or temporary_match.group(1) != intended_digest
                    ):
                        raise ArtifactGCIntegrityError(
                            "temporary quarantine tree failed safety validation"
                        )
                    candidate = _StoredTemporaryArtifact(
                        intended_digest,
                        file_entry.name,
                        stored.st_size,
                        stored.st_mtime,
                        stored.st_dev,
                        stored.st_ino,
                        stored.st_mtime_ns,
                        stored.st_nlink,
                    )
                    content_digest = self._verified_file_digest(
                        Path(file_entry.path),
                        candidate,
                    )
                    if content_digest is None:
                        raise ArtifactGCIntegrityError(
                            "temporary quarantine entry failed validation"
                        )
                    entries.append(
                        QuarantinedTemporaryArtifact(
                            identity_entry.name,
                            intended_digest,
                            content_digest,
                            file_entry.name,
                            stored.st_size,
                            int(match.group(1)) / 1_000_000_000,
                        )
                    )
                    if len(entries) > MAX_ARTIFACT_GC_LIMIT:
                        raise ArtifactGCIntegrityError(
                            "temporary quarantine listing bound was exceeded"
                        )
        entries.sort(key=lambda item: (item.quarantined_at, item.quarantine_id))
        return tuple(entries[:bounded_limit])

    def restore(
        self,
        entry: QuarantinedArtifact,
        ref: ArtifactRef,
    ) -> ArtifactRef:
        """Verify and atomically restore one quarantined local Artifact."""

        with self._exclusive_gc_lock():
            self._recover_interrupted_claims()
            return self._restore_locked(entry, ref)

    def _restore_locked(
        self,
        entry: QuarantinedArtifact,
        ref: ArtifactRef,
    ) -> ArtifactRef:
        try:
            restored_entry = QuarantinedArtifact(**entry.to_dict())
            validated_ref = self.artifact_store._validated_ref(ref)
        except Exception as exc:
            raise ArtifactGCRestoreError(
                "restore request failed validation"
            ) from exc
        if (
            restored_entry.sha256 != validated_ref.sha256
            or restored_entry.size != validated_ref.size
        ):
            raise ArtifactGCRestoreError(
                "restore reference does not match quarantine entry"
            )
        source = self._quarantine_path(restored_entry)
        self._require_quarantine_parent(restored_entry)
        candidate = self._candidate_from_path(
            source,
            restored_entry.sha256,
        )
        if (
            candidate is None
            or candidate.size != restored_entry.size
            or not self._path_matches_candidate(source, candidate)
        ):
            raise ArtifactGCRestoreError(
                "quarantine entry failed integrity validation"
            )
        destination = self.artifact_store._path_for_digest(
            restored_entry.sha256
        )
        try:
            self.artifact_store._ensure_parent(destination.parent)
            existing = destination.lstat()
        except FileNotFoundError:
            existing = None
        except Exception as exc:
            raise ArtifactGCRestoreError(
                "restore destination failed safety validation"
            ) from exc
        if existing is not None:
            raise ArtifactGCRestoreError("restore destination already exists")
        moved = False
        claim_release_started = False
        try:
            os.replace(source, destination)
            moved = True
            self.artifact_store._fsync_directory(source.parent)
            self.artifact_store._fsync_directory(destination.parent)
            if self.artifact_store.verify(validated_ref) is not True:
                raise ArtifactGCIntegrityError(
                    "restored Artifact failed integrity validation"
                )
            claim = self.run_store.get_artifact_gc_claim(
                restored_entry.sha256
            )
            if claim is not None:
                if (
                    claim.quarantine_id != restored_entry.quarantine_id
                    or claim.state != "quarantined"
                ):
                    raise ArtifactGCIntegrityError(
                        "restore does not match the durable GC claim"
                    )
                claim_release_started = True
                self.run_store.release_artifact_gc_claim(
                    restored_entry.sha256,
                    quarantine_id=restored_entry.quarantine_id,
                    expected_state="quarantined",
                )
        except Exception as exc:
            if claim_release_started:
                try:
                    if self.artifact_store.verify(validated_ref) is not True:
                        raise ArtifactGCIntegrityError(
                            "restored Artifact is no longer readable"
                        )
                except Exception as verification_exc:
                    raise ArtifactGCRestoreError(
                        "restore claim release and source state were not confirmed"
                    ) from verification_exc
                raise ArtifactGCRestoreError(
                    "restore claim release was not confirmed; "
                    "restored bytes remain available"
                ) from exc
            if moved:
                try:
                    if self._entry_exists(source):
                        raise ArtifactGCIntegrityError(
                            "quarantine source was occupied during rollback"
                        )
                    os.replace(destination, source)
                    self.artifact_store._fsync_directory(destination.parent)
                    self.artifact_store._fsync_directory(source.parent)
                    rolled_back = self._candidate_from_path(
                        source,
                        restored_entry.sha256,
                    )
                    if (
                        rolled_back is None
                        or rolled_back.size != restored_entry.size
                        or not self._path_matches_candidate(
                            source,
                            rolled_back,
                        )
                    ):
                        raise ArtifactGCIntegrityError(
                            "quarantine rollback failed integrity validation"
                        )
                except Exception as rollback_exc:
                    raise ArtifactGCRestoreError(
                        "restore failed and recoverable state was not confirmed"
                    ) from rollback_exc
            raise ArtifactGCRestoreError(
                "restore failed; Artifact remains quarantined"
            ) from exc
        return validated_ref

    def restore_temporary(
        self,
        entry: QuarantinedTemporaryArtifact,
    ) -> QuarantinedTemporaryArtifact:
        """Restore one verified temporary file to its managed temp name."""

        with self._exclusive_gc_lock():
            self._recover_interrupted_claims()
            return self._restore_temporary_locked(entry)

    def _restore_temporary_locked(
        self,
        entry: QuarantinedTemporaryArtifact,
    ) -> QuarantinedTemporaryArtifact:
        try:
            restored_entry = QuarantinedTemporaryArtifact(**entry.to_dict())
        except Exception as exc:
            raise ArtifactGCRestoreError(
                "temporary restore request failed validation"
            ) from exc
        self._require_temporary_quarantine_parent(restored_entry)
        source = self._temporary_quarantine_path(restored_entry)
        candidate = self._temporary_candidate_from_path(
            source,
            restored_entry,
        )
        if (
            candidate is None
            or self._verified_file_digest(source, candidate)
            != restored_entry.content_sha256
        ):
            raise ArtifactGCRestoreError(
                "temporary quarantine entry failed integrity validation"
            )
        destination = self._temporary_path(
            restored_entry.intended_sha256,
            restored_entry.temporary_name,
        )
        try:
            self.artifact_store._ensure_parent(destination.parent)
            existing = destination.lstat()
        except FileNotFoundError:
            existing = None
        except Exception as exc:
            raise ArtifactGCRestoreError(
                "temporary restore destination failed safety validation"
            ) from exc
        if existing is not None:
            raise ArtifactGCRestoreError(
                "temporary restore destination already exists"
            )
        moved = False
        try:
            os.replace(source, destination)
            moved = True
            self.artifact_store._fsync_directory(source.parent)
            self.artifact_store._fsync_directory(destination.parent)
            if (
                self._verified_file_digest(destination, candidate)
                != restored_entry.content_sha256
            ):
                raise ArtifactGCIntegrityError(
                    "restored temporary file failed integrity validation"
                )
        except Exception as exc:
            if moved:
                try:
                    if self._entry_exists(source):
                        raise ArtifactGCIntegrityError(
                            "temporary quarantine source was occupied"
                        )
                    os.replace(destination, source)
                    self.artifact_store._fsync_directory(destination.parent)
                    self.artifact_store._fsync_directory(source.parent)
                    rolled_back = self._temporary_candidate_from_path(
                        source,
                        restored_entry,
                    )
                    if (
                        rolled_back is None
                        or self._verified_file_digest(source, rolled_back)
                        != restored_entry.content_sha256
                    ):
                        raise ArtifactGCIntegrityError(
                            "temporary quarantine rollback failed validation"
                        )
                except Exception as rollback_exc:
                    raise ArtifactGCRestoreError(
                        "temporary restore failed and state was not confirmed"
                    ) from rollback_exc
            raise ArtifactGCRestoreError(
                "restore failed; temporary file remains quarantined"
            ) from exc
        return restored_entry

    def _collect_refs(self, value: Any, reachable: set[str]) -> None:
        stack: list[tuple[Any, int]] = [(value, 0)]
        while stack:
            item, depth = stack.pop()
            if depth > 32:
                raise ArtifactGCIntegrityError(
                    "committed reachability depth was exceeded"
                )
            if isinstance(item, dict):
                if set(item) == set(_ARTIFACT_FIELDS):
                    try:
                        ref = ArtifactRef.from_dict(item)
                        if ref.to_dict() != item:
                            continue
                        local_ref = self.artifact_store._validated_ref(ref)
                    except Exception:
                        continue
                    reachable.add(local_ref.sha256)
                    continue
                stack.extend((child, depth + 1) for child in item.values())
            elif isinstance(item, list):
                stack.extend((child, depth + 1) for child in item)

    @contextmanager
    def _exclusive_gc_lock(self) -> Iterator[None]:
        lock_path = self.artifact_store.root / _GC_LOCK_RELATIVE_PATH
        self.artifact_store._ensure_parent(lock_path.parent)
        flags = os.O_RDWR | os.O_CREAT
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(lock_path, flags, 0o600)
        except OSError as exc:
            raise ArtifactGCIntegrityError(
                "Artifact GC lock cannot be opened safely"
            ) from exc
        try:
            stored = os.fstat(descriptor)
            if not stat.S_ISREG(stored.st_mode) or stored.st_nlink != 1:
                raise ArtifactGCIntegrityError(
                    "Artifact GC lock failed safety validation"
                )
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)

    def _recover_interrupted_claims(self) -> None:
        claims = []
        offset = 0
        while True:
            page = self.run_store.list_artifact_gc_claims(
                limit=1_000,
                offset=offset,
            )
            claims.extend(page)
            if len(claims) > MAX_MANAGED_ARTIFACT_FILES:
                raise ArtifactGCIntegrityError(
                    "Artifact GC claim recovery bound was exceeded"
                )
            if len(page) < 1_000:
                break
            offset += len(page)
        for claim in claims:
            entry = QuarantinedArtifact(
                claim.quarantine_id,
                claim.sha256,
                claim.size,
                claim.claimed_at,
            )
            source = self.artifact_store._path_for_digest(claim.sha256)
            destination = self._quarantine_path(entry)
            source_candidate = self._candidate_from_path(
                source,
                claim.sha256,
            )
            destination_candidate = self._candidate_from_path(
                destination,
                claim.sha256,
            )
            source_valid = (
                source_candidate is not None
                and source_candidate.size == claim.size
                and self._path_matches_candidate(source, source_candidate)
            )
            destination_valid = (
                destination_candidate is not None
                and destination_candidate.size == claim.size
                and self._path_matches_candidate(
                    destination,
                    destination_candidate,
                )
            )
            source_exists = self._entry_exists(source)
            destination_exists = self._entry_exists(destination)
            if claim.state == "moving":
                if source_valid and not destination_exists:
                    self.run_store.release_artifact_gc_claim(
                        claim.sha256,
                        quarantine_id=claim.quarantine_id,
                        expected_state="moving",
                    )
                    continue
                if destination_valid and not source_exists:
                    self.run_store.mark_artifact_gc_quarantined(
                        claim.sha256,
                        quarantine_id=claim.quarantine_id,
                    )
                    continue
            elif claim.state == "quarantined":
                if destination_valid and not source_exists:
                    continue
                if source_valid and not destination_exists:
                    self.run_store.release_artifact_gc_claim(
                        claim.sha256,
                        quarantine_id=claim.quarantine_id,
                        expected_state="quarantined",
                    )
                    continue
            raise ArtifactGCIntegrityError(
                "interrupted Artifact GC state failed recovery validation"
            )

    @staticmethod
    def _fault(_stage: str, _sha256: str) -> None:
        """Fault-injection seam for claim/move crash recovery tests."""

    def _scan_managed_artifacts(
        self,
    ) -> tuple[
        list[_StoredArtifact],
        list[_StoredTemporaryArtifact],
    ]:
        root = self.artifact_store.root / "sha256"
        if not self._entry_exists(root):
            return [], []
        self._require_safe_directory(self.artifact_store.root)
        self._require_safe_directory(root)
        candidates: list[_StoredArtifact] = []
        temporary_candidates: list[_StoredTemporaryArtifact] = []
        for first in self._directory_entries(root):
            if not _LOWER_HEX_PAIR.fullmatch(first.name):
                raise ArtifactGCIntegrityError(
                    "artifact tree failed safety validation"
                )
            self._require_directory_entry(first)
            for second in self._directory_entries(Path(first.path)):
                if not _LOWER_HEX_PAIR.fullmatch(second.name):
                    raise ArtifactGCIntegrityError(
                        "artifact tree failed safety validation"
                    )
                self._require_directory_entry(second)
                for content in self._directory_entries(Path(second.path)):
                    temporary_match = _TEMP_ARTIFACT.fullmatch(
                        content.name
                    )
                    if temporary_match is not None:
                        intended_digest = temporary_match.group(1)
                        if (
                            intended_digest[:2] != first.name
                            or intended_digest[2:4] != second.name
                        ):
                            raise ArtifactGCIntegrityError(
                                "temporary Artifact tree failed safety validation"
                            )
                        stored = self._regular_entry_stat(content)
                        temporary_candidates.append(
                            _StoredTemporaryArtifact(
                                intended_digest,
                                content.name,
                                stored.st_size,
                                stored.st_mtime,
                                stored.st_dev,
                                stored.st_ino,
                                stored.st_mtime_ns,
                                stored.st_nlink,
                            )
                        )
                        self._check_managed_scan_bound(
                            len(candidates),
                            len(temporary_candidates),
                        )
                        continue
                    if (
                        not _SHA256.fullmatch(content.name)
                        or content.name[:2] != first.name
                        or content.name[2:4] != second.name
                    ):
                        raise ArtifactGCIntegrityError(
                            "artifact tree failed safety validation"
                        )
                    stored = self._regular_entry_stat(content)
                    candidates.append(
                        _StoredArtifact(
                            content.name,
                            stored.st_size,
                            stored.st_mtime,
                            stored.st_dev,
                            stored.st_ino,
                            stored.st_mtime_ns,
                            stored.st_nlink,
                        )
                    )
                    self._check_managed_scan_bound(
                        len(candidates),
                        len(temporary_candidates),
                    )
        candidates.sort(key=lambda item: item.sha256)
        temporary_candidates.sort(
            key=lambda item: (
                item.intended_sha256,
                item.temporary_name,
            )
        )
        return candidates, temporary_candidates

    @staticmethod
    def _check_managed_scan_bound(
        artifact_count: int,
        temporary_count: int,
    ) -> None:
        if artifact_count + temporary_count > MAX_MANAGED_ARTIFACT_FILES:
            raise ArtifactGCIntegrityError(
                "managed Artifact scan bound was exceeded"
            )

    def _candidate_is_valid(self, candidate: _StoredArtifact) -> bool:
        path = self.artifact_store._path_for_digest(candidate.sha256)
        return self._path_matches_candidate(path, candidate)

    def _temporary_content_digest(
        self,
        candidate: _StoredTemporaryArtifact,
    ) -> str | None:
        path = (
            self.artifact_store.root
            / "sha256"
            / candidate.intended_sha256[:2]
            / candidate.intended_sha256[2:4]
            / candidate.temporary_name
        )
        return self._verified_file_digest(path, candidate)

    def _path_matches_candidate(
        self,
        path: Path,
        candidate: _StoredArtifact | _StoredTemporaryArtifact,
    ) -> bool:
        digest = self._verified_file_digest(path, candidate)
        return (
            digest == candidate.sha256
            if isinstance(candidate, _StoredArtifact)
            else digest is not None
        )

    def _verified_file_digest(
        self,
        path: Path,
        candidate: _StoredArtifact | _StoredTemporaryArtifact,
    ) -> str | None:
        flags = os.O_RDONLY
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(path, flags)
        except OSError:
            return None
        try:
            stored = os.fstat(descriptor)
            if (
                not stat.S_ISREG(stored.st_mode)
                or stored.st_nlink != 1
                or stored.st_dev != candidate.device
                or stored.st_ino != candidate.inode
                or stored.st_size != candidate.size
                or stored.st_mtime_ns != candidate.modified_ns
            ):
                return None
            hasher = hashlib.sha256()
            while True:
                chunk = os.read(descriptor, 1024 * 1024)
                if not chunk:
                    break
                hasher.update(chunk)
            return hasher.hexdigest()
        finally:
            os.close(descriptor)

    def _quarantine(
        self,
        candidate: _StoredArtifact,
        *,
        now: float,
        entry: QuarantinedArtifact | None = None,
    ) -> QuarantinedArtifact:
        if entry is None:
            entry = self._new_quarantine_entry(candidate, now=now)
        elif (
            entry.sha256 != candidate.sha256
            or entry.size != candidate.size
        ):
            raise ArtifactGCIntegrityError(
                "quarantine entry does not match its candidate"
            )
        source = self.artifact_store._path_for_digest(candidate.sha256)
        destination = self._quarantine_path(entry)
        moved = False
        try:
            self.artifact_store._ensure_parent(destination.parent)
            source_root = self.artifact_store.root.stat()
            quarantine_root = (
                self.artifact_store.root / _QUARANTINE_RELATIVE_ROOT
            ).stat()
            if source_root.st_dev != quarantine_root.st_dev:
                raise ArtifactGCIntegrityError(
                    "quarantine must share the artifact filesystem"
                )
            if self._entry_exists(destination):
                raise ArtifactGCIntegrityError(
                    "quarantine identity already exists"
                )
            os.replace(source, destination)
            moved = True
            self.artifact_store._fsync_directory(source.parent)
            self.artifact_store._fsync_directory(destination.parent)
            if not self._path_matches_candidate(destination, candidate):
                raise ArtifactGCIntegrityError(
                    "quarantined Artifact failed integrity validation"
                )
        except Exception as exc:
            if moved:
                try:
                    if self._entry_exists(source):
                        raise ArtifactGCIntegrityError(
                            "artifact source was occupied during rollback"
                        )
                    os.replace(destination, source)
                    self.artifact_store._fsync_directory(
                        destination.parent
                    )
                    self.artifact_store._fsync_directory(source.parent)
                    if not self._path_matches_candidate(source, candidate):
                        raise ArtifactGCIntegrityError(
                            "artifact quarantine rollback failed validation"
                        )
                except Exception as rollback_exc:
                    raise ArtifactGCIntegrityError(
                        "artifact quarantine failed; state was not confirmed"
                    ) from rollback_exc
            raise ArtifactGCIntegrityError(
                "artifact quarantine failed; source was restored"
            ) from exc
        return entry

    @staticmethod
    def _new_quarantine_entry(
        candidate: _StoredArtifact,
        *,
        now: float,
    ) -> QuarantinedArtifact:
        timestamp_ns = int(now * 1_000_000_000)
        return QuarantinedArtifact(
            f"q{timestamp_ns:020d}_{uuid.uuid4().hex}",
            candidate.sha256,
            candidate.size,
            timestamp_ns / 1_000_000_000,
        )

    def _rollback_quarantine_move(
        self,
        candidate: _StoredArtifact,
        entry: QuarantinedArtifact,
    ) -> None:
        source = self.artifact_store._path_for_digest(candidate.sha256)
        destination = self._quarantine_path(entry)
        if self._entry_exists(source):
            raise ArtifactGCIntegrityError(
                "Artifact source was occupied during GC rollback"
            )
        os.replace(destination, source)
        self.artifact_store._fsync_directory(destination.parent)
        self.artifact_store._fsync_directory(source.parent)
        if not self._path_matches_candidate(source, candidate):
            raise ArtifactGCIntegrityError(
                "Artifact GC rollback failed integrity validation"
            )

    def _quarantine_temporary(
        self,
        candidate: _StoredTemporaryArtifact,
        content_digest: str,
        *,
        now: float,
    ) -> QuarantinedTemporaryArtifact:
        timestamp_ns = int(now * 1_000_000_000)
        entry = QuarantinedTemporaryArtifact(
            f"q{timestamp_ns:020d}_{uuid.uuid4().hex}",
            candidate.intended_sha256,
            content_digest,
            candidate.temporary_name,
            candidate.size,
            timestamp_ns / 1_000_000_000,
        )
        source = self._temporary_path(
            candidate.intended_sha256,
            candidate.temporary_name,
        )
        destination = self._temporary_quarantine_path(entry)
        moved = False
        try:
            self.artifact_store._ensure_parent(destination.parent)
            if (
                self.artifact_store.root.stat().st_dev
                != (
                    self.artifact_store.root
                    / _TEMP_QUARANTINE_RELATIVE_ROOT
                ).stat().st_dev
            ):
                raise ArtifactGCIntegrityError(
                    "temporary quarantine must share the artifact filesystem"
                )
            if self._entry_exists(destination):
                raise ArtifactGCIntegrityError(
                    "temporary quarantine identity already exists"
                )
            os.replace(source, destination)
            moved = True
            self.artifact_store._fsync_directory(source.parent)
            self.artifact_store._fsync_directory(destination.parent)
            if (
                self._verified_file_digest(destination, candidate)
                != content_digest
            ):
                raise ArtifactGCIntegrityError(
                    "quarantined temporary file failed validation"
                )
        except Exception as exc:
            if moved:
                try:
                    if self._entry_exists(source):
                        raise ArtifactGCIntegrityError(
                            "temporary source was occupied during rollback"
                        )
                    os.replace(destination, source)
                    self.artifact_store._fsync_directory(
                        destination.parent
                    )
                    self.artifact_store._fsync_directory(source.parent)
                    if (
                        self._verified_file_digest(source, candidate)
                        != content_digest
                    ):
                        raise ArtifactGCIntegrityError(
                            "temporary quarantine rollback failed validation"
                        )
                except Exception as rollback_exc:
                    raise ArtifactGCIntegrityError(
                        "temporary quarantine failed; state was not confirmed"
                    ) from rollback_exc
            raise ArtifactGCIntegrityError(
                "temporary quarantine failed; source was restored"
            ) from exc
        return entry

    def _quarantine_path(self, entry: QuarantinedArtifact) -> Path:
        relative = (
            _QUARANTINE_RELATIVE_ROOT
            / entry.sha256[:2]
            / entry.sha256
            / entry.quarantine_id
        )
        path = self.artifact_store.root / relative
        try:
            path.relative_to(self.artifact_store.root)
        except ValueError as exc:
            raise ArtifactGCIntegrityError(
                "quarantine path failed safety validation"
            ) from exc
        return path

    def _temporary_quarantine_path(
        self,
        entry: QuarantinedTemporaryArtifact,
    ) -> Path:
        relative = (
            _TEMP_QUARANTINE_RELATIVE_ROOT
            / entry.intended_sha256[:2]
            / entry.intended_sha256
            / entry.quarantine_id
            / entry.temporary_name
        )
        path = self.artifact_store.root / relative
        try:
            path.relative_to(self.artifact_store.root)
        except ValueError as exc:
            raise ArtifactGCIntegrityError(
                "temporary quarantine path failed safety validation"
            ) from exc
        return path

    def _temporary_path(
        self,
        intended_digest: str,
        temporary_name: str,
    ) -> Path:
        match = _TEMP_ARTIFACT.fullmatch(temporary_name)
        if (
            not _SHA256.fullmatch(intended_digest)
            or match is None
            or match.group(1) != intended_digest
        ):
            raise ArtifactGCIntegrityError(
                "temporary Artifact identity is invalid"
            )
        return (
            self.artifact_store.root
            / "sha256"
            / intended_digest[:2]
            / intended_digest[2:4]
            / temporary_name
        )

    def _require_quarantine_parent(
        self,
        entry: QuarantinedArtifact,
    ) -> None:
        current = self.artifact_store.root
        for component in (
            ".artifact-quarantine",
            "v1",
            entry.sha256[:2],
            entry.sha256,
        ):
            current = current / component
            self._require_safe_directory(current)

    def _require_temporary_quarantine_parent(
        self,
        entry: QuarantinedTemporaryArtifact,
    ) -> None:
        current = self.artifact_store.root
        for component in (
            ".artifact-quarantine",
            "temp-v1",
            entry.intended_sha256[:2],
            entry.intended_sha256,
            entry.quarantine_id,
        ):
            current = current / component
            self._require_safe_directory(current)

    def _candidate_from_path(
        self,
        path: Path,
        digest: str,
    ) -> _StoredArtifact | None:
        try:
            stored = path.lstat()
        except OSError:
            return None
        if not stat.S_ISREG(stored.st_mode) or stored.st_nlink != 1:
            return None
        return _StoredArtifact(
            digest,
            stored.st_size,
            stored.st_mtime,
            stored.st_dev,
            stored.st_ino,
            stored.st_mtime_ns,
            stored.st_nlink,
        )

    def _temporary_candidate_from_path(
        self,
        path: Path,
        entry: QuarantinedTemporaryArtifact,
    ) -> _StoredTemporaryArtifact | None:
        try:
            stored = path.lstat()
        except OSError:
            return None
        if not stat.S_ISREG(stored.st_mode) or stored.st_nlink != 1:
            return None
        if stored.st_size != entry.size:
            return None
        return _StoredTemporaryArtifact(
            entry.intended_sha256,
            entry.temporary_name,
            stored.st_size,
            stored.st_mtime,
            stored.st_dev,
            stored.st_ino,
            stored.st_mtime_ns,
            stored.st_nlink,
        )

    @staticmethod
    def _directory_entries(path: Path) -> list[os.DirEntry[str]]:
        try:
            with os.scandir(path) as scanner:
                return sorted(scanner, key=lambda item: item.name)
        except OSError as exc:
            raise ArtifactGCIntegrityError(
                "managed directory cannot be inspected safely"
            ) from exc

    @staticmethod
    def _entry_exists(path: Path) -> bool:
        try:
            path.lstat()
        except FileNotFoundError:
            return False
        except OSError as exc:
            raise ArtifactGCIntegrityError(
                "managed entry cannot be inspected safely"
            ) from exc
        return True

    @staticmethod
    def _require_safe_directory(path: Path) -> None:
        try:
            stored = path.lstat()
        except OSError as exc:
            raise ArtifactGCIntegrityError(
                "managed directory cannot be inspected safely"
            ) from exc
        if stat.S_ISLNK(stored.st_mode) or not stat.S_ISDIR(stored.st_mode):
            raise ArtifactGCIntegrityError(
                "managed directory failed safety validation"
            )

    @staticmethod
    def _require_directory_entry(entry: os.DirEntry[str]) -> None:
        try:
            stored = entry.stat(follow_symlinks=False)
        except OSError as exc:
            raise ArtifactGCIntegrityError(
                "managed directory entry cannot be inspected safely"
            ) from exc
        if stat.S_ISLNK(stored.st_mode) or not stat.S_ISDIR(stored.st_mode):
            raise ArtifactGCIntegrityError(
                "managed directory entry failed safety validation"
            )

    @staticmethod
    def _regular_entry_stat(entry: os.DirEntry[str]) -> os.stat_result:
        try:
            stored = entry.stat(follow_symlinks=False)
        except OSError as exc:
            raise ArtifactGCIntegrityError(
                "managed file cannot be inspected safely"
            ) from exc
        if (
            stat.S_ISLNK(stored.st_mode)
            or not stat.S_ISREG(stored.st_mode)
            or stored.st_nlink != 1
        ):
            raise ArtifactGCIntegrityError(
                "managed file failed safety validation"
            )
        return stored


def _timestamp(value: Any, field: str) -> float:
    try:
        timestamp = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a finite timestamp") from exc
    if not math.isfinite(timestamp) or timestamp < 0:
        raise ValueError(f"{field} must be a finite timestamp")
    return timestamp


def _validate_quarantine_identity(
    quarantine_id: Any,
    quarantined_at: Any,
) -> float:
    match = (
        _QUARANTINE_ID.fullmatch(quarantine_id)
        if isinstance(quarantine_id, str)
        else None
    )
    if match is None:
        raise ArtifactGCIntegrityError("quarantine identity is invalid")
    try:
        timestamp = float(quarantined_at)
    except (TypeError, ValueError) as exc:
        raise ArtifactGCIntegrityError(
            "quarantine timestamp is invalid"
        ) from exc
    if not math.isfinite(timestamp) or timestamp < 0:
        raise ArtifactGCIntegrityError(
            "quarantine timestamp is invalid"
        )
    expected_timestamp = int(match.group(1)) / 1_000_000_000
    if abs(timestamp - expected_timestamp) > 0.000001:
        raise ArtifactGCIntegrityError(
            "quarantine identity does not match its timestamp"
        )
    return timestamp


def _grace_seconds(value: Any) -> float:
    grace = _timestamp(value, "grace_seconds")
    if grace < MIN_ARTIFACT_GC_GRACE_SECONDS:
        raise ValueError(
            "grace_seconds is below the minimum Artifact GC grace"
        )
    return grace


def _limit(value: Any) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < 1
        or value > MAX_ARTIFACT_GC_LIMIT
    ):
        raise ValueError("limit is outside the Artifact GC bound")
    return value


__all__ = [
    "ArtifactGCError",
    "ArtifactGCIntegrityError",
    "ArtifactGCReport",
    "ArtifactGCRestoreError",
    "DEFAULT_ARTIFACT_GC_GRACE_SECONDS",
    "DEFAULT_ARTIFACT_GC_LIMIT",
    "LocalArtifactGarbageCollector",
    "MIN_ARTIFACT_GC_GRACE_SECONDS",
    "MAX_ARTIFACT_GC_LIMIT",
    "QuarantinedArtifact",
    "QuarantinedTemporaryArtifact",
]
