"""Local, content-addressed storage for durable orchestration artifacts.

The event store only keeps :class:`ArtifactRef` metadata.  Artifact bytes are
installed before an event may reference them, using a temporary file in the
destination directory followed by an atomic no-clobber link.  The local store
does not encrypt content; deployments that need encryption must provide a
different ``ArtifactStore`` implementation and report that fact in the
reference metadata.
"""

from __future__ import annotations

import errno
import hashlib
import io
import json
import math
import os
import stat
import tempfile
import time
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Any, BinaryIO, Callable, Mapping, Protocol, runtime_checkable

from .metadata_security import contains_sensitive_key

ARTIFACT_SCHEMA_VERSION = 1
MAX_ARTIFACT_METADATA_BYTES = 8 * 1024
MAX_METADATA_KEY_CHARS = 128
MAX_TEXT_FIELD_CHARS = 512
_COPY_CHUNK_SIZE = 1024 * 1024
_SHA256_HEX_LENGTH = 64

JsonValue = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]
FaultHook = Callable[[str, Path], None]


class ArtifactError(RuntimeError):
    """Base error for artifact persistence."""


class ArtifactValidationError(ArtifactError, ValueError):
    """Artifact metadata is malformed or exceeds its durable boundary."""


class ArtifactIntegrityError(ArtifactError):
    """Stored bytes do not match the immutable artifact reference."""


class ArtifactWriteError(ArtifactError):
    """Artifact bytes could not be durably and atomically installed."""


class ArtifactKind(StrEnum):
    AGENT_REQUEST = "agent_request"
    AGENT_EXECUTION_MANIFEST = "agent_execution_manifest"
    AGENT_TURN_CHECKPOINT = "agent_turn_checkpoint"
    MODEL_RESPONSE = "model_response"
    TOOL_RECEIPT = "tool_receipt"
    TOOL_RESULT = "tool_result"
    FILE_SNAPSHOT = "file_snapshot"
    REPORT = "report"
    LOG = "log"
    GENERIC = "generic"


class ArtifactSensitivity(StrEnum):
    PUBLIC = "public"
    INTERNAL = "internal"
    SENSITIVE = "sensitive"
    SECRET = "secret"


class ArtifactEncryption(StrEnum):
    NONE = "none"
    DEPLOYMENT_MANAGED = "deployment_managed"


def _validate_digest(value: Any) -> str:
    digest = str(value or "")
    if len(digest) != _SHA256_HEX_LENGTH:
        raise ArtifactValidationError("sha256 must contain exactly 64 lowercase hex characters")
    if digest != digest.lower() or any(character not in "0123456789abcdef" for character in digest):
        raise ArtifactValidationError("sha256 must contain exactly 64 lowercase hex characters")
    return digest


def _bounded_text(
    value: Any,
    field_name: str,
    *,
    required: bool = True,
    max_chars: int = MAX_TEXT_FIELD_CHARS,
) -> str | None:
    if value is None and not required:
        return None
    if not isinstance(value, str):
        raise ArtifactValidationError(f"{field_name} must be a string")
    text = value.strip()
    if required and not text:
        raise ArtifactValidationError(f"{field_name} must not be empty")
    if len(text) > max_chars:
        raise ArtifactValidationError(f"{field_name} exceeds the {max_chars}-character limit")
    if any(ord(character) < 32 or ord(character) == 127 for character in text):
        raise ArtifactValidationError(f"{field_name} must not contain control characters")
    return text


def _canonical_metadata(value: Any) -> tuple[Mapping[str, JsonValue], bytes]:
    if value is None:
        value = {}
    if not isinstance(value, Mapping):
        raise ArtifactValidationError("metadata must be a JSON object")
    detached: dict[str, Any] = {}
    for key, item in value.items():
        if not isinstance(key, str) or not key:
            raise ArtifactValidationError("metadata keys must be non-empty strings")
        if len(key) > MAX_METADATA_KEY_CHARS:
            raise ArtifactValidationError(
                f"metadata key exceeds the {MAX_METADATA_KEY_CHARS}-character limit"
            )
        detached[key] = item
    try:
        encoded = json.dumps(
            detached,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise ArtifactValidationError("metadata must be JSON serializable") from exc
    if len(encoded) > MAX_ARTIFACT_METADATA_BYTES:
        raise ArtifactValidationError(
            f"metadata exceeds the {MAX_ARTIFACT_METADATA_BYTES}-byte limit"
        )
    normalized = json.loads(encoded.decode("utf-8"))
    if contains_sensitive_key(normalized):
        raise ArtifactValidationError(
            "metadata contains a forbidden sensitive key"
        )
    return MappingProxyType(normalized), encoded


def _artifact_uri(digest: str) -> str:
    return f"sha256/{digest[:2]}/{digest[2:4]}/{digest}"


def _relative_uri(value: Any) -> str:
    uri = _bounded_text(value, "uri", max_chars=1024)
    assert uri is not None
    if "\\" in uri or uri.startswith("/"):
        raise ArtifactValidationError("uri must be a normalized relative URI")
    parts = uri.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise ArtifactValidationError("uri must be a normalized relative URI")
    return uri


@dataclass(frozen=True, slots=True)
class ArtifactRef:
    """Bounded, versioned metadata for immutable artifact content.

    ``metadata`` defaults to an empty object so credentials or raw payloads are
    never captured implicitly.  Callers are responsible for only supplying
    explicitly reviewed, non-secret metadata.
    """

    artifact_id: str
    sha256: str
    size: int
    media_type: str = "application/octet-stream"
    kind: ArtifactKind = ArtifactKind.GENERIC
    uri: str = ""
    sensitivity: ArtifactSensitivity = ArtifactSensitivity.INTERNAL
    encryption: ArtifactEncryption = ArtifactEncryption.NONE
    producer_run_id: str | None = None
    producer_node_id: str | None = None
    producer_attempt_id: str | None = None
    encryption_key_ref: str | None = None
    metadata: Mapping[str, JsonValue] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    schema_version: int = ARTIFACT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        digest = _validate_digest(self.sha256)
        object.__setattr__(self, "sha256", digest)
        object.__setattr__(
            self,
            "artifact_id",
            _bounded_text(self.artifact_id, "artifact_id", max_chars=255),
        )
        object.__setattr__(self, "uri", _relative_uri(self.uri))

        if isinstance(self.size, bool) or not isinstance(self.size, int) or self.size < 0:
            raise ArtifactValidationError("size must be a non-negative integer")
        object.__setattr__(
            self,
            "media_type",
            _bounded_text(self.media_type, "media_type", max_chars=255),
        )
        try:
            object.__setattr__(self, "kind", ArtifactKind(self.kind))
        except ValueError as exc:
            raise ArtifactValidationError(f"invalid artifact kind: {self.kind}") from exc
        try:
            object.__setattr__(self, "sensitivity", ArtifactSensitivity(self.sensitivity))
        except ValueError as exc:
            raise ArtifactValidationError(
                f"invalid artifact sensitivity: {self.sensitivity}"
            ) from exc
        try:
            object.__setattr__(self, "encryption", ArtifactEncryption(self.encryption))
        except ValueError as exc:
            raise ArtifactValidationError(
                f"invalid artifact encryption: {self.encryption}"
            ) from exc

        for field_name in (
            "producer_run_id",
            "producer_node_id",
            "producer_attempt_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _bounded_text(getattr(self, field_name), field_name, required=False),
            )
        key_ref = _bounded_text(
            self.encryption_key_ref,
            "encryption_key_ref",
            required=False,
        )
        if self.encryption is ArtifactEncryption.NONE and key_ref is not None:
            raise ArtifactValidationError(
                "encryption_key_ref is forbidden when encryption is none"
            )
        if self.encryption is ArtifactEncryption.DEPLOYMENT_MANAGED and key_ref is None:
            raise ArtifactValidationError(
                "encryption_key_ref is required for deployment-managed encryption"
            )
        object.__setattr__(self, "encryption_key_ref", key_ref)

        metadata, _ = _canonical_metadata(self.metadata)
        object.__setattr__(self, "metadata", metadata)
        try:
            created_at = float(self.created_at)
        except (TypeError, ValueError) as exc:
            raise ArtifactValidationError("created_at must be a finite timestamp") from exc
        if not math.isfinite(created_at) or created_at < 0:
            raise ArtifactValidationError("created_at must be a finite timestamp")
        object.__setattr__(self, "created_at", created_at)
        if self.schema_version != ARTIFACT_SCHEMA_VERSION:
            raise ArtifactValidationError(
                f"unsupported artifact schema version: {self.schema_version}"
            )

    def to_dict(self) -> dict[str, Any]:
        _, encoded_metadata = _canonical_metadata(self.metadata)
        return {
            "schema_version": self.schema_version,
            "artifact_id": self.artifact_id,
            "sha256": self.sha256,
            "size": self.size,
            "media_type": self.media_type,
            "kind": self.kind.value,
            "uri": self.uri,
            "sensitivity": self.sensitivity.value,
            "encryption": self.encryption.value,
            "producer_run_id": self.producer_run_id,
            "producer_node_id": self.producer_node_id,
            "producer_attempt_id": self.producer_attempt_id,
            "encryption_key_ref": self.encryption_key_ref,
            "metadata": json.loads(encoded_metadata.decode("utf-8")),
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "ArtifactRef":
        if not isinstance(payload, Mapping):
            raise ArtifactValidationError("artifact reference must be an object")
        return cls(
            schema_version=payload.get("schema_version", ARTIFACT_SCHEMA_VERSION),
            artifact_id=payload.get("artifact_id", ""),
            sha256=payload.get("sha256", ""),
            size=payload.get("size", -1),
            media_type=payload.get("media_type", "application/octet-stream"),
            kind=payload.get("kind", ArtifactKind.GENERIC),
            uri=payload.get("uri", ""),
            sensitivity=payload.get("sensitivity", ArtifactSensitivity.INTERNAL),
            encryption=payload.get("encryption", ArtifactEncryption.NONE),
            producer_run_id=payload.get("producer_run_id"),
            producer_node_id=payload.get("producer_node_id"),
            producer_attempt_id=payload.get("producer_attempt_id"),
            encryption_key_ref=payload.get("encryption_key_ref"),
            metadata=payload.get("metadata", {}),
            created_at=payload.get("created_at", 0),
        )


def _local_artifact_id(ref: ArtifactRef) -> str:
    identity = ref.to_dict()
    identity.pop("artifact_id")
    identity.pop("created_at")
    encoded = json.dumps(
        identity,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return f"artifact_{hashlib.sha256(encoded).hexdigest()}"


@runtime_checkable
class ArtifactStore(Protocol):
    """Capability boundary for artifact persistence implementations."""

    def put_bytes(self, content: bytes, **metadata: Any) -> ArtifactRef: ...

    def put_json(self, value: Any, **metadata: Any) -> ArtifactRef: ...

    def open(self, ref: ArtifactRef) -> BinaryIO: ...

    def read(self, ref: ArtifactRef) -> bytes: ...

    def verify(self, ref: ArtifactRef) -> bool: ...

    def exists(self, ref: ArtifactRef) -> bool: ...


class LocalArtifactStore:
    """Stdlib-only immutable artifact store rooted in one runtime directory."""

    def __init__(self, root: str | os.PathLike[str], *, fault_hook: FaultHook | None = None):
        root_path = Path(root).expanduser().resolve()
        root_path.mkdir(mode=0o700, parents=True, exist_ok=True)
        if not root_path.is_dir():
            raise ArtifactValidationError("artifact root must be a directory")
        self.root = root_path
        self._fault_hook = fault_hook

    def put_bytes(
        self,
        content: bytes,
        *,
        media_type: str = "application/octet-stream",
        kind: ArtifactKind | str = ArtifactKind.GENERIC,
        sensitivity: ArtifactSensitivity | str = ArtifactSensitivity.INTERNAL,
        producer_run_id: str | None = None,
        producer_node_id: str | None = None,
        producer_attempt_id: str | None = None,
        metadata: Mapping[str, JsonValue] | None = None,
    ) -> ArtifactRef:
        if not isinstance(content, bytes):
            raise ArtifactValidationError("content must be bytes")
        digest = hashlib.sha256(content).hexdigest()
        path = self._path_for_digest(digest)
        self._ensure_parent(path.parent)

        # Validate all reference metadata before performing filesystem writes.
        ref_values = {
            "sha256": digest,
            "size": len(content),
            "media_type": media_type,
            "kind": kind,
            "uri": _artifact_uri(digest),
            "sensitivity": sensitivity,
            "encryption": ArtifactEncryption.NONE,
            "producer_run_id": producer_run_id,
            "producer_node_id": producer_node_id,
            "producer_attempt_id": producer_attempt_id,
            "metadata": {} if metadata is None else metadata,
        }
        provisional_ref = ArtifactRef(artifact_id="pending", created_at=0, **ref_values)
        ref_values["artifact_id"] = _local_artifact_id(provisional_ref)

        try:
            existing = path.lstat()
        except FileNotFoundError:
            existing = None
        if existing is not None:
            if not stat.S_ISREG(existing.st_mode):
                raise ArtifactIntegrityError("artifact path is not a regular file")
            self._verify_path(path, digest, len(content))
            return ArtifactRef(created_at=existing.st_mtime, **ref_values)

        file_descriptor = -1
        temporary_path: Path | None = None
        installed = False
        try:
            file_descriptor, temporary_name = tempfile.mkstemp(
                prefix=f".{digest}.",
                suffix=".tmp",
                dir=path.parent,
            )
            temporary_path = Path(temporary_name)
            os.chmod(temporary_path, 0o600)
            with os.fdopen(file_descriptor, "wb", closefd=True) as output:
                file_descriptor = -1
                output.write(content)
                output.flush()
                os.fsync(output.fileno())
            self._fault("after_temp_fsync", temporary_path)
            # Preserve the historical fault-stage names while installing with
            # no-clobber semantics.  Replacing an existing content-addressed
            # path lets concurrent writers change its mtime, so equal writes
            # can otherwise return unequal ArtifactRefs.
            self._fault("before_replace", path)
            try:
                os.link(temporary_path, path)
                installed = True
            except FileExistsError:
                installed = False
            if installed:
                temporary_path.unlink()
                temporary_path = None
            self._fault("after_replace", path)
            self._fsync_directory(path.parent)
            self._fault("after_directory_fsync", path)
            self._verify_path(path, digest, len(content))
            final_stat = path.stat()
            return ArtifactRef(created_at=final_stat.st_mtime, **ref_values)
        except ArtifactError:
            raise
        except Exception as exc:
            state = "installed completely" if installed else "not installed"
            raise ArtifactWriteError(f"artifact write failed; content was {state}") from exc
        finally:
            if file_descriptor >= 0:
                os.close(file_descriptor)
            if temporary_path is not None:
                try:
                    temporary_path.unlink()
                except FileNotFoundError:
                    pass

    def put_json(
        self,
        value: Any,
        *,
        media_type: str = "application/json",
        kind: ArtifactKind | str = ArtifactKind.GENERIC,
        sensitivity: ArtifactSensitivity | str = ArtifactSensitivity.INTERNAL,
        producer_run_id: str | None = None,
        producer_node_id: str | None = None,
        producer_attempt_id: str | None = None,
        metadata: Mapping[str, JsonValue] | None = None,
    ) -> ArtifactRef:
        try:
            content = json.dumps(
                value,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        except (TypeError, ValueError, OverflowError, RecursionError) as exc:
            raise ArtifactValidationError("JSON artifact must be serializable") from exc
        return self.put_bytes(
            content,
            media_type=media_type,
            kind=kind,
            sensitivity=sensitivity,
            producer_run_id=producer_run_id,
            producer_node_id=producer_node_id,
            producer_attempt_id=producer_attempt_id,
            metadata=metadata,
        )

    def open(self, ref: ArtifactRef) -> BinaryIO:
        validated_ref = self._validated_ref(ref)
        path = self._path_for_digest(validated_ref.sha256)
        flags = os.O_RDONLY
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            file_descriptor = os.open(path, flags)
        except FileNotFoundError as exc:
            raise ArtifactIntegrityError("artifact content is missing") from exc
        except OSError as exc:
            raise ArtifactIntegrityError("artifact content cannot be opened safely") from exc
        try:
            self._verify_descriptor(
                file_descriptor,
                validated_ref.sha256,
                validated_ref.size,
            )
            os.lseek(file_descriptor, 0, os.SEEK_SET)
            return os.fdopen(file_descriptor, "rb", closefd=True)
        except Exception:
            os.close(file_descriptor)
            raise

    def read(self, ref: ArtifactRef) -> bytes:
        with self.open(ref) as artifact:
            return artifact.read()

    def read_bytes(self, ref: ArtifactRef) -> bytes:
        return self.read(ref)

    def verify(self, ref: ArtifactRef) -> bool:
        with self.open(ref):
            return True

    def exists(self, ref: ArtifactRef) -> bool:
        validated_ref = self._validated_ref(ref)
        path = self._path_for_digest(validated_ref.sha256)
        try:
            stored = path.lstat()
        except FileNotFoundError:
            return False
        if not stat.S_ISREG(stored.st_mode):
            raise ArtifactIntegrityError("artifact path is not a regular file")
        return True

    def _validated_ref(self, ref: ArtifactRef) -> ArtifactRef:
        if not isinstance(ref, ArtifactRef):
            raise ArtifactValidationError("artifact reference must be an ArtifactRef")
        # Reconstruct to defend against references mutated through object.__setattr__.
        validated_ref = ArtifactRef.from_dict(ref.to_dict())
        if validated_ref.uri != _artifact_uri(validated_ref.sha256):
            raise ArtifactValidationError("uri is not managed by this local artifact store")
        if validated_ref.encryption is not ArtifactEncryption.NONE:
            raise ArtifactValidationError("local artifact store cannot read encrypted content")
        if validated_ref.artifact_id != _local_artifact_id(validated_ref):
            raise ArtifactValidationError("artifact_id does not match the local reference metadata")
        return validated_ref

    def _path_for_digest(self, digest: str) -> Path:
        valid_digest = _validate_digest(digest)
        parent = self.root / "sha256" / valid_digest[:2] / valid_digest[2:4]
        resolved_parent = parent.resolve(strict=False)
        try:
            resolved_parent.relative_to(self.root)
        except ValueError as exc:
            raise ArtifactValidationError("artifact path escapes the store root") from exc
        return parent / valid_digest

    def _ensure_parent(self, parent: Path) -> None:
        try:
            relative_parent = parent.relative_to(self.root)
        except ValueError as exc:
            raise ArtifactValidationError("artifact path escapes the store root") from exc
        if any(part in {"", ".", ".."} for part in relative_parent.parts):
            raise ArtifactValidationError("artifact parent is not a managed relative path")

        flags = os.O_RDONLY
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        if hasattr(os, "O_DIRECTORY"):
            flags |= os.O_DIRECTORY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            directory_fd = os.open(self.root, flags)
        except OSError as exc:
            raise ArtifactValidationError("artifact root cannot be opened safely") from exc
        try:
            for component in relative_parent.parts:
                try:
                    os.mkdir(component, mode=0o700, dir_fd=directory_fd)
                except FileExistsError:
                    pass
                try:
                    stored = os.stat(
                        component,
                        dir_fd=directory_fd,
                        follow_symlinks=False,
                    )
                except OSError as exc:
                    raise ArtifactValidationError(
                        "artifact parent cannot be inspected safely"
                    ) from exc
                if stat.S_ISLNK(stored.st_mode) or not stat.S_ISDIR(stored.st_mode):
                    raise ArtifactValidationError(
                        "artifact parent contains a symlink or non-directory"
                    )
                try:
                    next_fd = os.open(component, flags, dir_fd=directory_fd)
                except OSError as exc:
                    raise ArtifactValidationError(
                        "artifact parent cannot be opened safely"
                    ) from exc
                os.close(directory_fd)
                directory_fd = next_fd
        finally:
            os.close(directory_fd)

    def _verify_path(self, path: Path, digest: str, expected_size: int) -> None:
        flags = os.O_RDONLY
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            file_descriptor = os.open(path, flags)
        except OSError as exc:
            raise ArtifactIntegrityError("artifact content cannot be opened safely") from exc
        try:
            self._verify_descriptor(file_descriptor, digest, expected_size)
        finally:
            os.close(file_descriptor)

    @staticmethod
    def _verify_descriptor(file_descriptor: int, digest: str, expected_size: int) -> None:
        stored = os.fstat(file_descriptor)
        if not stat.S_ISREG(stored.st_mode):
            raise ArtifactIntegrityError("artifact content is not a regular file")
        if stored.st_size != expected_size:
            raise ArtifactIntegrityError(
                f"artifact size mismatch: expected {expected_size}, got {stored.st_size}"
            )
        hasher = hashlib.sha256()
        os.lseek(file_descriptor, 0, os.SEEK_SET)
        while True:
            chunk = os.read(file_descriptor, _COPY_CHUNK_SIZE)
            if not chunk:
                break
            hasher.update(chunk)
        actual_digest = hasher.hexdigest()
        if actual_digest != digest:
            raise ArtifactIntegrityError(
                f"artifact digest mismatch: expected {digest}, got {actual_digest}"
            )

    def _fault(self, stage: str, path: Path) -> None:
        if self._fault_hook is not None:
            self._fault_hook(stage, path)

    @staticmethod
    def _fsync_directory(directory: Path) -> None:
        flags = os.O_RDONLY
        if hasattr(os, "O_DIRECTORY"):
            flags |= os.O_DIRECTORY
        try:
            file_descriptor = os.open(directory, flags)
        except OSError as exc:
            if exc.errno in {errno.EINVAL, errno.ENOTSUP, errno.EOPNOTSUPP}:
                return
            raise
        try:
            os.fsync(file_descriptor)
        except OSError as exc:
            if exc.errno not in {errno.EINVAL, errno.ENOTSUP, errno.EOPNOTSUPP}:
                raise
        finally:
            os.close(file_descriptor)


@dataclass(frozen=True, slots=True)
class JsonArtifactResultWriter:
    """Persist a normalized legacy result without copying task data into metadata."""

    store: LocalArtifactStore
    producer_run_id: str
    producer_node_id: str
    producer_attempt_id: str

    def __post_init__(self) -> None:
        if not isinstance(self.store, LocalArtifactStore):
            raise ArtifactValidationError("store must be a LocalArtifactStore")
        for field_name in (
            "producer_run_id",
            "producer_node_id",
            "producer_attempt_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _bounded_text(getattr(self, field_name), field_name),
            )

    def __call__(self, normalized_result: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(normalized_result, Mapping):
            raise ArtifactValidationError("normalized result must be a JSON object")
        ref = self.store.put_json(
            dict(normalized_result),
            kind=ArtifactKind.MODEL_RESPONSE,
            sensitivity=ArtifactSensitivity.SENSITIVE,
            producer_run_id=self.producer_run_id,
            producer_node_id=self.producer_node_id,
            producer_attempt_id=self.producer_attempt_id,
            metadata={},
        )
        return ref.to_dict()


def canonical_json_bytes(value: Any) -> bytes:
    """Return the exact canonical JSON representation used by ``put_json``."""

    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise ArtifactValidationError("JSON artifact must be serializable") from exc
