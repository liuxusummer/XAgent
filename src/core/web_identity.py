from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from src.core.agent_kernel import Principal, canonical_digest
from src.core.local_policy import HOST_READ_SCOPE, LOCAL_PRINCIPAL_SCOPES

WEB_IDENTITY_SCHEMA_VERSION = 1
MAX_OPAQUE_TOKEN_BYTES = 8192
MAX_WEB_IDENTITY_REGISTRY_BYTES = 4 * 1024 * 1024
MAX_WEB_IDENTITIES = 256
MAX_WEB_WORKSPACES = 1024
MAX_WEB_WORKSPACES_PER_IDENTITY = 32
_TOKEN_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_WORKSPACE_ALIAS_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,126}\.ws$")


def _path_identity(path: Path) -> tuple[int, int]:
    metadata = path.stat()
    return int(metadata.st_dev), int(metadata.st_ino)


def _path_ancestry_identities(path: Path) -> frozenset[tuple[int, int]]:
    return frozenset(
        _path_identity(ancestor)
        for ancestor in (path, *path.parents)
    )


def _directory_contains_path(directory: Path, candidate: Path) -> bool:
    """Compare physical ancestors instead of platform-dependent path spelling."""

    return _path_identity(directory) in _path_ancestry_identities(candidate)


def physical_directory_contains_path(
    directory: str | Path,
    candidate: str | Path,
) -> bool:
    """Return whether an existing path has the directory as a physical ancestor."""

    return _directory_contains_path(Path(directory), Path(candidate))


def _directories_physically_overlap(first: Path, second: Path) -> bool:
    return _directory_contains_path(first, second) or _directory_contains_path(
        second,
        first,
    )


class WebIdentityError(PermissionError):
    """The request did not carry a valid server-recognized Web identity."""


class WebIdentityConfigurationError(ValueError):
    """The trusted Web identity registry is invalid."""


@dataclass(frozen=True, slots=True)
class WebIdentity:
    """Authenticated Web identity with server-owned workspace mappings."""

    subject: str
    tenant_id: str
    scopes: tuple[str, ...]
    workspace_aliases: tuple[tuple[str, str], ...] = ()
    local_workspace_root: str = ""
    workspace_identities: tuple[tuple[str, int, int], ...] = field(
        default=(),
        init=False,
        repr=False,
    )

    def __post_init__(self) -> None:
        # Reuse the kernel's bounded identity validation without retaining a
        # credential or request object in Principal.
        validated = Principal(
            subject=self.subject,
            tenant_id=self.tenant_id,
            session_id="web-identity",
            run_id="web-identity",
            agent_id="main",
            scopes=self.scopes,
        )
        object.__setattr__(self, "subject", validated.subject)
        object.__setattr__(self, "tenant_id", validated.tenant_id)
        object.__setattr__(self, "scopes", validated.scopes)

        normalized: list[tuple[str, str]] = []
        identities: list[tuple[str, int, int]] = []
        seen: set[str] = set()
        for alias, raw_path in self.workspace_aliases:
            try:
                workspace_alias = _validate_workspace_alias(alias)
            except WebIdentityError as exc:
                raise WebIdentityConfigurationError(
                    "invalid workspace alias in Web identity registry"
                ) from exc
            if workspace_alias in seen:
                raise WebIdentityConfigurationError(
                    f"duplicate workspace alias: {workspace_alias}"
                )
            path = Path(str(raw_path)).expanduser()
            if not path.is_absolute():
                raise WebIdentityConfigurationError(
                    f"workspace path must be absolute: {workspace_alias}"
                )
            try:
                resolved = path.resolve(strict=True)
                metadata = resolved.stat()
            except OSError as exc:
                raise WebIdentityConfigurationError(
                    f"workspace path is unavailable: {workspace_alias}"
                ) from exc
            if not resolved.is_dir():
                raise WebIdentityConfigurationError(
                    f"workspace path must be a directory: {workspace_alias}"
                )
            normalized.append((workspace_alias, str(resolved)))
            identities.append(
                (
                    workspace_alias,
                    int(metadata.st_dev),
                    int(metadata.st_ino),
                )
            )
            seen.add(workspace_alias)
        object.__setattr__(
            self,
            "workspace_aliases",
            tuple(sorted(normalized)),
        )
        object.__setattr__(
            self,
            "workspace_identities",
            tuple(sorted(identities)),
        )

        if self.local_workspace_root:
            root = Path(self.local_workspace_root).expanduser()
            if not root.is_absolute():
                raise WebIdentityConfigurationError(
                    "local workspace root must be absolute"
                )
            object.__setattr__(
                self,
                "local_workspace_root",
                str(root.resolve(strict=False)),
            )

    @property
    def owner_digest(self) -> str:
        """Stable non-secret owner binding; never derived from the bearer token."""

        return canonical_digest(
            {
                "schema_version": WEB_IDENTITY_SCHEMA_VERSION,
                "subject": self.subject,
                "tenant_id": self.tenant_id,
                "scopes": list(self.scopes),
                "workspace_aliases": [
                    {"alias": alias, "path": path}
                    for alias, path in self.workspace_aliases
                ],
                "workspace_identities": [
                    {
                        "alias": alias,
                        "device": device,
                        "inode": inode,
                    }
                    for alias, device, inode in self.workspace_identities
                ],
                "local_workspace_root": self.local_workspace_root,
            },
            "WebIdentity owner",
        )

    def principal(
        self,
        *,
        session_id: str,
        run_id: str | None = None,
        agent_id: str = "main",
    ) -> Principal:
        return Principal(
            subject=self.subject,
            tenant_id=self.tenant_id,
            session_id=session_id,
            run_id=run_id or session_id,
            agent_id=agent_id,
            scopes=self.scopes,
        )

    def resolve_workspace(self, alias: str) -> str:
        workspace_alias = _validate_workspace_alias(alias)
        configured = dict(self.workspace_aliases)
        if workspace_alias in configured:
            configured_path = Path(configured[workspace_alias])
            try:
                resolved = configured_path.resolve(strict=True)
                metadata = resolved.stat()
            except OSError as exc:
                raise WebIdentityError("workspace is not authorized") from exc
            expected = {
                name: (device, inode)
                for name, device, inode in self.workspace_identities
            }.get(workspace_alias)
            if (
                not resolved.is_dir()
                or resolved != configured_path
                or expected
                != (int(metadata.st_dev), int(metadata.st_ino))
            ):
                raise WebIdentityError("workspace is not authorized")
            return str(resolved)
        if not self.local_workspace_root:
            raise WebIdentityError("workspace is not authorized")
        root = Path(self.local_workspace_root).resolve(strict=False)
        resolved = (root / workspace_alias).resolve(strict=False)
        try:
            resolved.relative_to(root)
        except ValueError as exc:
            raise WebIdentityError("workspace is not authorized") from exc
        return str(resolved)

    def workspace_boundaries_valid(self) -> bool:
        try:
            for alias, _path in self.workspace_aliases:
                self.resolve_workspace(alias)
        except WebIdentityError:
            return False
        return True


@dataclass(frozen=True, slots=True)
class _RegistryEntry:
    token_sha256: str
    identity: WebIdentity


class WebIdentityProvider:
    """Authenticate opaque tokens against server-owned identity records.

    Request headers are deliberately outside this API. A Web adapter may
    extract only the opaque credential and pass it to ``authenticate``;
    subject, tenant, scopes, and workspace paths always come from this
    provider's trusted configuration.
    """

    def __init__(
        self,
        *,
        local_identity: WebIdentity | None = None,
        registry: tuple[_RegistryEntry, ...] = (),
    ) -> None:
        if (local_identity is None) == (not registry):
            raise WebIdentityConfigurationError(
                "configure exactly one of local identity or secure registry"
            )
        self._local_identity = local_identity
        self._registry = tuple(registry)

    @classmethod
    def local(
        cls,
        workspace_root: str | Path,
        *,
        subject: str = "local-user",
        tenant_id: str = "local",
        scopes: tuple[str, ...] = LOCAL_PRINCIPAL_SCOPES,
    ) -> "WebIdentityProvider":
        root = Path(workspace_root).expanduser().resolve(strict=False)
        return cls(
            local_identity=WebIdentity(
                subject=subject,
                tenant_id=tenant_id,
                scopes=scopes,
                local_workspace_root=str(root),
            )
        )

    @classmethod
    def from_registry_file(cls, path: str | Path) -> "WebIdentityProvider":
        try:
            registry_path = Path(path).expanduser().resolve(strict=True)
        except OSError as exc:
            raise WebIdentityConfigurationError(
                "Web identity registry could not be loaded"
            ) from exc
        if not registry_path.is_file():
            raise WebIdentityConfigurationError(
                "Web identity registry must be a regular file"
            )
        try:
            with registry_path.open("rb") as handle:
                raw_payload = handle.read(MAX_WEB_IDENTITY_REGISTRY_BYTES + 1)
            if len(raw_payload) > MAX_WEB_IDENTITY_REGISTRY_BYTES:
                raise WebIdentityConfigurationError(
                    "Web identity registry exceeds its size limit"
                )
            payload = json.loads(raw_payload.decode("utf-8"))
        except WebIdentityConfigurationError:
            raise
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise WebIdentityConfigurationError(
                "Web identity registry could not be loaded"
            ) from exc
        provider = cls.from_trusted_config(payload)
        for identity in provider.identities():
            for _, workspace_path in identity.workspace_aliases:
                try:
                    registry_in_workspace = _directory_contains_path(
                        Path(workspace_path),
                        registry_path.parent,
                    )
                except OSError as exc:
                    raise WebIdentityConfigurationError(
                        "Web identity registry boundary could not be verified"
                    ) from exc
                if registry_in_workspace:
                    raise WebIdentityConfigurationError(
                        "Web identity registry must be outside Agent workspaces"
                    )
        return provider

    @classmethod
    def from_trusted_config(
        cls,
        payload: Mapping[str, Any],
    ) -> "WebIdentityProvider":
        if not isinstance(payload, Mapping):
            raise WebIdentityConfigurationError(
                "Web identity registry must be an object"
            )
        if set(payload) - {"schema_version", "identities"}:
            raise WebIdentityConfigurationError(
                "Web identity registry contains unknown fields"
            )
        if payload.get("schema_version") != WEB_IDENTITY_SCHEMA_VERSION:
            raise WebIdentityConfigurationError(
                "unsupported Web identity registry schema"
            )
        records = payload.get("identities")
        if (
            not isinstance(records, list)
            or not records
            or len(records) > MAX_WEB_IDENTITIES
        ):
            raise WebIdentityConfigurationError(
                "secure Web identity registry has an invalid identity count"
            )

        entries: list[_RegistryEntry] = []
        seen_digests: set[str] = set()
        workspace_owners: list[
            tuple[tuple[int, int], frozenset[tuple[int, int]], str]
        ] = []
        workspace_count = 0
        for raw_record in records:
            if not isinstance(raw_record, Mapping):
                raise WebIdentityConfigurationError(
                    "Web identity registry entry must be an object"
                )
            allowed_fields = {
                "token_sha256",
                "subject",
                "tenant_id",
                "scopes",
                "workspaces",
                "enabled",
            }
            if set(raw_record) - allowed_fields:
                raise WebIdentityConfigurationError(
                    "Web identity registry entry contains unknown fields"
                )
            enabled = raw_record.get("enabled", True)
            if not isinstance(enabled, bool):
                raise WebIdentityConfigurationError(
                    "Web identity enabled flag must be a boolean"
                )
            if not enabled:
                continue
            token_digest = str(raw_record.get("token_sha256") or "")
            if not _TOKEN_DIGEST_RE.fullmatch(token_digest):
                raise WebIdentityConfigurationError(
                    "token_sha256 must be a lowercase SHA-256 digest"
                )
            if token_digest in seen_digests:
                raise WebIdentityConfigurationError(
                    "duplicate token_sha256 in Web identity registry"
                )
            scopes = raw_record.get("scopes")
            if (
                not isinstance(scopes, list)
                or not all(isinstance(scope, str) for scope in scopes)
            ):
                raise WebIdentityConfigurationError(
                    "Web identity scopes must be a list of strings"
                )
            workspaces = raw_record.get("workspaces")
            if (
                not isinstance(workspaces, Mapping)
                or not workspaces
                or len(workspaces) > MAX_WEB_WORKSPACES_PER_IDENTITY
                or not all(
                    isinstance(alias, str) and isinstance(workspace_path, str)
                    for alias, workspace_path in workspaces.items()
                )
            ):
                raise WebIdentityConfigurationError(
                    "secure Web identity workspaces must map aliases to paths"
                )
            workspace_count += len(workspaces)
            if workspace_count > MAX_WEB_WORKSPACES:
                raise WebIdentityConfigurationError(
                    "Web identity registry has too many workspace mappings"
                )
            subject = raw_record.get("subject")
            tenant_id = raw_record.get("tenant_id")
            if not isinstance(subject, str) or not isinstance(tenant_id, str):
                raise WebIdentityConfigurationError(
                    "Web identity subject and tenant_id must be strings"
                )
            identity = WebIdentity(
                subject=subject,
                tenant_id=tenant_id,
                scopes=tuple(scopes),
                workspace_aliases=tuple(
                    (alias, workspace_path)
                    for alias, workspace_path in workspaces.items()
                ),
            )
            if HOST_READ_SCOPE in identity.scopes:
                raise WebIdentityConfigurationError(
                    "host.read is reserved for local operator sessions"
                )
            for _, workspace_path in identity.workspace_aliases:
                workspace = Path(workspace_path)
                try:
                    workspace_identity = _path_identity(workspace)
                    workspace_ancestry = _path_ancestry_identities(workspace)
                except OSError as exc:
                    raise WebIdentityConfigurationError(
                        "workspace boundary could not be verified"
                    ) from exc
                for (
                    existing_identity,
                    existing_ancestry,
                    existing_owner,
                ) in workspace_owners:
                    if existing_owner == identity.owner_digest:
                        continue
                    if (
                        existing_identity in workspace_ancestry
                        or workspace_identity in existing_ancestry
                    ):
                        raise WebIdentityConfigurationError(
                            "workspace paths cannot overlap across identities"
                        )
                workspace_owners.append(
                    (
                        workspace_identity,
                        workspace_ancestry,
                        identity.owner_digest,
                    )
                )
            entries.append(
                _RegistryEntry(
                    token_sha256=token_digest,
                    identity=identity,
                )
            )
            seen_digests.add(token_digest)
        if not entries:
            raise WebIdentityConfigurationError(
                "secure Web identity registry has no enabled identities"
            )
        return cls(registry=tuple(entries))

    @property
    def is_local(self) -> bool:
        return self._local_identity is not None

    def identities(self) -> tuple[WebIdentity, ...]:
        """Return unique server-owned identities for trusted schedulers."""

        if self._local_identity is not None:
            return (self._local_identity,)
        by_owner = {entry.identity.owner_digest: entry.identity for entry in self._registry}
        return tuple(by_owner[key] for key in sorted(by_owner))

    def resolve_owner(self, owner_digest: str) -> WebIdentity:
        matched: WebIdentity | None = None
        for identity in self.identities():
            if hmac.compare_digest(identity.owner_digest, str(owner_digest or "")):
                matched = identity
        if matched is None:
            raise WebIdentityError("identity owner is unknown or revoked")
        if not matched.workspace_boundaries_valid():
            raise WebIdentityError("identity workspace boundary is unavailable")
        return matched

    def authenticate(self, opaque_token: str | None = None) -> WebIdentity:
        if self._local_identity is not None:
            return self._local_identity
        if not isinstance(opaque_token, str) or not opaque_token:
            raise WebIdentityError("authentication is required")
        encoded = opaque_token.encode("utf-8")
        if len(encoded) > MAX_OPAQUE_TOKEN_BYTES:
            raise WebIdentityError("invalid authentication credential")
        presented_digest = hashlib.sha256(encoded).hexdigest()

        matched: WebIdentity | None = None
        # Always scan the full registry. The stored digest, not user-provided
        # identity claims, selects the server-owned record.
        for entry in self._registry:
            if hmac.compare_digest(entry.token_sha256, presented_digest):
                matched = entry.identity
        if matched is None:
            raise WebIdentityError("invalid authentication credential")
        if not matched.workspace_boundaries_valid():
            raise WebIdentityError("identity workspace boundary is unavailable")
        return matched


def _validate_workspace_alias(alias: str) -> str:
    value = str(alias or "").strip()
    if (
        not _WORKSPACE_ALIAS_RE.fullmatch(value)
        or value in {".ws", "..ws"}
        or ".." in value.split(".")
    ):
        raise WebIdentityError("invalid workspace alias")
    return value


__all__ = [
    "MAX_OPAQUE_TOKEN_BYTES",
    "MAX_WEB_IDENTITIES",
    "MAX_WEB_IDENTITY_REGISTRY_BYTES",
    "MAX_WEB_WORKSPACES",
    "MAX_WEB_WORKSPACES_PER_IDENTITY",
    "WEB_IDENTITY_SCHEMA_VERSION",
    "WebIdentity",
    "WebIdentityConfigurationError",
    "WebIdentityError",
    "WebIdentityProvider",
]
