from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any


SCENARIO_PACK_SCHEMA_VERSION = 1
MAX_MANIFEST_BYTES = 64 * 1024
MAX_FIXTURE_FILES = 1_000
MAX_FIXTURE_BYTES = 20 * 1024 * 1024
MAX_LIST_ITEMS = 64
MAX_TEXT_CHARS = 120
SCENARIO_PACK_V1_SCOPES = frozenset(
    {
        "workspace.read",
        "workspace.write",
        "workspace.delete",
    }
)
SCENARIO_PACK_V1_TOOLS = frozenset(
    {
        "file_read",
        "file_search",
        "file_write",
        "file_patch",
        "file_delete",
    }
)
SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$")
SEMVER_RE = re.compile(
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$"
)


class ScenarioPackError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class ScenarioPack:
    pack_id: str
    name: str
    version: str
    pack_dir: Path
    dataset_file: str
    fixtures_dir: str
    default_scopes: tuple[str, ...]
    tools_allowlist: tuple[str, ...]
    skill_allowlist: tuple[str, ...]
    memory_mode: str
    max_turns: int
    pack_digest: str
    dataset_sha256: str
    dataset_bytes: int
    fixture_files: int
    fixture_bytes: int
    fixture_inventory: tuple[tuple[str, str, int], ...]

    def public_metadata(self, workspace_root: str | Path) -> dict[str, Any]:
        workspace = Path(workspace_root).resolve()
        try:
            source_dir = self.pack_dir.relative_to(workspace).as_posix()
        except ValueError as exc:
            raise ScenarioPackError("scenario pack is outside the workspace") from exc
        return {
            "schema_version": SCENARIO_PACK_SCHEMA_VERSION,
            "id": self.pack_id,
            "name": self.name,
            "version": self.version,
            "source_dir": source_dir,
            "dataset_file": self.dataset_file,
            "pack_digest": self.pack_digest,
            "fixture_files": self.fixture_files,
            "fixture_bytes": self.fixture_bytes,
            "isolation": "case_workspace",
        }


@dataclass(frozen=True, slots=True)
class EvalCaseRuntime:
    workspace_root: str
    sandbox_parent: str
    scopes: tuple[str, ...]
    tools_allowlist: tuple[str, ...]
    skill_allowlist: tuple[str, ...]
    memory_mode: str
    max_turns: int
    pack_id: str
    pack_version: str
    pack_digest: str


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ScenarioPackError(f"duplicate manifest key: {key}")
        value[key] = item
    return value


def _reject_constant(value: str) -> None:
    raise ScenarioPackError(f"non-standard JSON number is not allowed: {value}")


def _bounded_text(value: Any, field: str, *, limit: int = MAX_TEXT_CHARS) -> str:
    if not isinstance(value, str):
        raise ScenarioPackError(f"{field} must be a string")
    text = value.strip()
    if not text:
        raise ScenarioPackError(f"{field} must not be empty")
    if len(text) > limit:
        raise ScenarioPackError(f"{field} exceeds {limit} characters")
    if any(ord(character) < 32 or ord(character) == 127 for character in text):
        raise ScenarioPackError(f"{field} contains control characters")
    return text


def _safe_id(value: Any, field: str) -> str:
    text = _bounded_text(value, field, limit=80)
    if not SAFE_ID_RE.fullmatch(text) or text in {".", ".."}:
        raise ScenarioPackError(f"{field} is not a safe id")
    return text


def _bounded_string_list(
    value: Any,
    field: str,
    *,
    safe_ids: bool = False,
) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ScenarioPackError(f"{field} must be an array")
    if len(value) > MAX_LIST_ITEMS:
        raise ScenarioPackError(f"{field} exceeds {MAX_LIST_ITEMS} items")
    normalized = tuple(
        _safe_id(item, field)
        if safe_ids
        else _bounded_text(item, field, limit=80)
        for item in value
    )
    if len(set(normalized)) != len(normalized):
        raise ScenarioPackError(f"{field} values must be unique")
    return normalized


def _relative_component(value: Any, field: str) -> str:
    text = _safe_id(value, field)
    if "/" in text or "\\" in text:
        raise ScenarioPackError(f"{field} must be one relative path component")
    return text


def _read_manifest(path: Path) -> dict[str, Any]:
    try:
        with path.open("rb") as file:
            encoded = file.read(MAX_MANIFEST_BYTES + 1)
        if len(encoded) > MAX_MANIFEST_BYTES:
            raise ScenarioPackError("scenario pack manifest exceeds 64KB")
        value = json.loads(
            encoded.decode("utf-8"),
            object_pairs_hook=_strict_object,
            parse_constant=_reject_constant,
        )
    except ScenarioPackError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError, RecursionError) as exc:
        raise ScenarioPackError(
            f"failed to read scenario pack manifest: {type(exc).__name__}"
        ) from exc
    if not isinstance(value, dict):
        raise ScenarioPackError("scenario pack manifest must be an object")
    return value


def _file_sha256(path: Path, *, size_limit: int) -> tuple[str, int]:
    try:
        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        file_descriptor = os.open(path, flags)
        total = 0
        digest = hashlib.sha256()
        with os.fdopen(file_descriptor, "rb") as file:
            while True:
                chunk = file.read(64 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > size_limit:
                    raise ScenarioPackError("scenario pack content exceeds size limit")
                digest.update(chunk)
    except ScenarioPackError:
        raise
    except OSError as exc:
        raise ScenarioPackError(
            f"failed to read scenario pack file: {type(exc).__name__}"
        ) from exc
    return digest.hexdigest(), total


def _fixture_inventory(fixtures_root: Path) -> tuple[list[dict[str, Any]], int]:
    if fixtures_root.is_symlink() or not fixtures_root.is_dir():
        raise ScenarioPackError("scenario pack fixtures directory is missing")
    inventory: list[dict[str, Any]] = []
    total_bytes = 0
    for path in sorted(fixtures_root.rglob("*")):
        if path.is_symlink():
            raise ScenarioPackError("scenario pack fixtures cannot contain symbolic links")
        if path.is_dir():
            continue
        if not path.is_file():
            raise ScenarioPackError("scenario pack fixtures must be regular files")
        if len(inventory) >= MAX_FIXTURE_FILES:
            raise ScenarioPackError(
                f"scenario pack exceeds {MAX_FIXTURE_FILES} fixture files"
            )
        digest, size = _file_sha256(
            path,
            size_limit=MAX_FIXTURE_BYTES - total_bytes,
        )
        total_bytes += size
        inventory.append(
            {
                "path": path.relative_to(fixtures_root).as_posix(),
                "sha256": digest,
                "size_bytes": size,
            }
        )
    return inventory, total_bytes


def load_scenario_pack_for_dataset(
    workspace_root: str | Path,
    dataset_path: str | Path,
) -> ScenarioPack | None:
    workspace = Path(workspace_root).resolve()
    dataset = Path(dataset_path).resolve()
    manifest_path = dataset.parent / "pack.json"
    if not manifest_path.is_file():
        return None
    if manifest_path.is_symlink():
        raise ScenarioPackError("scenario pack manifest cannot be a symbolic link")
    try:
        dataset.parent.relative_to((workspace / "system" / "eval").resolve())
    except ValueError as exc:
        raise ScenarioPackError(
            "scenario packs must live under workspace/system/eval"
        ) from exc

    manifest = _read_manifest(manifest_path)
    allowed_fields = {
        "schema_version",
        "id",
        "name",
        "version",
        "dataset_file",
        "fixtures_dir",
        "default_scopes",
        "tools_allowlist",
        "skill_allowlist",
        "memory_mode",
        "max_turns",
    }
    unknown_fields = set(manifest) - allowed_fields
    if unknown_fields:
        raise ScenarioPackError(
            f"unsupported scenario pack fields: {sorted(unknown_fields)}"
        )
    if manifest.get("schema_version") != SCENARIO_PACK_SCHEMA_VERSION:
        raise ScenarioPackError("unsupported scenario pack schema_version")
    pack_id = _safe_id(manifest.get("id"), "scenario pack id")
    name = _bounded_text(manifest.get("name"), "scenario pack name")
    version = _bounded_text(manifest.get("version"), "scenario pack version")
    if not SEMVER_RE.fullmatch(version):
        raise ScenarioPackError("scenario pack version must be semantic versioning")
    dataset_file = _relative_component(
        manifest.get("dataset_file"),
        "scenario pack dataset_file",
    )
    if dataset_file != dataset.name:
        raise ScenarioPackError("scenario pack dataset_file does not match dataset")
    if dataset.suffix.lower() != ".jsonl":
        raise ScenarioPackError("scenario pack datasets must use JSONL")
    fixtures_dir = _relative_component(
        manifest.get("fixtures_dir", "fixtures"),
        "scenario pack fixtures_dir",
    )
    default_scopes = _bounded_string_list(
        manifest.get("default_scopes", []),
        "scenario pack default_scopes",
    )
    unsupported_scopes = set(default_scopes) - SCENARIO_PACK_V1_SCOPES
    if unsupported_scopes:
        raise ScenarioPackError(
            "scenario pack v1 does not isolate scopes: "
            f"{sorted(unsupported_scopes)}"
        )
    tools_allowlist = _bounded_string_list(
        manifest.get("tools_allowlist", []),
        "scenario pack tools_allowlist",
        safe_ids=True,
    )
    unsupported_tools = set(tools_allowlist) - SCENARIO_PACK_V1_TOOLS
    if unsupported_tools:
        raise ScenarioPackError(
            "scenario pack v1 does not isolate tools: "
            f"{sorted(unsupported_tools)}"
        )
    skill_allowlist = _bounded_string_list(
        manifest.get("skill_allowlist", []),
        "scenario pack skill_allowlist",
        safe_ids=True,
    )
    memory_mode = _bounded_text(
        manifest.get("memory_mode", "none"),
        "scenario pack memory_mode",
        limit=16,
    )
    if memory_mode != "none":
        raise ScenarioPackError("scenario packs currently require memory_mode none")
    max_turns = manifest.get("max_turns", 12)
    if (
        isinstance(max_turns, bool)
        or not isinstance(max_turns, int)
        or max_turns <= 0
        or max_turns > 40
    ):
        raise ScenarioPackError("scenario pack max_turns must be between 1 and 40")

    dataset_digest, dataset_bytes = _file_sha256(
        dataset,
        size_limit=20 * 1024 * 1024,
    )
    fixtures_root = dataset.parent / fixtures_dir
    inventory, fixture_bytes = _fixture_inventory(fixtures_root)
    digest_payload = {
        "manifest": {
            "schema_version": SCENARIO_PACK_SCHEMA_VERSION,
            "id": pack_id,
            "name": name,
            "version": version,
            "dataset_file": dataset_file,
            "fixtures_dir": fixtures_dir,
            "default_scopes": list(default_scopes),
            "tools_allowlist": list(tools_allowlist),
            "skill_allowlist": list(skill_allowlist),
            "memory_mode": memory_mode,
            "max_turns": max_turns,
        },
        "dataset": {
            "sha256": dataset_digest,
            "size_bytes": dataset_bytes,
        },
        "fixtures": inventory,
    }
    pack_digest = hashlib.sha256(
        json.dumps(
            digest_payload,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    return ScenarioPack(
        pack_id=pack_id,
        name=name,
        version=version,
        pack_dir=dataset.parent,
        dataset_file=dataset_file,
        fixtures_dir=fixtures_dir,
        default_scopes=default_scopes,
        tools_allowlist=tools_allowlist,
        skill_allowlist=skill_allowlist,
        memory_mode=memory_mode,
        max_turns=max_turns,
        pack_digest=pack_digest,
        dataset_sha256=dataset_digest,
        dataset_bytes=dataset_bytes,
        fixture_files=len(inventory),
        fixture_bytes=fixture_bytes,
        fixture_inventory=tuple(
            (item["path"], item["sha256"], item["size_bytes"])
            for item in inventory
        ),
    )


def validate_scenario_dataset_bytes(
    pack: ScenarioPack,
    encoded: bytes,
) -> None:
    if (
        len(encoded) != pack.dataset_bytes
        or hashlib.sha256(encoded).hexdigest() != pack.dataset_sha256
    ):
        raise ScenarioPackError(
            "scenario dataset changed while the pack was being read"
        )


def _case_runtime_config(
    pack: ScenarioPack,
    case: dict[str, Any],
) -> tuple[str, tuple[str, ...]]:
    raw = case.get("runtime")
    if raw in (None, {}):
        return "", pack.default_scopes
    if not isinstance(raw, dict):
        raise ScenarioPackError("scenario case runtime must be an object")
    unknown = set(raw) - {"fixture", "scopes"}
    if unknown:
        raise ScenarioPackError(
            f"unsupported scenario case runtime fields: {sorted(unknown)}"
        )
    fixture = ""
    if raw.get("fixture") not in (None, ""):
        fixture = _relative_component(raw["fixture"], "scenario case fixture")
    scopes = (
        _bounded_string_list(raw["scopes"], "scenario case scopes")
        if "scopes" in raw
        else pack.default_scopes
    )
    if not set(scopes).issubset(set(pack.default_scopes)):
        raise ScenarioPackError(
            "scenario case scopes must be a subset of pack default_scopes"
        )
    return fixture, scopes


def validate_scenario_cases(
    pack: ScenarioPack,
    cases: list[dict[str, Any]],
) -> None:
    for case in cases:
        fixture, _scopes = _case_runtime_config(pack, case)
        if fixture:
            fixture_root = pack.pack_dir / pack.fixtures_dir / fixture
            if fixture_root.is_symlink() or not fixture_root.is_dir():
                raise ScenarioPackError(
                    f"scenario case fixture not found: {fixture}"
                )


def _copy_fixture_tree(
    source: Path,
    target: Path,
    expected_inventory: tuple[tuple[str, str, int], ...],
) -> None:
    if source.is_symlink() or not source.is_dir():
        raise ScenarioPackError(f"scenario case fixture not found: {source.name}")
    copied_files = 0
    copied_bytes = 0
    copied_inventory: list[tuple[str, str, int]] = []
    for path in sorted(source.rglob("*")):
        if path.is_symlink():
            raise ScenarioPackError("scenario case fixture cannot contain symbolic links")
        relative = path.relative_to(source)
        destination = target / relative
        if path.is_dir():
            destination.mkdir(parents=True, exist_ok=True)
            continue
        if not path.is_file():
            raise ScenarioPackError("scenario case fixture must contain regular files")
        copied_files += 1
        if copied_files > MAX_FIXTURE_FILES:
            raise ScenarioPackError("scenario case fixture exceeds file limit")
        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            flags = os.O_RDONLY
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            source_descriptor = os.open(path, flags)
            with (
                os.fdopen(source_descriptor, "rb") as source_file,
                destination.open("xb") as destination_file,
            ):
                digest = hashlib.sha256()
                file_bytes = 0
                while True:
                    chunk = source_file.read(64 * 1024)
                    if not chunk:
                        break
                    copied_bytes += len(chunk)
                    file_bytes += len(chunk)
                    if copied_bytes > MAX_FIXTURE_BYTES:
                        raise ScenarioPackError(
                            "scenario case fixture exceeds size limit"
                        )
                    digest.update(chunk)
                    destination_file.write(chunk)
                copied_inventory.append(
                    (
                        relative.as_posix(),
                        digest.hexdigest(),
                        file_bytes,
                    )
                )
        except ScenarioPackError:
            raise
        except OSError as exc:
            raise ScenarioPackError(
                f"failed to copy scenario case fixture: {type(exc).__name__}"
            ) from exc
    if tuple(copied_inventory) != expected_inventory:
        raise ScenarioPackError(
            "scenario case fixture changed after pack validation"
        )


def prepare_case_workspace(
    pack: ScenarioPack,
    case: dict[str, Any],
    run_dir: str | Path,
) -> EvalCaseRuntime:
    case_id = _safe_id(case.get("id"), "scenario case id")
    fixture, scopes = _case_runtime_config(pack, case)
    sandbox_parent = Path(run_dir).resolve() / "sandboxes"
    sandbox_parent.mkdir(parents=True, exist_ok=True)
    workspace = sandbox_parent / f"{case_id}-{uuid.uuid4().hex[:12]}"
    workspace.mkdir()
    try:
        if fixture:
            prefix = f"{fixture}/"
            expected_inventory = tuple(
                (path[len(prefix) :], digest, size)
                for path, digest, size in pack.fixture_inventory
                if path.startswith(prefix)
            )
            _copy_fixture_tree(
                pack.pack_dir / pack.fixtures_dir / fixture,
                workspace,
                expected_inventory,
            )
    except Exception:
        shutil.rmtree(workspace, ignore_errors=True)
        raise
    return EvalCaseRuntime(
        workspace_root=str(workspace),
        sandbox_parent=str(sandbox_parent),
        scopes=scopes,
        tools_allowlist=pack.tools_allowlist,
        skill_allowlist=pack.skill_allowlist,
        memory_mode=pack.memory_mode,
        max_turns=pack.max_turns,
        pack_id=pack.pack_id,
        pack_version=pack.version,
        pack_digest=pack.pack_digest,
    )


def cleanup_case_workspace(runtime: EvalCaseRuntime) -> None:
    workspace = Path(runtime.workspace_root)
    sandbox_parent = Path(runtime.sandbox_parent).resolve()
    if (
        workspace.parent.resolve() != sandbox_parent
        or sandbox_parent.name != "sandboxes"
    ):
        raise ScenarioPackError("refusing to clean an invalid scenario workspace")
    if workspace.is_symlink():
        workspace.unlink()
        raise ScenarioPackError(
            "scenario workspace was replaced by a symbolic link"
        )
    shutil.rmtree(workspace)
