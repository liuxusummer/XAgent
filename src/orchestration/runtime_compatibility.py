"""Neutral, deterministic Worker runtime compatibility primitives."""

from __future__ import annotations

import re
from dataclasses import dataclass

_RUNTIME_VERSION = re.compile(
    r"^(0|[1-9][0-9]{0,5})(?:\.(0|[1-9][0-9]{0,5})){0,3}$"
)


class RuntimeCompatibilityValidationError(ValueError):
    """A runtime version or compatibility range is not canonical."""


def runtime_version_tuple(
    value: object,
    field_name: str = "runtime_version",
) -> tuple[int, int, int, int]:
    if not isinstance(value, str) or _RUNTIME_VERSION.fullmatch(value) is None:
        raise RuntimeCompatibilityValidationError(
            f"{field_name} must be a canonical numeric runtime version"
        )
    parts = tuple(int(part) for part in value.split("."))
    return parts + (0,) * (4 - len(parts))


def canonical_runtime_version(
    value: object,
    field_name: str = "runtime_version",
) -> str:
    runtime_version_tuple(value, field_name)
    assert isinstance(value, str)
    return value


@dataclass(frozen=True, slots=True)
class RuntimeCompatibility:
    """Canonical numeric Worker runtime range shared by admission paths."""

    min_runtime_version: str = "0"
    max_runtime_version: str | None = None

    def __post_init__(self) -> None:
        minimum = canonical_runtime_version(
            self.min_runtime_version,
            "min_runtime_version",
        )
        object.__setattr__(self, "min_runtime_version", minimum)
        maximum = self.max_runtime_version
        if maximum is not None:
            maximum = canonical_runtime_version(
                maximum,
                "max_runtime_version",
            )
            if runtime_version_tuple(
                maximum,
                "max_runtime_version",
            ) < runtime_version_tuple(
                minimum,
                "min_runtime_version",
            ):
                raise RuntimeCompatibilityValidationError(
                    "max_runtime_version must not precede min_runtime_version"
                )
        object.__setattr__(self, "max_runtime_version", maximum)

    def accepts(self, runtime_version: object) -> bool:
        """Return False for malformed or out-of-range Worker versions."""

        try:
            worker_version = runtime_version_tuple(runtime_version)
        except RuntimeCompatibilityValidationError:
            return False
        if worker_version < runtime_version_tuple(
            self.min_runtime_version,
            "min_runtime_version",
        ):
            return False
        if (
            self.max_runtime_version is not None
            and worker_version
            > runtime_version_tuple(
                self.max_runtime_version,
                "max_runtime_version",
            )
        ):
            return False
        return True


__all__ = [
    "RuntimeCompatibility",
    "RuntimeCompatibilityValidationError",
    "canonical_runtime_version",
    "runtime_version_tuple",
]
