"""Descriptor-relative, no-follow filesystem reads.

Callers resolve their authorization boundary first, then open a relative path
component-by-component beneath that boundary. The returned descriptor remains
bound to the opened object even if directory entries are replaced concurrently.
"""

from __future__ import annotations

import os
import stat as stat_module
from pathlib import Path


class SecureFileReadUnavailableError(OSError):
    pass


class SecurePathError(OSError):
    pass


class FileChangedDuringReadError(OSError):
    pass


class FileSizeLimitExceededError(ValueError):
    pass


class UnsafeFileContentError(ValueError):
    pass


def secure_descriptor_reads_supported() -> bool:
    return (
        hasattr(os, "O_NOFOLLOW")
        and hasattr(os, "O_DIRECTORY")
        and os.open in os.supports_dir_fd
    )


def open_path_beneath(
    root: str | Path,
    relative_path: str | Path,
) -> tuple[int, os.stat_result]:
    if not secure_descriptor_reads_supported():
        raise SecureFileReadUnavailableError(
            "descriptor-relative no-follow file reads are unavailable"
        )
    candidate = Path(relative_path)
    parts = candidate.parts
    if (
        candidate.is_absolute()
        or any(part in {"", ".", ".."} for part in parts)
    ):
        raise SecurePathError(
            "path is not safe relative to the read boundary"
        )

    close_on_exec = getattr(os, "O_CLOEXEC", 0)
    directory_flags = (
        os.O_RDONLY
        | os.O_DIRECTORY
        | os.O_NOFOLLOW
        | close_on_exec
    )
    final_flags = os.O_RDONLY | os.O_NOFOLLOW | close_on_exec
    directory_descriptors: list[int] = []
    file_descriptor: int | None = None
    try:
        if not parts:
            file_descriptor = os.open(
                Path(root),
                final_flags,
            )
            return file_descriptor, os.fstat(file_descriptor)
        current_directory = os.open(Path(root), directory_flags)
        directory_descriptors.append(current_directory)
        for part in parts[:-1]:
            current_directory = os.open(
                part,
                directory_flags,
                dir_fd=current_directory,
            )
            directory_descriptors.append(current_directory)
        file_descriptor = os.open(
            parts[-1],
            final_flags,
            dir_fd=current_directory,
        )
        return file_descriptor, os.fstat(file_descriptor)
    except BaseException:
        if file_descriptor is not None:
            os.close(file_descriptor)
        raise
    finally:
        for directory_descriptor in reversed(directory_descriptors):
            os.close(directory_descriptor)


def open_regular_file_beneath(
    root: str | Path,
    relative_path: str | Path,
) -> tuple[int, os.stat_result]:
    file_descriptor, current_stat = open_path_beneath(root, relative_path)
    if not stat_module.S_ISREG(current_stat.st_mode):
        os.close(file_descriptor)
        raise SecurePathError("read target must be a regular file")
    return file_descriptor, current_stat


def read_stable_bytes(
    file_descriptor: int,
    initial_stat: os.stat_result,
    *,
    max_bytes: int | None = None,
) -> bytes:
    size_limit = max_bytes if max_bytes is not None and max_bytes > 0 else None
    if size_limit is not None and initial_stat.st_size > size_limit:
        raise FileSizeLimitExceededError("file exceeds read size limit")

    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = os.read(file_descriptor, 64 * 1024)
        if not chunk:
            break
        total += len(chunk)
        if size_limit is not None and total > size_limit:
            raise FileSizeLimitExceededError("file exceeds read size limit")
        chunks.append(chunk)

    final_stat = os.fstat(file_descriptor)
    before = (
        initial_stat.st_dev,
        initial_stat.st_ino,
        initial_stat.st_size,
        initial_stat.st_mtime_ns,
        initial_stat.st_ctime_ns,
    )
    after = (
        final_stat.st_dev,
        final_stat.st_ino,
        final_stat.st_size,
        final_stat.st_mtime_ns,
        final_stat.st_ctime_ns,
    )
    if before != after or total != final_stat.st_size:
        raise FileChangedDuringReadError(
            "file changed while it was being read"
        )
    return b"".join(chunks)


def read_stable_text(
    file_descriptor: int,
    initial_stat: os.stat_result,
    *,
    max_bytes: int | None = None,
    reject_nul: bool = True,
) -> str:
    content = read_stable_bytes(
        file_descriptor,
        initial_stat,
        max_bytes=max_bytes,
    ).decode("utf-8")
    if reject_nul and "\x00" in content:
        raise UnsafeFileContentError("file contains NUL bytes")
    return content


__all__ = [
    "FileChangedDuringReadError",
    "FileSizeLimitExceededError",
    "SecureFileReadUnavailableError",
    "SecurePathError",
    "UnsafeFileContentError",
    "open_path_beneath",
    "open_regular_file_beneath",
    "read_stable_bytes",
    "read_stable_text",
    "secure_descriptor_reads_supported",
]
