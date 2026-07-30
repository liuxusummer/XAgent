"""Descriptor-relative, no-follow filesystem operations.

Callers resolve their authorization boundary first, then open a relative path
component-by-component beneath that boundary. The returned descriptor remains
bound to the opened object even if directory entries are replaced concurrently.
"""

from __future__ import annotations

import os
import secrets
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


def secure_descriptor_mutations_supported() -> bool:
    return (
        secure_descriptor_reads_supported()
        and hasattr(os, "fchmod")
        and hasattr(os, "fsync")
        and os.mkdir in os.supports_dir_fd
        and os.stat in os.supports_dir_fd
        and os.stat in os.supports_follow_symlinks
        and os.unlink in os.supports_dir_fd
        and os.rmdir in os.supports_dir_fd
        and os.rename in os.supports_dir_fd
    )


def _open_directory_beneath(
    root: str | Path,
    parts: tuple[str, ...],
    *,
    create: bool,
) -> int:
    close_on_exec = getattr(os, "O_CLOEXEC", 0)
    directory_flags = (
        os.O_RDONLY
        | os.O_DIRECTORY
        | os.O_NOFOLLOW
        | close_on_exec
    )
    current_directory = os.open(Path(root), directory_flags)
    try:
        for part in parts:
            try:
                next_directory = os.open(
                    part,
                    directory_flags,
                    dir_fd=current_directory,
                )
            except FileNotFoundError:
                if not create:
                    raise
                try:
                    os.mkdir(part, mode=0o755, dir_fd=current_directory)
                except FileExistsError:
                    # A concurrent writer may have created this entry after
                    # open(). Re-open with O_NOFOLLOW to validate its type.
                    pass
                next_directory = os.open(
                    part,
                    directory_flags,
                    dir_fd=current_directory,
                )
            os.close(current_directory)
            current_directory = next_directory
        return current_directory
    except BaseException:
        os.close(current_directory)
        raise


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
    final_flags = os.O_RDONLY | os.O_NOFOLLOW | close_on_exec
    file_descriptor: int | None = None
    parent_descriptor: int | None = None
    try:
        if not parts:
            file_descriptor = os.open(
                Path(root),
                final_flags,
            )
            return file_descriptor, os.fstat(file_descriptor)
        parent_descriptor = _open_directory_beneath(
            root,
            parts[:-1],
            create=False,
        )
        file_descriptor = os.open(
            parts[-1],
            final_flags,
            dir_fd=parent_descriptor,
        )
        return file_descriptor, os.fstat(file_descriptor)
    except BaseException:
        if file_descriptor is not None:
            os.close(file_descriptor)
        raise
    finally:
        if parent_descriptor is not None:
            os.close(parent_descriptor)


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


def _mutation_target(
    root: str | Path,
    relative_path: str | Path,
    *,
    create_parents: bool,
) -> tuple[int, str]:
    if not secure_descriptor_mutations_supported():
        raise SecureFileReadUnavailableError(
            "descriptor-relative filesystem mutations are unavailable"
        )
    candidate = Path(relative_path)
    parts = candidate.parts
    if (
        candidate.is_absolute()
        or not parts
        or any(part in {"", ".", ".."} for part in parts)
    ):
        raise SecurePathError(
            "mutation path is not safe relative to its boundary"
        )
    parent_descriptor = _open_directory_beneath(
        root,
        parts[:-1],
        create=create_parents,
    )
    return parent_descriptor, parts[-1]


def atomic_write_bytes_beneath(
    root: str | Path,
    relative_path: str | Path,
    content: bytes,
    *,
    create_parents: bool = True,
    default_mode: int = 0o644,
) -> None:
    if not isinstance(content, bytes):
        raise TypeError("content must be bytes")
    parent_descriptor, target_name = _mutation_target(
        root,
        relative_path,
        create_parents=create_parents,
    )
    temp_name = ""
    temp_descriptor: int | None = None
    try:
        try:
            target_stat = os.stat(
                target_name,
                dir_fd=parent_descriptor,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            target_stat = None
        if target_stat is not None:
            if stat_module.S_ISLNK(target_stat.st_mode):
                raise SecurePathError(
                    "refusing to replace a symbolic link"
                )
            if stat_module.S_ISDIR(target_stat.st_mode):
                raise IsADirectoryError("write target is a directory")
            if not stat_module.S_ISREG(target_stat.st_mode):
                raise SecurePathError(
                    "write target must be a regular file"
                )
            # Preserve ordinary access bits, but never reproduce setuid/setgid
            # or sticky bits on Agent-generated replacement content.
            target_mode = stat_module.S_IMODE(target_stat.st_mode) & 0o777
            preserve_mode = True
        else:
            target_mode = default_mode
            preserve_mode = False

        flags = (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | os.O_NOFOLLOW
            | getattr(os, "O_CLOEXEC", 0)
        )
        for _attempt in range(32):
            temp_name = f".xagent-{secrets.token_hex(12)}.tmp"
            try:
                temp_descriptor = os.open(
                    temp_name,
                    flags,
                    target_mode,
                    dir_fd=parent_descriptor,
                )
                break
            except FileExistsError:
                continue
        if temp_descriptor is None:
            raise OSError("could not allocate a unique temporary file")

        view = memoryview(content)
        written = 0
        while written < len(view):
            write_count = os.write(temp_descriptor, view[written:])
            if write_count <= 0:
                raise OSError("temporary file write made no progress")
            written += write_count
        if preserve_mode:
            os.fchmod(temp_descriptor, target_mode)
        os.fsync(temp_descriptor)
        os.close(temp_descriptor)
        temp_descriptor = None
        os.rename(
            temp_name,
            target_name,
            src_dir_fd=parent_descriptor,
            dst_dir_fd=parent_descriptor,
        )
        temp_name = ""
        _fsync_directory_descriptor(parent_descriptor)
    finally:
        if temp_descriptor is not None:
            os.close(temp_descriptor)
        if temp_name:
            try:
                os.unlink(temp_name, dir_fd=parent_descriptor)
            except FileNotFoundError:
                pass
        os.close(parent_descriptor)


def atomic_write_text_beneath(
    root: str | Path,
    relative_path: str | Path,
    content: str,
    *,
    encoding: str = "utf-8",
    create_parents: bool = True,
) -> None:
    if not isinstance(content, str):
        raise TypeError("content must be text")
    atomic_write_bytes_beneath(
        root,
        relative_path,
        content.encode(encoding),
        create_parents=create_parents,
    )


def open_lock_file_beneath(
    root: str | Path,
    relative_path: str | Path,
    *,
    default_mode: int = 0o600,
) -> int:
    parent_descriptor, target_name = _mutation_target(
        root,
        relative_path,
        create_parents=True,
    )
    file_descriptor: int | None = None
    try:
        file_descriptor = os.open(
            target_name,
            os.O_RDWR
            | os.O_CREAT
            | os.O_NOFOLLOW
            | getattr(os, "O_CLOEXEC", 0),
            default_mode,
            dir_fd=parent_descriptor,
        )
        current_stat = os.fstat(file_descriptor)
        if not stat_module.S_ISREG(current_stat.st_mode):
            raise SecurePathError("lock target must be a regular file")
        result = file_descriptor
        file_descriptor = None
        return result
    finally:
        if file_descriptor is not None:
            os.close(file_descriptor)
        os.close(parent_descriptor)


def _fsync_directory_descriptor(directory_descriptor: int) -> None:
    # The namespace mutation has already committed. Some filesystems do not
    # support directory fsync, so preserve the successful operation semantics.
    try:
        os.fsync(directory_descriptor)
    except OSError:
        pass


def _remove_tree_at(parent_descriptor: int, name: str) -> None:
    close_on_exec = getattr(os, "O_CLOEXEC", 0)
    directory_descriptor = os.open(
        name,
        os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | close_on_exec,
        dir_fd=parent_descriptor,
    )
    try:
        if os.listdir not in os.supports_fd:
            raise SecureFileReadUnavailableError(
                "descriptor-relative directory listing is unavailable"
            )
        for child_name in sorted(os.listdir(directory_descriptor)):
            child_stat = os.stat(
                child_name,
                dir_fd=directory_descriptor,
                follow_symlinks=False,
            )
            if stat_module.S_ISDIR(child_stat.st_mode):
                _remove_tree_at(directory_descriptor, child_name)
            else:
                os.unlink(child_name, dir_fd=directory_descriptor)
    finally:
        os.close(directory_descriptor)
    os.rmdir(name, dir_fd=parent_descriptor)


def remove_path_beneath(
    root: str | Path,
    relative_path: str | Path,
    *,
    recursive: bool = False,
) -> bool:
    parent_descriptor, target_name = _mutation_target(
        root,
        relative_path,
        create_parents=False,
    )
    try:
        target_stat = os.stat(
            target_name,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
        is_directory = stat_module.S_ISDIR(target_stat.st_mode)
        if is_directory:
            if not recursive:
                raise IsADirectoryError(
                    "recursive deletion is required for a directory"
                )
            _remove_tree_at(parent_descriptor, target_name)
        else:
            os.unlink(target_name, dir_fd=parent_descriptor)
        _fsync_directory_descriptor(parent_descriptor)
        return is_directory
    finally:
        os.close(parent_descriptor)


__all__ = [
    "atomic_write_bytes_beneath",
    "atomic_write_text_beneath",
    "FileChangedDuringReadError",
    "FileSizeLimitExceededError",
    "SecureFileReadUnavailableError",
    "SecurePathError",
    "UnsafeFileContentError",
    "open_lock_file_beneath",
    "open_path_beneath",
    "open_regular_file_beneath",
    "remove_path_beneath",
    "read_stable_bytes",
    "read_stable_text",
    "secure_descriptor_reads_supported",
    "secure_descriptor_mutations_supported",
]
