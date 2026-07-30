from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from src.core.eval_scenarios import (
    ScenarioPackError,
    load_scenario_pack_for_dataset,
    validate_scenario_dataset_bytes,
    validate_scenario_cases,
)
from src.core.eval import EvalError, infer_format, parse_dataset


MAX_DATASET_BYTES = 20 * 1024 * 1024


def _workspace_dataset(workspace: str, dataset: str) -> tuple[Path, Path]:
    workspace_root = Path(workspace).expanduser().resolve()
    relative = str(dataset or "").strip().replace("\\", "/")
    if not relative or os.path.isabs(relative):
        raise ScenarioPackError("dataset must be a workspace-relative path")
    dataset_path = (workspace_root / relative).resolve()
    try:
        dataset_path.relative_to(workspace_root)
    except ValueError as exc:
        raise ScenarioPackError("dataset path escapes the workspace") from exc
    return workspace_root, dataset_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate an XAgent versioned Eval scenario pack"
    )
    parser.add_argument("--workspace", required=True, help="Workspace root")
    parser.add_argument(
        "--dataset",
        required=True,
        help="Workspace-relative path to the pack dataset",
    )
    args = parser.parse_args(argv)

    try:
        workspace, dataset = _workspace_dataset(args.workspace, args.dataset)
        pack = load_scenario_pack_for_dataset(workspace, dataset)
        if pack is None:
            raise ScenarioPackError("scenario pack manifest is missing")
        try:
            with dataset.open("rb") as file:
                encoded = file.read(MAX_DATASET_BYTES + 1)
            if len(encoded) > MAX_DATASET_BYTES:
                raise ScenarioPackError("scenario dataset exceeds 20MB limit")
            validate_scenario_dataset_bytes(pack, encoded)
            cases = parse_dataset(
                encoded.decode("utf-8"),
                infer_format(dataset.name),
                strict=True,
            )
            validate_scenario_cases(
                pack,
                [case.to_dict() for case in cases],
            )
        except (OSError, UnicodeError) as exc:
            raise ScenarioPackError(
                f"failed to read scenario dataset: {type(exc).__name__}"
            ) from exc
        revalidated = load_scenario_pack_for_dataset(workspace, dataset)
        if revalidated is None or revalidated.pack_digest != pack.pack_digest:
            raise ScenarioPackError("scenario pack changed during validation")
        print(
            json.dumps(
                {
                    "verdict": "valid",
                    "scenario_pack": pack.public_metadata(workspace),
                    "case_count": len(cases),
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (EvalError, ScenarioPackError) as exc:
        print(
            json.dumps(
                {"verdict": "invalid", "error": str(exc)},
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
