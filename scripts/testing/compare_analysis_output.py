#!/usr/bin/env python3
"""Compare generated analysis artefacts against stored baselines."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable

import pandas as pd

DEFAULT_BASELINE = Path("tests/data/baseline")
SUPPORTED_EXTENSIONS = {".csv", ".json", ".txt"}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("current", type=Path, help="当前分析输出所在的目录")
    parser.add_argument(
        "--baseline",
        type=Path,
        default=DEFAULT_BASELINE,
        help="参考基线目录，默认使用 tests/data/baseline",
    )
    parser.add_argument(
        "--tolerance",
        type=float,
        default=1e-6,
        help="允许的数值误差 (用于浮点列)",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="遇到第一个差异即退出",
    )
    return parser.parse_args(argv)


def _iter_baseline_files(base_dir: Path) -> Iterable[Path]:
    for path in sorted(base_dir.rglob("*")):
        if path.is_file() and path.suffix.lower() in SUPPORTED_EXTENSIONS:
            yield path


def _load_table(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() == ".json":
        with path.open(encoding="utf-8") as handle:
            payload = json.load(handle)
        return pd.json_normalize(payload)
    return pd.read_csv(path, header=None)


def _compare_frames(
    baseline: pd.DataFrame,
    current: pd.DataFrame,
    *,
    tolerance: float,
) -> list[str]:
    errors: list[str] = []
    if baseline.shape != current.shape:
        errors.append(f"shape mismatch: expected {baseline.shape}, got {current.shape}")
        return errors

    for column in baseline.columns:
        base_series = baseline[column]
        curr_series = current[column]
        if pd.api.types.is_numeric_dtype(base_series):
            if not base_series.equals(curr_series):
                diff = (base_series - curr_series).abs().fillna(0)
                if (diff > tolerance).any():
                    errors.append(f"numeric column '{column}' differs beyond tolerance")
        else:
            if not base_series.fillna("<NA>").equals(curr_series.fillna("<NA>")):
                errors.append(f"column '{column}' differs")
    return errors


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    if not args.current.exists():
        print(f"❌ current output directory not found: {args.current}")
        return 1
    if not args.baseline.exists():
        print(f"❌ baseline directory not found: {args.baseline}")
        return 1

    failures: list[str] = []
    for baseline_file in _iter_baseline_files(args.baseline):
        rel = baseline_file.relative_to(args.baseline)
        current_file = args.current / rel
        if not current_file.exists():
            message = f"missing file: {rel}"
            failures.append(message)
            print(f"❌ {message}")
            if args.fail_fast:
                break
            continue

        try:
            baseline_frame = _load_table(baseline_file)
            current_frame = _load_table(current_file)
        except Exception as exc:  # noqa: BLE001 - bubble up details to user
            message = f"failed to load {rel}: {exc}"
            failures.append(message)
            print(f"❌ {message}")
            if args.fail_fast:
                break
            continue

        diff = _compare_frames(baseline_frame, current_frame, tolerance=args.tolerance)
        if diff:
            for item in diff:
                message = f"{rel}: {item}"
                failures.append(message)
                print(f"❌ {message}")
                if args.fail_fast:
                    break
        else:
            print(f"✅ {rel} matched baseline")

        if args.fail_fast and failures:
            break

    if failures:
        print(f"\n比较完成：发现 {len(failures)} 处差异")
        return 1

    print("\n比较完成：所有文件与基线一致")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
