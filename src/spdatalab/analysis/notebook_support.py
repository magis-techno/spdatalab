"""Helper utilities shared across exploratory notebooks."""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

import pandas as pd

NB_FAST_ENV_VAR = "SPDATALAB_NOTEBOOK_FAST"

__all__ = [
    "NB_FAST_ENV_VAR",
    "NotebookDisplayConfig",
    "configure_project_paths",
    "display_config",
    "is_fast_mode",
    "preview_dataframe",
]


@dataclass(slots=True)
class NotebookDisplayConfig:
    """Simple container describing the active notebook context."""

    project_root: Path
    extra_paths: tuple[Path, ...]


def configure_project_paths(start: Path | None = None) -> NotebookDisplayConfig:
    """Ensure the project root and ``src`` directory are importable."""

    start = start or Path.cwd()
    project_root = _find_project_root(start)

    src_path = project_root / "src"
    extra_paths: list[Path] = []

    for path in (project_root, src_path):
        if path.exists() and path.is_dir() and str(path) not in sys.path:
            sys.path.insert(0, str(path))
            extra_paths.append(path)

    return NotebookDisplayConfig(project_root=project_root, extra_paths=tuple(extra_paths))


def _find_project_root(start: Path) -> Path:
    for candidate in [start, *start.parents]:
        if (candidate / "pyproject.toml").exists():
            return candidate
    raise FileNotFoundError("Unable to locate project root from notebook location")


def display_config(config: Mapping[str, object] | NotebookDisplayConfig) -> None:
    """Print the active configuration in a consistent way for notebooks."""

    if isinstance(config, NotebookDisplayConfig):
        payload = {
            "project_root": str(config.project_root),
            "extra_paths": ", ".join(str(path) for path in config.extra_paths) or "<none>",
        }
    else:
        payload = {key: value for key, value in config.items()}

    for key, value in payload.items():
        print(f"{key}: {value}")


def is_fast_mode() -> bool:
    """Return ``True`` when the notebook should run in lightweight mode."""

    return os.environ.get(NB_FAST_ENV_VAR, "0") not in {"", "0", "false", "False"}


def preview_dataframe(frame: pd.DataFrame, *, max_rows: int = 5) -> pd.DataFrame:
    """Return a trimmed copy of ``frame`` for quick inspection."""

    if frame.empty:
        return frame
    return frame.head(max_rows)
