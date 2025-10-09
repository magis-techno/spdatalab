"""
Shared pipeline utilities used by the bbox dataset workflows.

This module provides two building blocks that used to live in the monolithic
``bbox.py`` file:

* ``LightweightProgressTracker`` – persists batch progress and keeps success /
  failure caches backed by Parquet files.
* ``setup_interrupt_handlers`` / ``InterruptFlag`` – register signal handlers
  that coordinate graceful shutdown when long running jobs receive interrupts.
"""

from __future__ import annotations

import json
import signal
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable, Optional

import pandas as pd

try:  # Optional dependency that enables Parquet persistence.
    import pyarrow as pa  # noqa: F401  # pragma: no cover - imported for side effect
    import pyarrow.parquet as pq  # noqa: F401

    PARQUET_AVAILABLE = True
except ImportError:  # pragma: no cover - executed in local dev environments.
    PARQUET_AVAILABLE = False

__all__ = [
    "PARQUET_AVAILABLE",
    "InterruptFlag",
    "setup_interrupt_handlers",
    "LightweightProgressTracker",
]


class InterruptFlag:
    """Mutable flag that records whether an interrupt was requested."""

    __slots__ = ("_interrupted",)

    def __init__(self) -> None:
        self._interrupted = False

    def set(self) -> None:
        """Mark the flag as interrupted."""

        self._interrupted = True

    def reset(self) -> None:
        """Clear any previous interrupt state."""

        self._interrupted = False

    def is_set(self) -> bool:
        """Return ``True`` when an interrupt has been recorded."""

        return self._interrupted

    def __bool__(self) -> bool:  # pragma: no cover - convenience for legacy code.
        return self._interrupted


def setup_interrupt_handlers(
    flag: InterruptFlag,
    *,
    on_interrupt: Optional[Callable[[int, Optional[object]], None]] = None,
    handle_sigbreak: bool | None = None,
) -> None:
    """
    Register signal handlers that set ``flag`` and optionally execute a callback.

    Parameters
    ----------
    flag:
        The mutable ``InterruptFlag`` instance that tracks the interrupt state.
    on_interrupt:
        Optional callback invoked on the first received signal. The callback
        receives ``(signum, frame)`` just like a regular ``signal`` handler.
    handle_sigbreak:
        When ``True`` the Windows ``SIGBREAK`` signal is also captured. Defaults
        to ``True`` on Windows and ``False`` elsewhere.
    """

    if handle_sigbreak is None:
        handle_sigbreak = hasattr(signal, "SIGBREAK")

    def _handler(signum: int, frame: object) -> None:
        if not flag.is_set() and on_interrupt is not None:
            on_interrupt(signum, frame)
        flag.set()

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)

    if handle_sigbreak and hasattr(signal, "SIGBREAK"):  # pragma: no cover
        signal.signal(signal.SIGBREAK, _handler)


@dataclass
class LightweightProgressTracker:
    """
    Persists pipeline progress using Parquet snapshots and JSON metadata.

    The tracker mirrors the behaviour of the original implementation while
    encapsulating the filesystem access so it can be reused by the refactored
    pipelines and unit tests.
    """

    work_dir: str = "./bbox_import_logs"
    buffer_size: int = 1000

    def __post_init__(self) -> None:
        base_dir = Path(self.work_dir).resolve()
        try:
            base_dir.mkdir(exist_ok=True, parents=True)
        except PermissionError:  # pragma: no cover - fallback on restricted FS.
            tmp_dir = Path(tempfile.gettempdir()) / "bbox_import_logs"
            tmp_dir.mkdir(exist_ok=True, parents=True)
            print(f"权限不足，使用临时目录: {tmp_dir}")
            base_dir = tmp_dir

        self.work_dir = str(base_dir)
        self.success_file = base_dir / "successful_tokens.parquet"
        self.failed_file = base_dir / "failed_tokens.parquet"
        self.progress_file = base_dir / "progress.json"

        self._success_cache = self._load_success_cache()
        self._failed_buffer: list[dict] = []
        self._success_buffer: list[dict] = []

    # ------------------------------------------------------------------ buffers
    def _load_success_cache(self) -> set[str]:
        if self.success_file.exists() and PARQUET_AVAILABLE:
            try:
                df = pd.read_parquet(self.success_file)
                cache = set(df["scene_token"].tolist())
                print(f"已加载 {len(cache)} 个成功处理的 scene_token")
                return cache
            except Exception as exc:  # pragma: no cover - defensive path
                print(f"加载成功记录失败: {exc}，将创建新文件")
        return set()

    # --------------------------------------------------------- success tracking
    def save_successful_batch(
        self, scene_tokens: Iterable[str], batch_num: int | None = None
    ) -> None:
        tokens = list(scene_tokens)
        if not tokens:
            return

        timestamp = datetime.now()
        for token in tokens:
            if token not in self._success_cache:
                self._success_buffer.append(
                    {
                        "scene_token": token,
                        "processed_at": timestamp,
                        "batch_num": batch_num,
                    }
                )
                self._success_cache.add(token)

        if len(self._success_buffer) >= self.buffer_size:
            self._flush_success_buffer()

    def _flush_success_buffer(self) -> None:
        if not self._success_buffer or not PARQUET_AVAILABLE:
            return

        new_df = pd.DataFrame(self._success_buffer)
        try:
            if self.success_file.exists():
                existing_df = pd.read_parquet(self.success_file)
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                combined_df = combined_df.drop_duplicates(
                    subset=["scene_token"], keep="last"
                )
            else:
                combined_df = new_df

            combined_df.to_parquet(self.success_file, index=False)
            print(f"已保存 {len(self._success_buffer)} 个成功记录到文件")
        except Exception as exc:  # pragma: no cover - IO errors
            print(f"保存成功记录失败: {exc}")

        self._success_buffer = []

    # ---------------------------------------------------------- failure tracking
    def save_failed_record(
        self,
        scene_token: str,
        error_msg: str,
        batch_num: int | None = None,
        fail_stage: str = "unknown",
    ) -> None:
        self._failed_buffer.append(
            {
                "scene_token": scene_token,
                "error_msg": str(error_msg),
                "batch_num": batch_num,
                "step": fail_stage,
                "failed_at": datetime.now(),
            }
        )

        if len(self._failed_buffer) >= self.buffer_size:
            self._flush_failed_buffer()

    def _flush_failed_buffer(self) -> None:
        if not self._failed_buffer or not PARQUET_AVAILABLE:
            return

        new_df = pd.DataFrame(self._failed_buffer)
        try:
            if self.failed_file.exists():
                existing_df = pd.read_parquet(self.failed_file)
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            else:
                combined_df = new_df

            combined_df.to_parquet(self.failed_file, index=False)
            print(f"已保存 {len(self._failed_buffer)} 个失败记录到文件")
        except Exception as exc:  # pragma: no cover
            print(f"保存失败记录失败: {exc}")

        self._failed_buffer = []

    # --------------------------------------------------------------- cache utils
    def get_remaining_tokens(self, all_tokens: Iterable[str]) -> list[str]:
        tokens = list(all_tokens)
        remaining = [token for token in tokens if token not in self._success_cache]
        print(
            f"总计 {len(tokens)} 条记录，已成功 {len(self._success_cache)} 条，剩余 {len(remaining)} 条"
        )
        return remaining

    def check_tokens_exist(self, tokens: Iterable[str]) -> set[str]:
        return set(tokens) & self._success_cache

    def load_failed_tokens(self) -> list[str]:
        if not self.failed_file.exists() or not PARQUET_AVAILABLE:
            return []

        try:
            failed_df = pd.read_parquet(self.failed_file)
            active_failed = failed_df[
                ~failed_df["scene_token"].isin(self._success_cache)
            ]
            failed_tokens = active_failed["scene_token"].unique().tolist()
            print(f"加载到 {len(failed_tokens)} 个失败的 scene_token")
            return failed_tokens
        except Exception as exc:  # pragma: no cover
            print(f"加载失败记录失败: {exc}")
            return []

    # -------------------------------------------------------------- progress meta
    def save_progress(
        self,
        total_scenes: int,
        processed_scenes: int,
        inserted_records: int,
        current_batch: int,
    ) -> None:
        progress = {
            "total_scenes": total_scenes,
            "processed_scenes": processed_scenes,
            "inserted_records": inserted_records,
            "current_batch": current_batch,
            "timestamp": datetime.now().isoformat(),
            "successful_count": len(self._success_cache),
            "failed_count": len(self._failed_buffer)
            + (
                len(pd.read_parquet(self.failed_file))
                if self.failed_file.exists() and PARQUET_AVAILABLE
                else 0
            ),
        }

        try:
            with open(self.progress_file, "w", encoding="utf-8") as handle:
                json.dump(progress, handle, ensure_ascii=False, indent=2)
        except Exception as exc:  # pragma: no cover
            print(f"保存进度信息失败: {exc}")

    def get_statistics(self) -> dict[str, object]:
        success_count = len(self._success_cache)
        failed_count = 0
        failed_by_step: dict[str, int] = {}

        if self.failed_file.exists() and PARQUET_AVAILABLE:
            try:
                failed_df = pd.read_parquet(self.failed_file)
                active_failed = failed_df[
                    ~failed_df["scene_token"].isin(self._success_cache)
                ]
                failed_count = len(active_failed["scene_token"].unique())
                if not active_failed.empty:
                    failed_by_step = (
                        active_failed["step"].value_counts().to_dict()  # type: ignore[assignment]
                    )
            except Exception as exc:  # pragma: no cover
                print(f"统计失败记录时发生异常: {exc}")

        return {
            "success_count": success_count,
            "failed_count": failed_count,
            "failed_by_step": failed_by_step,
        }

    def finalize(self) -> None:
        """Flush any buffered success / failure information to disk."""

        self._flush_success_buffer()
        self._flush_failed_buffer()
