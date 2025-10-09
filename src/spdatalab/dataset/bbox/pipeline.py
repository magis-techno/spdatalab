"""
Utility objects that orchestrate bbox analysis jobs.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Protocol


class ProgressSink(Protocol):
    """Captures progress events for long running pipelines."""

    def on_success(self, tokens: Iterable[str], batch_num: int | None = None) -> None:
        ...

    def on_failure(
        self,
        tokens: Iterable[str],
        reason: str,
        batch_num: int | None = None,
        stage: str | None = None,
    ) -> None:
        ...


@dataclass(slots=True)
class ProgressTrackerAdapter:
    """
    Thin adapter that will later wrap the legacy ``LightweightProgressTracker``.
    """

    tracker: Any
    success_cache: set[str] = field(default_factory=set)

    def on_success(self, tokens: Iterable[str], batch_num: int | None = None) -> None:
        for token in tokens:
            if token not in self.success_cache:
                self.success_cache.add(token)
                self.tracker.save_successful_batch([token], batch_num=batch_num)

    def on_failure(
        self,
        tokens: Iterable[str],
        reason: str,
        batch_num: int | None = None,
        stage: str | None = None,
    ) -> None:
        for token in tokens:
            self.tracker.save_failed_record(
                token, reason, batch_num=batch_num, fail_stage=stage
            )
