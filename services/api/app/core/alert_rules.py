"""Vegetation-index alert rules - pure evaluation logic (no I/O).

Parameters live in the index registry (``AlertDefaults`` in
app/tasks/indices.py); this module only decides, for one observation of one
index, which alerts its rules produce. See deploy/coolify/alert-rules.md for what is
verified and what still needs agronomic calibration.

Rules (all "below" comparisons on the field's zonal mean):

* ``<index>_threshold`` - the mean is below ``threshold``; high severity
  below ``threshold_high``, medium otherwise.
* ``<index>_drop`` - the mean dropped at least ``drop_pct`` % against the
  reference = mean of the last ``drop_window`` valid observations of the
  SAME index strictly before the observed date. With fewer valid prior
  observations, or a non-positive reference (a % is meaningless there), the
  rule abstains.

A rule whose parameter is ``None`` is not configured and never evaluated.
Invalid input (None/NaN/inf) never produces an alert.
"""

from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date

RULES_VERSION = 2


@dataclass(frozen=True)
class AlertCandidate:
    rule_name: str
    index_type: str
    observed_date: date
    severity: str
    message: str
    params: dict = field(default_factory=dict)


def _valid(value) -> bool:
    try:
        return value is not None and math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def clean_history(
    history: Iterable[tuple[date, float | None]], before: date
) -> list[tuple[date, float]]:
    """Prior observations usable as a reference, oldest first.

    Keeps only valid values dated strictly before ``before`` (so the
    observation being evaluated - or an older copy of it when a scene is
    reprocessed - is never part of its own reference). A date carrying two
    different values is ambiguous and dropped. The result depends only on
    the data, never on the input order.
    """
    by_date: dict[date, set[float]] = {}
    for obs_date, value in history:
        if obs_date is None or obs_date >= before or not _valid(value):
            continue
        by_date.setdefault(obs_date, set()).add(round(float(value), 9))
    return [
        (d, next(iter(vals))) for d, vals in sorted(by_date.items()) if len(vals) == 1
    ]


def evaluate_threshold(index_def, observed_date: date, value) -> AlertCandidate | None:
    cfg = index_def.alerts
    if cfg.threshold is None or not _valid(value):
        return None
    value = float(value)
    if value >= cfg.threshold:
        return None
    high = cfg.threshold_high is not None and value < cfg.threshold_high
    return AlertCandidate(
        rule_name=f"{index_def.key}_threshold",
        index_type=index_def.key,
        observed_date=observed_date,
        severity="high" if high else "medium",
        message=(
            f"{index_def.label} mean ({value:.3f}) below threshold "
            f"({cfg.threshold}). Consider scouting."
        ),
        params={
            "rules_version": RULES_VERSION,
            "threshold": cfg.threshold,
            "threshold_high": cfg.threshold_high,
            "comparison": "below",
            "observed": round(value, 6),
            "calibration": cfg.calibration,
        },
    )


def evaluate_drop(
    index_def,
    observed_date: date,
    value,
    history: Iterable[tuple[date, float | None]],
) -> AlertCandidate | None:
    cfg = index_def.alerts
    if cfg.drop_pct is None or not _valid(value):
        return None
    prior = clean_history(history, observed_date)
    if len(prior) < cfg.drop_window:
        return None  # not enough evidence for a reference
    window = prior[-cfg.drop_window :]
    reference = sum(v for _, v in window) / len(window)
    if reference <= 0:
        return None  # a relative drop is undefined for non-positive references
    value = float(value)
    drop = (reference - value) / reference * 100
    if drop < cfg.drop_pct:
        return None
    severity = "high" if drop >= 30 else "medium" if drop >= 20 else "low"
    return AlertCandidate(
        rule_name=f"{index_def.key}_drop",
        index_type=index_def.key,
        observed_date=observed_date,
        severity=severity,
        message=(
            f"{index_def.label} dropped {drop:.1f}% "
            f"(from avg {reference:.3f} to {value:.3f}). Investigate crop stress."
        ),
        params={
            "rules_version": RULES_VERSION,
            # kept for compatibility: the configured threshold and window
            "drop_pct": cfg.drop_pct,
            "window": cfg.drop_window,
            "observed": round(value, 6),
            "observed_drop_pct": round(drop, 3),
            "reference_mean": round(reference, 6),
            "reference_dates": [d.isoformat() for d, _ in window],
            "calibration": cfg.calibration,
        },
    )


def evaluate_index_rules(
    index_def,
    observed_date: date,
    value,
    history: Iterable[tuple[date, float | None]],
) -> list[AlertCandidate]:
    history = list(history)
    return [
        c
        for c in (
            evaluate_threshold(index_def, observed_date, value),
            evaluate_drop(index_def, observed_date, value, history),
        )
        if c is not None
    ]
