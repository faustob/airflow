# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""StatsD-shaped metrics logger that is backed by the OpenTelemetry Metrics API.

This module replaces the previous `statsd.StatsClient`-backed logger. It keeps the exact
same public surface (`incr`, `decr`, `gauge`, `timing`, `timer`) and configuration knobs
(prefix, allow/block lists, influxdb-style tags, stat name handler) used throughout
Airflow's metrics call sites (`Stats.incr(...)`, `Stats.gauge(...)`, `Stats.timing(...)`,
etc.), so every call site that previously drove the StatsD wire protocol now emits the
same metric name/value/tags through OTel instruments instead.

Metric names are preserved verbatim (including any configured `statsd_prefix`, since the
vendor client concatenated `prefix.stat` onto the wire and callers relied on that final
name appearing on dashboards). We reproduce that same concatenation here.

Instrument selection:
  * `incr` -> a monotonic `Counter` per stat name (StatsD counters are cumulative counts
    and dashboards aggregate them monotonically), EXCEPT stats listed in `UP_DOWN_STATS`
    (the ones the estate actually `decr`s), which use a single `UpDownCounter` for both
    `incr` and `decr` so contributions net on one instrument. This mirrors the repo's own
    OTel backend (`otel_logger.UP_DOWN_COUNTERS`).
  * `gauge` -> a synchronous `Gauge` (`meter.create_gauge`) since this OTel target version
    (>=1.27.0 API surface with SDK >=1.27.0) supports synchronous gauges via
    `Meter.create_gauge`. Each `gauge()` call directly records the current value, matching
    the vendor's "set" semantics (including its `delta` flag, which we honor by tracking
    last-seen value per stat/tag-set and adding the delta before recording, to preserve the
    vendor's incremental-gauge behavior).
  * `timing`/`timer` -> a `Histogram` per stat name, recording milliseconds exactly as the
    vendor client did.

All instruments are created once per stat name (module-level registry keyed by name+kind)
rather than per call, per the contract.
"""

from __future__ import annotations

import datetime
import logging
import time
from collections.abc import Callable
from functools import wraps
from typing import TYPE_CHECKING, TypeVar, cast

from opentelemetry import metrics as otel_metrics

from .protocols import Timer
from .validators import (
    PatternAllowListValidator,
    PatternBlockListValidator,
    get_validator,
    validate_stat,
)

if TYPE_CHECKING:
    from .protocols import DeltaType
    from .validators import ListValidator

T = TypeVar("T", bound=Callable)

log = logging.getLogger(__name__)

_meter = otel_metrics.get_meter("airflow.observability.statsd_compat")

# Stats that legitimately move in both directions: `incr` AND `decr` are called on them,
# so both must land on ONE UpDownCounter to net correctly. Keyed by the UNPREFIXED stat
# name (instrument selection happens before the configured `statsd_prefix` is applied, so
# a non-default prefix cannot break netting). Mirrors `otel_logger.UP_DOWN_COUNTERS`
# (kept local: that module imports the OTel SDK, this one stays API-only). Every current
# `decr` call site in the codebase targets dag_processing.processes; a stat that gains a
# `decr` caller later must be added here or its decrements are dropped with a warning.
UP_DOWN_STATS = {"dag_processing.processes"}

_counters: dict[str, otel_metrics.Counter | otel_metrics.UpDownCounter] = {}
_gauges: dict[str, otel_metrics.Gauge] = {}
_histograms: dict[str, otel_metrics.Histogram] = {}
_gauge_last_values: dict[tuple, float] = {}


def _get_counter(full_stat: str, updown: bool) -> otel_metrics.Counter | otel_metrics.UpDownCounter:
    counter = _counters.get(full_stat)
    if counter is None:
        if updown:
            counter = _meter.create_up_down_counter(name=full_stat)
        else:
            counter = _meter.create_counter(name=full_stat)
        _counters[full_stat] = counter
    return counter


def _get_gauge(stat: str) -> otel_metrics.Gauge:
    gauge = _gauges.get(stat)
    if gauge is None:
        gauge = _meter.create_gauge(name=stat)
        _gauges[stat] = gauge
    return gauge


def _get_histogram(stat: str) -> otel_metrics.Histogram:
    histogram = _histograms.get(stat)
    if histogram is None:
        histogram = _meter.create_histogram(name=stat, unit="ms")
        _histograms[stat] = histogram
    return histogram


def prepare_stat_with_tags(fn: T) -> T:
    """Add tags to stat with influxdb standard format if influxdb_tags_enabled is True.

    Kept for parity with the previous StatsD-wire-protocol behavior for any code that
    still relies on the influxdb-style suffixed stat name (e.g. for allow/block list
    matching against the composed name). The resulting attributes dict is passed through
    unchanged to the OTel API as attributes.
    """

    @wraps(fn)
    def wrapper(
        self, stat: str | None = None, *args, tags: dict[str, str] | None = None, **kwargs
    ) -> Callable[[str], str]:
        if self.influxdb_tags_enabled:
            if stat is not None and tags is not None:
                for k, v in tags.items():
                    if self.metric_tags_validator.test(k):
                        v_str = "true" if v == "" else v
                        if all(c not in [",", "="] for c in f"{v_str}{k}"):
                            stat += f",{k}={v_str}"
                        else:
                            log.error("Dropping invalid tag: %s=%s.", k, v)
        return fn(self, stat, *args, tags=tags, **kwargs)

    return cast("T", wrapper)


class OtelCompatStatsdLogger:
    """StatsD-shaped logger whose calls emit OpenTelemetry metrics under the same names."""

    def __init__(
        self,
        prefix: str | None = None,
        metrics_validator: ListValidator | None = None,
        influxdb_tags_enabled: bool = False,
        metric_tags_validator: ListValidator | None = None,
        stat_name_handler: Callable[[str], str] | None = None,
        statsd_influxdb_enabled: bool = False,
    ) -> None:
        self.prefix = prefix
        self.metrics_validator = metrics_validator or PatternAllowListValidator()
        self.influxdb_tags_enabled = influxdb_tags_enabled
        self.metric_tags_validator = metric_tags_validator or PatternBlockListValidator()
        self.stat_name_handler = stat_name_handler
        self.statsd_influxdb_enabled = statsd_influxdb_enabled

    def _full_stat(self, stat: str) -> str:
        # Reproduce the vendor StatsClient's `prefix.stat` concatenation so the final
        # metric name on the wire/dashboard is unchanged.
        if self.prefix:
            return f"{self.prefix}.{stat}"
        return stat

    @prepare_stat_with_tags
    @validate_stat
    def incr(
        self,
        stat: str,
        count: int = 1,
        rate: float = 1,
        *,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Increment stat."""
        if self.metrics_validator.test(stat):
            if rate < 1 and __import__("random").random() > rate:
                return None
            updown = stat in UP_DOWN_STATS
            if count < 0 and not updown:
                # A negative add on a monotonic Counter is rejected by the OTel API
                # (same outcome as the repo's own otel_logger backend).
                log.warning(
                    "Dropping negative incr(%s, %s): stat is a monotonic Counter; "
                    "add it to UP_DOWN_STATS if it should net with decrements.",
                    stat,
                    count,
                )
                return None
            _get_counter(self._full_stat(stat), updown).add(count, attributes=tags or {})
        return None

    @prepare_stat_with_tags
    @validate_stat
    def decr(
        self,
        stat: str,
        count: int = 1,
        rate: float = 1,
        *,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Decrement stat."""
        if self.metrics_validator.test(stat):
            if rate < 1 and __import__("random").random() > rate:
                return None
            if stat not in UP_DOWN_STATS:
                # Routing this through a second instrument type under the same name
                # would split incr/decr contributions that can never net. Surface it
                # loudly instead: the fix is one line in UP_DOWN_STATS.
                log.warning(
                    "Dropping decr(%s): stat is not in UP_DOWN_STATS, so its incr side "
                    "uses a monotonic Counter; add it to UP_DOWN_STATS to net properly.",
                    stat,
                )
                return None
            _get_counter(self._full_stat(stat), True).add(-count, attributes=tags or {})
        return None

    @prepare_stat_with_tags
    @validate_stat
    def gauge(
        self,
        stat: str,
        value: int | float,
        rate: float = 1,
        delta: bool = False,
        *,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Gauge stat."""
        if self.metrics_validator.test(stat):
            full_stat = self._full_stat(stat)
            attrs = tags or {}
            key = (full_stat, tuple(sorted(attrs.items())))
            if delta:
                new_value = _gauge_last_values.get(key, 0) + value
            else:
                new_value = value
            _gauge_last_values[key] = new_value
            _get_gauge(full_stat).set(new_value, attributes=attrs)
        return None

    @prepare_stat_with_tags
    @validate_stat
    def timing(
        self,
        stat: str,
        dt: DeltaType,
        *,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Stats timing."""
        if self.metrics_validator.test(stat):
            full_stat = self._full_stat(stat)
            # Vendor-parity units: the statsd client treats numeric input as
            # milliseconds already and converts timedelta to ms — the identical
            # branch lives in the repo's own otel_logger.timing.
            if isinstance(dt, datetime.timedelta):
                value_ms = dt.total_seconds() * 1000.0
            else:
                value_ms = float(dt)
            _get_histogram(full_stat).record(value_ms, attributes=tags or {})
        return None

    @prepare_stat_with_tags
    @validate_stat
    def timer(
        self,
        stat: str | None = None,
        *args,
        tags: dict[str, str] | None = None,
        **kwargs,
    ) -> Timer:
        """Timer metric that can be cancelled."""
        if stat and self.metrics_validator.test(stat):
            return Timer(_OtelTimerContext(self, stat, tags or {}))
        return Timer()


class _OtelTimerContext:
    """Minimal context-manager/handle compatible with the vendor `statsd.Timer` object.

    Records elapsed wall-clock time (ms) into the same histogram used by `timing()`
    when `stop()` is called or the context manager exits, mirroring `statsd.Timer`
    semantics (including cancellation by simply not calling stop/never entering).
    """

    def __init__(self, logger: OtelCompatStatsdLogger, stat: str, tags: dict[str, str]) -> None:
        self._logger = logger
        self._stat = stat
        self._tags = tags
        self._start: float | None = None

    def start(self) -> _OtelTimerContext:
        self._start = time.monotonic()
        return self

    def stop(self, send: bool = True) -> None:
        if self._start is None:
            return
        elapsed_ms = (time.monotonic() - self._start) * 1000
        self._start = None
        if send:
            full_stat = self._logger._full_stat(self._stat)
            _get_histogram(full_stat).record(elapsed_ms, attributes=self._tags)

    def __enter__(self) -> _OtelTimerContext:
        return self.start()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()


def get_otel_compat_statsd_logger(
    *,
    prefix: str | None = None,
    influxdb_tags_enabled: bool = False,
    statsd_disabled_tags: str | None = None,
    metrics_allow_list: str | None = None,
    metrics_block_list: str | None = None,
    stat_name_handler: Callable[[str], str] | None = None,
    statsd_influxdb_enabled: bool = False,
) -> OtelCompatStatsdLogger:
    """Return a StatsD-shaped logger that emits metrics via OpenTelemetry."""
    metric_tags_validator = PatternBlockListValidator(statsd_disabled_tags)
    validator = get_validator(metrics_allow_list, metrics_block_list)
    return OtelCompatStatsdLogger(
        prefix,
        validator,
        influxdb_tags_enabled,
        metric_tags_validator,
        stat_name_handler,
        statsd_influxdb_enabled,
    )
