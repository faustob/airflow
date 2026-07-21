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
"""OpenTelemetry-backed replacement for the StatsD-client based `SafeStatsdLogger`.

This module preserves the exact call signatures (`incr`, `decr`, `gauge`, `timing`,
`timer`) and the metric-name/tag semantics (including the InfluxDB-style tag
suffix format and allow/block-list validation) previously provided by the
`statsd`-package client, but records values through OpenTelemetry instruments
instead of sending StatsD UDP packets. All metric names remain byte-identical
to what the application configured (prefix + stat name).
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from functools import wraps
from typing import TYPE_CHECKING, TypeVar, cast

from opentelemetry import metrics as otel_metrics

from .validators import (
    PatternAllowListValidator,
    PatternBlockListValidator,
    get_validator,
    validate_stat,
)

if TYPE_CHECKING:
    from opentelemetry.metrics import Counter, Histogram, UpDownCounter

    from .protocols import DeltaType
    from .validators import ListValidator

T = TypeVar("T", bound=Callable)

log = logging.getLogger(__name__)

_meter = otel_metrics.get_meter("airflow.statsd_bridge")

# Instrument registries keyed by final (prefixed) stat name, created once per name.
_counters: dict[str, Counter] = {}
_updown_counters: dict[str, UpDownCounter] = {}
_gauge_updowns: dict[str, UpDownCounter] = {}
_gauge_last_value: dict[str, float] = {}
_histograms: dict[str, Histogram] = {}


def _get_counter(stat: str) -> Counter:
    inst = _counters.get(stat)
    if inst is None:
        inst = _meter.create_counter(name=stat)
        _counters[stat] = inst
    return inst


def _get_decr_counter(stat: str) -> UpDownCounter:
    # decr can only ever push the value down from the caller's perspective, but the
    # underlying series must support arbitrary (positive) additions from incr() too,
    # so any stat that receives a decr() call is modeled as an UpDownCounter.
    inst = _updown_counters.get(stat)
    if inst is None:
        inst = _meter.create_up_down_counter(name=stat)
        _updown_counters[stat] = inst
    return inst


def _get_gauge_updown(stat: str) -> UpDownCounter:
    inst = _gauge_updowns.get(stat)
    if inst is None:
        inst = _meter.create_up_down_counter(name=stat)
        _gauge_updowns[stat] = inst
    return inst


def _get_histogram(stat: str) -> Histogram:
    inst = _histograms.get(stat)
    if inst is None:
        inst = _meter.create_histogram(name=stat)
        _histograms[stat] = inst
    return inst


def prepare_stat_with_tags(fn: T) -> T:
    """Add tags to stat with influxdb standard format if influxdb_tags_enabled is True."""

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


class _OtelTimer:
    """Timer metric that can be cancelled, backed by an OTel histogram."""

    def __init__(self, histogram: Histogram | None = None, attributes: dict[str, str] | None = None):
        self._histogram = histogram
        self._attributes = attributes or {}
        self._start: float | None = None
        self._cancelled = False

    def __enter__(self) -> _OtelTimer:
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if not self._cancelled:
            self.stop()

    def start(self) -> _OtelTimer:
        self._start = time.perf_counter()
        return self

    def stop(self, send: bool = True) -> None:
        if self._start is None:
            return
        elapsed_ms = (time.perf_counter() - self._start) * 1000.0
        if send and self._histogram is not None:
            self._histogram.record(elapsed_ms, attributes=self._attributes)
        self._start = None

    def cancel(self) -> None:
        self._cancelled = True
        self._start = None


class OtelStatsdBridgeLogger:
    """StatsD-API-compatible logger that records through OpenTelemetry instruments."""

    def __init__(
        self,
        prefix: str | None = None,
        metrics_validator: ListValidator | None = None,
        influxdb_tags_enabled: bool = False,
        metric_tags_validator: ListValidator | None = None,
        stat_name_handler: Callable[[str], str] | None = None,
    ) -> None:
        self.prefix = prefix
        self.metrics_validator = metrics_validator or PatternAllowListValidator()
        self.influxdb_tags_enabled = influxdb_tags_enabled
        self.metric_tags_validator = metric_tags_validator or PatternBlockListValidator()
        self.stat_name_handler = stat_name_handler

    def _full_name(self, stat: str) -> str:
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
            name = self._full_name(stat)
            _get_counter(name).add(count, attributes=tags or {})
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
            name = self._full_name(stat)
            _get_decr_counter(name).add(-count, attributes=tags or {})
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
            name = self._full_name(stat)
            updown = _get_gauge_updown(name)
            if delta:
                updown.add(value, attributes=tags or {})
                _gauge_last_value[name] = _gauge_last_value.get(name, 0.0) + value
            else:
                previous = _gauge_last_value.get(name, 0.0)
                diff = value - previous
                if diff != 0:
                    updown.add(diff, attributes=tags or {})
                _gauge_last_value[name] = value
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
            name = self._full_name(stat)
            if hasattr(dt, "total_seconds"):
                value_ms = dt.total_seconds() * 1000.0
            else:
                value_ms = float(dt)
            _get_histogram(name).record(value_ms, attributes=tags or {})
        return None

    @prepare_stat_with_tags
    @validate_stat
    def timer(
        self,
        stat: str | None = None,
        *args,
        tags: dict[str, str] | None = None,
        **kwargs,
    ) -> _OtelTimer:
        """Timer metric that can be cancelled."""
        if stat and self.metrics_validator.test(stat):
            name = self._full_name(stat)
            return _OtelTimer(_get_histogram(name), attributes=tags or {})
        return _OtelTimer()


def get_otel_statsd_bridge_logger(
    *,
    prefix: str | None = None,
    influxdb_tags_enabled: bool = False,
    statsd_disabled_tags: str | None = None,
    metrics_allow_list: str | None = None,
    metrics_block_list: str | None = None,
    stat_name_handler: Callable[[str], str] | None = None,
) -> OtelStatsdBridgeLogger:
    """Return an OTel-backed logger that is API-compatible with the former StatsD logger."""
    metric_tags_validator = PatternBlockListValidator(statsd_disabled_tags)
    validator = get_validator(metrics_allow_list, metrics_block_list)
    return OtelStatsdBridgeLogger(
        prefix,
        validator,
        influxdb_tags_enabled,
        metric_tags_validator,
        stat_name_handler,
    )
