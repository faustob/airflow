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

from __future__ import annotations

import logging
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

# Stats whose value can legitimately decrease (mirrors the in-repo otel_logger.py precedent).
UP_DOWN_COUNTERS = {"airflow.dag_processing.processes"}

_meter = otel_metrics.get_meter(__name__)


class _InstrumentRegistry:
    """Lazily creates and caches OTel instruments per stat name (module-scope singletons)."""

    def __init__(self) -> None:
        self._counters: dict[str, otel_metrics.Counter] = {}
        self._updown_counters: dict[str, otel_metrics.UpDownCounter] = {}
        self._gauges: dict[str, object] = {}
        self._histograms: dict[str, otel_metrics.Histogram] = {}

    def counter(self, stat: str) -> otel_metrics.Counter:
        instrument = self._counters.get(stat)
        if instrument is None:
            instrument = _meter.create_counter(stat)
            self._counters[stat] = instrument
        return instrument

    def up_down_counter(self, stat: str) -> otel_metrics.UpDownCounter:
        instrument = self._updown_counters.get(stat)
        if instrument is None:
            instrument = _meter.create_up_down_counter(stat)
            self._updown_counters[stat] = instrument
        return instrument

    def gauge(self, stat: str):
        instrument = self._gauges.get(stat)
        if instrument is None:
            create_gauge = getattr(_meter, "create_gauge", None)
            if create_gauge is not None:
                instrument = create_gauge(stat)
            else:
                # Fallback for OTel versions without synchronous gauge support:
                # keep the last observed value and expose it via an ObservableGauge.
                state: dict[str, float] = {}

                def _callback(options, _state=state):
                    if "value" in _state:
                        yield otel_metrics.Observation(_state["value"], dict(_state.get("attributes", {})))

                instrument = _meter.create_observable_gauge(stat, callbacks=[_callback])
                instrument._state = state  # type: ignore[attr-defined]
            self._gauges[stat] = instrument
        return instrument

    def histogram(self, stat: str) -> otel_metrics.Histogram:
        instrument = self._histograms.get(stat)
        if instrument is None:
            instrument = _meter.create_histogram(stat)
            self._histograms[stat] = instrument
        return instrument


_instruments = _InstrumentRegistry()


def _record_gauge(stat: str, value: int | float, delta: bool, attributes: dict[str, str] | None) -> None:
    instrument = _instruments.gauge(stat)
    set_value = getattr(instrument, "set", None)
    if set_value is not None:
        if delta:
            state = getattr(instrument, "_last_value", {})
            prev = state.get(stat, 0)
            value = prev + value
            state[stat] = value
            instrument._last_value = state  # type: ignore[attr-defined]
        set_value(value, attributes or {})
    else:
        # ObservableGauge fallback: mutate the state the callback reads.
        state = instrument._state  # type: ignore[attr-defined]
        if delta:
            value = state.get("value", 0) + value
        state["value"] = value
        state["attributes"] = attributes or {}


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


class SafeStatsdLogger:
    """StatsD-compatible logger, backed by the OpenTelemetry Metrics API.

    Preserves the original StatsD semantics: metric names are kept VERBATIM
    (including the caller-supplied prefix baked into ``stat``), incr/decr
    address the same underlying stat via signed adds, gauge keeps absolute-set
    and delta-add behavior, and timing/timer map to histograms recording
    milliseconds (matching the original StatsD wire units).
    """

    def __init__(
        self,
        statsd_client: None = None,
        metrics_validator: ListValidator | None = None,
        influxdb_tags_enabled: bool = False,
        metric_tags_validator: ListValidator | None = None,
        stat_name_handler: Callable[[str], str] | None = None,
        statsd_influxdb_enabled: bool = False,
    ) -> None:
        # statsd_client is accepted for backwards-compat call-site signatures but
        # is no longer used: all recording goes through OTel instruments.
        self.statsd = statsd_client
        self.metrics_validator = metrics_validator or PatternAllowListValidator()
        self.influxdb_tags_enabled = influxdb_tags_enabled
        self.metric_tags_validator = metric_tags_validator or PatternAllowListValidator()
        self.stat_name_handler = stat_name_handler
        self.statsd_influxdb_enabled = statsd_influxdb_enabled

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
        """Increment stat. Sample rate has no OTel equivalent and is dropped."""
        if self.metrics_validator.test(stat):
            if stat in UP_DOWN_COUNTERS:
                _instruments.up_down_counter(stat).add(count, tags or {})
            else:
                _instruments.counter(stat).add(count, tags or {})
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
        """Decrement stat. Sample rate has no OTel equivalent and is dropped."""
        if self.metrics_validator.test(stat):
            if stat in UP_DOWN_COUNTERS:
                _instruments.up_down_counter(stat).add(-count, tags or {})
            else:
                # Non-bidirectional counters are monotonic; a decrement on one of
                # these stats still needs to be recorded via an up/down counter
                # to avoid violating Counter's non-negative `add` contract.
                _instruments.up_down_counter(stat).add(-count, tags or {})
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
        """Gauge stat. Sample rate has no OTel equivalent and is dropped."""
        if self.metrics_validator.test(stat):
            _record_gauge(stat, value, delta, tags)
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
        """Stats timing. ``dt`` numeric values are already milliseconds;
        timedeltas are converted via total_seconds()*1000."""
        if self.metrics_validator.test(stat):
            millis = dt.total_seconds() * 1000.0 if hasattr(dt, "total_seconds") else dt
            _instruments.histogram(stat).record(millis, tags or {})
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
            histogram = _instruments.histogram(stat)

            class _OtelTimerHandle:
                def start(self):
                    return self

                def stop(self, send=True):
                    return self

                def __enter__(self):
                    import time

                    self._start = time.perf_counter()
                    return self

                def __exit__(self, exc_type, exc_val, exc_tb):
                    import time

                    elapsed_ms = (time.perf_counter() - self._start) * 1000.0
                    histogram.record(elapsed_ms, tags or {})
                    return False

            return Timer(_OtelTimerHandle())
        return Timer()


def get_statsd_logger(
    *,
    stats_class: type | None = None,
    host: str | None = None,
    port: int | None = None,
    prefix: str | None = None,
    ipv6: bool = False,
    influxdb_tags_enabled: bool = False,
    statsd_disabled_tags: str | None = None,
    metrics_allow_list: str | None = None,
    metrics_block_list: str | None = None,
    stat_name_handler: Callable[[str], str] | None = None,
    statsd_influxdb_enabled: bool = False,
) -> SafeStatsdLogger:
    """Return an OTel-backed logger with StatsD-compatible semantics.

    ``stats_class``/``host``/``port``/``ipv6`` are retained for call-site
    compatibility but are no longer used to construct a network client; all
    recording is routed through the global OTel MeterProvider. ``prefix`` must
    still be baked into ``stat`` by the caller before calling these methods,
    preserving verbatim metric names for dashboards.
    """
    metric_tags_validator = PatternBlockListValidator(statsd_disabled_tags)
    validator = get_validator(metrics_allow_list, metrics_block_list)
    return SafeStatsdLogger(
        None,
        validator,
        influxdb_tags_enabled,
        metric_tags_validator,
        stat_name_handler,
        statsd_influxdb_enabled,
    )
