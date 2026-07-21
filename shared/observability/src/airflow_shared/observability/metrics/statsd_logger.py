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
import threading
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

# UP_DOWN_COUNTERS mirrors the in-repo OTel precedent (otel_logger.py): only these
# stats are bidirectional (can decrement); all other counters are monotonic.
UP_DOWN_COUNTERS: set[str] = {"airflow.dag_processing.processes"}

_meter = otel_metrics.get_meter(__name__)
_instrument_lock = threading.Lock()
_counters: dict[str, object] = {}
_updown_counters: dict[str, object] = {}
_gauges: dict[str, object] = {}
_histograms: dict[str, object] = {}
# Stateful gauge values, written by gauge() and read by the ObservableGauge callbacks.
_gauge_values: dict[str, float] = {}


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


def _get_counter(stat: str):
    with _instrument_lock:
        instrument = _counters.get(stat)
        if instrument is None:
            instrument = _meter.create_counter(stat)
            _counters[stat] = instrument
        return instrument


def _get_updown_counter(stat: str):
    with _instrument_lock:
        instrument = _updown_counters.get(stat)
        if instrument is None:
            instrument = _meter.create_up_down_counter(stat)
            _updown_counters[stat] = instrument
        return instrument


def _get_histogram(stat: str):
    with _instrument_lock:
        instrument = _histograms.get(stat)
        if instrument is None:
            instrument = _meter.create_histogram(stat, unit="ms")
            _histograms[stat] = instrument
        return instrument


def _gauge_callback(stat: str):
    def _callback(options):
        value = _gauge_values.get(stat)
        if value is None:
            return []
        return [otel_metrics.Observation(value)]

    return _callback


def _get_gauge(stat: str, value: int | float):
    with _instrument_lock:
        _gauge_values[stat] = value
        instrument = _gauges.get(stat)
        if instrument is None:
            if hasattr(_meter, "create_gauge"):
                instrument = _meter.create_gauge(stat)
            else:
                instrument = _meter.create_observable_gauge(stat, callbacks=[_gauge_callback(stat)])
            _gauges[stat] = instrument
        return instrument


class SafeStatsdLogger:
    """StatsD-compatible Logger backed by OpenTelemetry metrics."""

    def __init__(
        self,
        statsd_client=None,
        metrics_validator: ListValidator | None = None,
        influxdb_tags_enabled: bool = False,
        metric_tags_validator: ListValidator | None = None,
        stat_name_handler: Callable[[str], str] | None = None,
        statsd_influxdb_enabled: bool = False,
    ) -> None:
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
        """Increment stat."""
        if self.metrics_validator.test(stat):
            attributes = tags or {}
            if stat in UP_DOWN_COUNTERS:
                _get_updown_counter(stat).add(count, attributes=attributes)
            else:
                _get_counter(stat).add(count, attributes=attributes)
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
            attributes = tags or {}
            # decr always subtracts, so the underlying stat is inherently bidirectional here.
            _get_updown_counter(stat).add(-count, attributes=attributes)
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
            with _instrument_lock:
                if delta:
                    new_value = _gauge_values.get(stat, 0) + value
                else:
                    new_value = value
            attributes = tags or {}
            instrument = _get_gauge(stat, new_value)
            if hasattr(instrument, "set"):
                instrument.set(new_value, attributes=attributes)
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
            # dt may be a numeric value already in milliseconds, or a timedelta;
            # timedelta converts via total_seconds()*1000 to keep the same unit.
            if hasattr(dt, "total_seconds"):
                value_ms = dt.total_seconds() * 1000
            else:
                value_ms = dt
            attributes = tags or {}
            _get_histogram(stat).record(value_ms, attributes=attributes)
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
            histogram = _get_histogram(stat)
            attributes = tags or {}

            class _OtelTimer:
                def __enter__(self):
                    import time

                    self._start = time.monotonic()
                    return self

                def __exit__(self, exc_type, exc_val, exc_tb):
                    self.stop()
                    return False

                def start(self):
                    import time

                    self._start = time.monotonic()
                    return self

                def stop(self, send=True):
                    import time

                    if send and getattr(self, "_start", None) is not None:
                        elapsed_ms = (time.monotonic() - self._start) * 1000
                        histogram.record(elapsed_ms, attributes=attributes)

            return Timer(_OtelTimer())
        return Timer()


def get_statsd_logger(
    *,
    stats_class=None,
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
    """Return logger backed by OpenTelemetry metrics (kept for API/config compatibility)."""
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
