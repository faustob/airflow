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

_otel_meter = otel_metrics.get_meter(__name__)

_instrument_lock = threading.Lock()
_counters: dict[str, object] = {}
_updown_counters: dict[str, object] = {}
_gauges: dict[str, object] = {}
_histograms: dict[str, object] = {}
_gauge_state: dict[str, float] = {}


def _get_counter(name: str):
    with _instrument_lock:
        instr = _counters.get(name)
        if instr is None:
            instr = _otel_meter.create_counter(name)
            _counters[name] = instr
        return instr


def _get_updown_counter(name: str):
    with _instrument_lock:
        instr = _updown_counters.get(name)
        if instr is None:
            instr = _otel_meter.create_up_down_counter(name)
            _updown_counters[name] = instr
        return instr


def _gauge_callback(name: str):
    def _callback(options):
        value = _gauge_state.get(name)
        if value is None:
            return []
        return [otel_metrics.Observation(value, {})]

    return _callback


def _get_gauge(name: str):
    with _instrument_lock:
        instr = _gauges.get(name)
        if instr is None:
            instr = _otel_meter.create_observable_gauge(name, callbacks=[_gauge_callback(name)])
            _gauges[name] = instr
        return instr


def _get_histogram(name: str):
    with _instrument_lock:
        instr = _histograms.get(name)
        if instr is None:
            instr = _otel_meter.create_histogram(name)
            _histograms[name] = instr
        return instr


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
    """StatsD-compatible logger backed by OpenTelemetry instruments."""

    def __init__(
        self,
        metrics_validator: ListValidator | None = None,
        influxdb_tags_enabled: bool = False,
        metric_tags_validator: ListValidator | None = None,
        stat_name_handler: Callable[[str], str] | None = None,
        statsd_influxdb_enabled: bool = False,
        prefix: str | None = None,
    ) -> None:
        self.metrics_validator = metrics_validator or PatternAllowListValidator()
        self.influxdb_tags_enabled = influxdb_tags_enabled
        self.metric_tags_validator = metric_tags_validator or PatternAllowListValidator()
        self.stat_name_handler = stat_name_handler
        self.statsd_influxdb_enabled = statsd_influxdb_enabled
        self.prefix = prefix

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
            _get_counter(self._full_name(stat)).add(count, attributes=tags or {})
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
            _get_updown_counter(self._full_name(stat)).add(-count, attributes=tags or {})
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
            full_name = self._full_name(stat)
            if delta:
                _gauge_state[full_name] = _gauge_state.get(full_name, 0) + value
            else:
                _gauge_state[full_name] = value
            _get_gauge(full_name)
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
            value = dt.total_seconds() * 1000 if hasattr(dt, "total_seconds") else dt
            _get_histogram(self._full_name(stat)).record(value, attributes=tags or {})
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
            full_name = self._full_name(stat)
            histogram = _get_histogram(full_name)

            class _OtelTimerCtx:
                def __enter__(self_inner):
                    import time

                    self_inner._start = time.monotonic()
                    return self_inner

                def __exit__(self_inner, exc_type, exc_val, exc_tb):
                    import time

                    elapsed_ms = (time.monotonic() - self_inner._start) * 1000
                    histogram.record(elapsed_ms, attributes=tags or {})
                    return False

                def start(self_inner):
                    return self_inner.__enter__()

                def stop(self_inner):
                    return self_inner.__exit__(None, None, None)

            return Timer(_OtelTimerCtx())
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
    """Return an OTel-backed logger with the same interface StatsD callers expect."""
    metric_tags_validator = PatternBlockListValidator(statsd_disabled_tags)
    validator = get_validator(metrics_allow_list, metrics_block_list)
    return SafeStatsdLogger(
        validator,
        influxdb_tags_enabled,
        metric_tags_validator,
        stat_name_handler,
        statsd_influxdb_enabled,
        prefix=prefix,
    )
