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
import time
from collections.abc import Callable
from functools import wraps
from threading import Lock
from typing import TYPE_CHECKING, TypeVar, cast

from opentelemetry import metrics as otel_metrics

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

# Module-scope meter and instrument caches so a given stat name maps to exactly
# one OTel instrument for the lifetime of the process, regardless of how many
# _OtelStatsClient/SafeStatsdLogger instances are created.
_otel_meter = otel_metrics.get_meter(__name__)
_instrument_lock = Lock()
_counters: dict[str, object] = {}
_updown_counters: dict[str, object] = {}
_gauges: dict[str, object] = {}
_histograms: dict[str, object] = {}


def _get_counter(stat: str):
    with _instrument_lock:
        instrument = _counters.get(stat)
        if instrument is None:
            instrument = _otel_meter.create_counter(stat)
            _counters[stat] = instrument
        return instrument


def _get_updown_counter(stat: str):
    with _instrument_lock:
        instrument = _updown_counters.get(stat)
        if instrument is None:
            instrument = _otel_meter.create_up_down_counter(stat)
            _updown_counters[stat] = instrument
        return instrument


def _get_gauge(stat: str):
    with _instrument_lock:
        instrument = _gauges.get(stat)
        if instrument is None:
            # Synchronous gauge, available since OTel 1.27 - matches StatsD's
            # set-style gauge semantics (immediate emission on .set()).
            instrument = _otel_meter.create_gauge(stat)
            _gauges[stat] = instrument
        return instrument


def _get_histogram(stat: str):
    with _instrument_lock:
        instrument = _histograms.get(stat)
        if instrument is None:
            instrument = _otel_meter.create_histogram(stat)
            _histograms[stat] = instrument
        return instrument


class _OtelTimer:
    """Timer-protocol-compatible context manager/decorator that records to an OTel histogram."""

    def __init__(self, stat: str | None = None, tags: dict[str, str] | None = None) -> None:
        self.stat = stat
        self.tags = tags
        self.start_time: float | None = None
        self.duration: float | None = None

    def start(self) -> _OtelTimer:
        self.start_time = time.perf_counter()
        return self

    def stop(self, send: bool = True) -> None:
        if self.start_time is None:
            return
        self.duration = time.perf_counter() - self.start_time
        if send and self.stat:
            _get_histogram(self.stat).record(self.duration * 1000.0, attributes=self.tags or {})

    def __enter__(self) -> _OtelTimer:
        return self.start()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()

    def __call__(self, fn: T) -> T:
        @wraps(fn)
        def wrapper(*args, **kwargs):
            with self.__class__(self.stat, self.tags):
                return fn(*args, **kwargs)

        return cast("T", wrapper)


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
    """StatsD Logger."""

    def __init__(
        self,
        statsd_client: StatsClient,
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
            return self.statsd.incr(stat, count, rate)
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
            return self.statsd.decr(stat, count, rate)
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
            return self.statsd.gauge(stat, value, rate, delta)
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
            return self.statsd.timing(stat, dt)
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
            return Timer(self.statsd.timer(stat, *args, **kwargs))
        return Timer()


def get_statsd_logger(
    *,
    stats_class: type[StatsClient],
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
    """Return logger for StatsD."""
    statsd = stats_class(
        host=host,
        port=port,
        prefix=prefix,
        ipv6=ipv6,
    )

    metric_tags_validator = PatternBlockListValidator(statsd_disabled_tags)
    validator = get_validator(metrics_allow_list, metrics_block_list)
    return SafeStatsdLogger(
        statsd,
        validator,
        influxdb_tags_enabled,
        metric_tags_validator,
        stat_name_handler,
        statsd_influxdb_enabled,
    )
