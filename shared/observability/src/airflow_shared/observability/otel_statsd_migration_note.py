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
"""Migration note.

The StatsD client (`statsd` PyPI package / `statsd.StatsClient`) previously used
for emitting Airflow's runtime metrics has been removed from this project's
dependency manifests (task-sdk, airflow-core, shared/observability, and the
top-level `apache-airflow[all]` aggregate extra).

No application source files matching the StatsD instrumentation API
(`statsd.StatsClient`, `.incr()`, `.decr()`, `.timing()`, `.gauge()`, or the
node-style `node-statsd`/`statsd` JS clients) were present in the files
supplied for this migration -- only build-manifest (`pyproject.toml`) files
referencing the `statsd` optional-dependency extras were found. Airflow's own
metrics-emission call sites already route through Airflow's internal
`airflow.stats.Stats` facade (not shown here), which selects a backend based
on configuration (`[metrics] statsd_on`); with the `statsd` extra removed,
that facade's StatsD backend can no longer be installed, and OpenTelemetry
(already wired via the `otel` extras seen in these manifests, using
`opentelemetry-api`, `opentelemetry-exporter-otlp`, and
`opentelemetry-exporter-prometheus`) is the supported metrics path going
forward.

If/when the actual `airflow.stats` StatsD-backend source file(s) are provided
in a follow-up pass, the vendor `statsd.StatsClient` calls
(`incr`/`decr`/`timing`/`gauge`) in that file must be translated in place to
`opentelemetry.metrics` instruments (`Counter`/`UpDownCounter`/`Histogram`/
`ObservableGauge`) obtained from the global `MeterProvider`, preserving every
metric name verbatim, exactly as required by the migration contract.
"""
