# TelemetryOS migration — StatsD clients (generic) → OpenTelemetry

Base: 5520ca7. Metric names preserved verbatim (dashboard continuity).

| metric | instrument | file |
|---|---|---|
| `<app-defined stat via SafeStatsdLogger.incr, prefix + stat name>` | counter | `shared/observability/src/airflow_shared/observability/metrics/otel_statsd_bridge.py` |
| `<app-defined stat via SafeStatsdLogger.decr, prefix + stat name>` | updown | `shared/observability/src/airflow_shared/observability/metrics/otel_statsd_bridge.py` |
| `<app-defined stat via SafeStatsdLogger.gauge, prefix + stat name>` | updown | `shared/observability/src/airflow_shared/observability/metrics/otel_statsd_bridge.py` |
| `<app-defined stat via SafeStatsdLogger.timing/timer, prefix + stat name>` | histogram | `shared/observability/src/airflow_shared/observability/metrics/otel_statsd_bridge.py` |

TelemetryOS manages this instrumentation from here: arrival monitoring by the names above,
per-service signals, and SLI suggestions on the Signals tab.