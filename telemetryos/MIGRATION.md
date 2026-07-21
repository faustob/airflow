# TelemetryOS migration — StatsD clients (generic) → OpenTelemetry

Base: 5520ca7. Metric names preserved verbatim (dashboard continuity).

| metric | instrument | file |
|---|---|---|
| `<app-defined stat name, verbatim as passed to Stats.incr()>` | counter | `airflow-core/src/airflow/stats.py` |
| `<app-defined stat name, verbatim as passed to Stats.decr()>` | updown | `airflow-core/src/airflow/stats.py` |
| `<app-defined stat name, verbatim as passed to Stats.gauge()>` | gauge | `airflow-core/src/airflow/stats.py` |
| `<app-defined stat name, verbatim as passed to Stats.timing()>` | histogram | `airflow-core/src/airflow/stats.py` |

TelemetryOS manages this instrumentation from here: arrival monitoring by the names above,
per-service signals, and SLI suggestions on the Signals tab.