# TelemetryOS migration — StatsD clients (generic) → OpenTelemetry

Base: 5520ca7. Metric names preserved verbatim (dashboard continuity).

| metric | instrument | file |
|---|---|---|

TelemetryOS manages this instrumentation from here: arrival monitoring by the names above,
per-service signals, and SLI suggestions on the Signals tab.