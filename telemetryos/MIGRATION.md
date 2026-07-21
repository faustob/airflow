# TelemetryOS migration — StatsD clients (generic) → OpenTelemetry

Base: 427ee90. Metric names preserved verbatim (dashboard continuity).

| metric | instrument | file |
|---|---|---|

## Activating the export

The translated code emits through the global OpenTelemetry meter. **Nothing is exported until the process initializes a MeterProvider with an exporter** — the legacy tool's config path does not do this. This repo already carries OTel dependencies, but verify the MIGRATED path actually initializes the SDK at startup (an existing init behind a different config flag does not cover it).

- Zero-code: run the process under `opentelemetry-instrument <entrypoint>` with `OTEL_SERVICE_NAME`, `OTEL_METRICS_EXPORTER=otlp`, and `OTEL_EXPORTER_OTLP_ENDPOINT`/`OTEL_EXPORTER_OTLP_HEADERS` set (deps: `opentelemetry-distro`, `opentelemetry-exporter-otlp`).
- In-code: initialize a `MeterProvider` with an OTLP metric reader at process start — before the first metric call — and install it via `opentelemetry.metrics.set_meter_provider(...)`.

TelemetryOS manages this instrumentation from here: arrival monitoring by the names above,
per-service signals, and SLI suggestions on the Signals tab.