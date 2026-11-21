# OCI self-scrape observability (first foot)

Decision (operator/GPT round): Prometheus lives on **OCI**, Grafana is the
external `grafana.aitestbed.kr`, connection will use the existing
cloudflared pattern with an Access service-token header.

This round installs the OCI self-scrape only:

- `prometheus` 3.14.0 (linux-arm64) — `--web.listen-address=127.0.0.1:9090`,
  30d retention, external label `machine_id: oci`
- `node_exporter` 1.12.1 — `127.0.0.1:9100`
- Both loopback-only; ports are NOT publicly opened.

## Planned tunnel hostname (documentation only — not created this round)

    prom-oci.robinco.dev  ->  http://127.0.0.1:9090   (CF Access service-token gated)

No secrets, no machine map, no tunnel credentials belong in this repository.

## Label rules

- `machine_id` is the global machine identity (never hostname, never herdr
  names). Metrics must never carry account numbers, order payloads, or tokens.

## Next feet (not this round)

- NCP / mac / rpi node_exporter on their next deployment feet.
- broker-edge process `/metrics` (CPU/RSS, request counts only) when a
  deployable unit exists.
