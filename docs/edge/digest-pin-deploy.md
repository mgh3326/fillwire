# Digest-pinned pull deployment

`scripts/edge/deploy-pull.sh` resolves a requested GHCR tag to its immutable
`RepoDigest`, writes a systemd `EnvironmentFile`, restarts one unit, and checks
its Prometheus metrics endpoint. It is intended to run **on the target host**;
this repository contains no host environment files or credentials.

## One-time unit setup

Copy and adapt the appropriate example in
`deploy/edge/systemd/broker-edge.service.example` or
`deploy/edge/systemd/gatewayd.service.example`. The absolute `EnvironmentFile` and `--env-file`
paths are placeholders: choose the existing host locations and
keep populated environment files off this public repository. The unit's
`EnvironmentFile` must be the state file managed by the script, and its
`ExecStart` must use `${IMAGE}`.

For a unit named `broker-edge.service`, the default state files are:

```text
/var/lib/broker-edge/broker-edge.service.image
/var/lib/broker-edge/broker-edge.service.image.previous
```

The current file contains exactly one line, such as
`IMAGE=ghcr.io/mgh3326/broker-edge@sha256:...`; the previous file is retained
only after a successful deployment. Create the state directory with the
ownership required by the host's service setup.

## Deploy

Set `BROKER_EDGE_IMAGE_REPOSITORY` to the approved legacy repository.
The tag defaults to `main`. See the [serve cutover runbook](../serve-cutover.md)
for new Fillwire images.

```sh
sudo BROKER_EDGE_IMAGE_REPOSITORY=ghcr.io/mgh3326/broker-edge scripts/edge/deploy-pull.sh broker-edge.service
sudo BROKER_EDGE_IMAGE_REPOSITORY=ghcr.io/mgh3326/broker-edge BROKER_EDGE_HEALTH_PORT=8791 scripts/edge/deploy-pull.sh gatewayd.service main
```

The port above is gatewayd's default listen port (8791); use the port of your configured `GATEWAYD_LISTEN_ADDR`.

The health gate is `GET /metrics`, defaulting to
`http://127.0.0.1:8080/metrics`. Configure a different port with
`BROKER_EDGE_HEALTH_PORT`, or a complete endpoint with
`BROKER_EDGE_HEALTH_URL`. `BROKER_EDGE_HEALTH_ATTEMPTS` (default `10`) and
`BROKER_EDGE_HEALTH_INTERVAL` (default `1` second) control retries.

On a failed gate, the script restores both state files to their pre-deploy
contents, restarts the previous digest, and gates it again. A failure of that
recovery gate is reported explicitly for operator intervention.

## Roll back

To return immediately to the recorded prior digest:

```sh
sudo BROKER_EDGE_IMAGE_REPOSITORY=ghcr.io/mgh3326/broker-edge scripts/edge/deploy-pull.sh --rollback broker-edge.service
```

Rollback swaps current and previous pins, restarts, and health-checks the
prior image. If it cannot pass the gate, the script restores and rechecks the
former current image. It refuses to act if no valid previous pin exists.

Useful non-secret configuration variables are `BROKER_EDGE_STATE_DIR`,
`BROKER_EDGE_IMAGE_REPOSITORY`, and the health-gate variables above. Unit names
and tags are input-validated; do not edit the generated `.image` files by hand.
