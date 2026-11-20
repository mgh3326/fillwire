# Digest-pinned container deployment (fillwire)

fillwire runs on the target host as a Docker container under systemd, pinned to
an immutable GHCR digest — the same pattern as broker-edge
(`deploy/systemd/broker-edge.service.example`). This repository is public: it
contains no host names, addresses, accounts, or credential values. The paths
below are placeholders (`/etc/fillwire/...`, `/var/lib/fillwire`); pick the
host's real locations at install time and keep populated files off this repo.

Container contract (see `deploy/systemd/fillwire.service.example`):

- `docker run --rm --network host` — KIS websocket, Redis TLS, and the ingest
  POST all use the host network; nothing is published.
- Secrets arrive only through `--env-file` (`KIS_APP_KEY`, `KIS_APP_SECRET`,
  `EXECUTION_LEDGER_INGEST_TOKEN`, `FILLWIRE_ALERT_TELEGRAM_BOT_TOKEN`, and
  `FILLWIRE_ALERT_TELEGRAM_CHAT_ID`; the first three are referenced by name
  in the TOML). No secret
  is ever a build arg or an image layer.
- The TOML is bind-mounted read-only; the binary is invoked as
  `fillwire -config <in-container path>`. Its `[ingest]` section also carries
  `token_header` (default `X-Execution-Ledger-Ingest-Token`): the name under
  which the token is sent, verbatim. It must equal the API side's
  `EXECUTION_LEDGER_INGEST_TOKEN_HEADER` or every ingest POST is refused with
  401. A host TOML predating the key needs no edit — the default applies.
- The image reference is `${IMAGE}` from a systemd `EnvironmentFile` that
  holds exactly one line, `IMAGE=ghcr.io/mgh3326/fillwire@sha256:<64 hex>`.
  Tags are never deployed.

Pin-state files (the unit's `EnvironmentFile` points at
`/var/lib/fillwire/fillwire.service.image`; the rest of the directory is
the pin history):

```text
/var/lib/fillwire/fillwire.service.image            # current pin
/var/lib/fillwire/fillwire.service.image.previous   # most recent pre-deploy pin
/var/lib/fillwire/fillwire.service.image.<UTC>      # per-deploy history snapshot
```

A history snapshot is written before every pin change, so the directory
accumulates the full pin lineage — after deploys A→B→C the history holds A
and B, and any recorded digest can be restored, not just the latest.

## Finding the digest for a commit

CI publishes `ghcr.io/mgh3326/fillwire:sha-<commit-sha>` on every push to
`main`. The workflow's **Surface pushed digest** step prints
`Pushed image: ghcr.io/mgh3326/fillwire:sha-<commit-sha>@sha256:<digest>` and
writes the same pair into the run's `GITHUB_STEP_SUMMARY` ("GHCR image"
block), so the canonical digest is on the workflow run page — no workflow
change is needed.

To resolve it from the host instead:

```sh
docker buildx imagetools inspect ghcr.io/mgh3326/fillwire:sha-<commit-sha> --format '{{.Manifest.Digest}}'
```

or pull the tag first and read its RepoDigest:

```sh
docker pull ghcr.io/mgh3326/fillwire:sha-<commit-sha>
docker image inspect --format '{{index .RepoDigests 0}}' ghcr.io/mgh3326/fillwire:sha-<commit-sha>
```

## Deploy

All commands run on the target host. `<digest>` is the full
`sha256:<64 hex>` value found above.

1. Snapshot the current pin into the UTC-stamped history, refresh
   `.previous`, then write the new pin:

   ```sh
   sudo cp /var/lib/fillwire/fillwire.service.image "/var/lib/fillwire/fillwire.service.image.$(date -u +%Y%m%dT%H%M%SZ)"
   sudo cp /var/lib/fillwire/fillwire.service.image /var/lib/fillwire/fillwire.service.image.previous
   printf 'IMAGE=ghcr.io/mgh3326/fillwire@%s\n' '<digest>' | sudo tee /var/lib/fillwire/fillwire.service.image
   ```

2. Pull exactly the pinned image:

   ```sh
   sudo docker pull ghcr.io/mgh3326/fillwire@<digest>
   ```

3. Restart the unit:

   ```sh
   sudo systemctl restart fillwire.service
   ```

4. Verify — all three must hold:

   ```sh
   systemctl is-active fillwire.service
   # expected: active

   systemctl show fillwire.service --property=NRestarts
   # expected: NRestarts=0

   sudo journalctl -u fillwire.service -n 50 --no-pager
   # expected: a line containing "KIS websocket initial subscription active"
   ```

   If the journal instead shows exit code 42 (`ws: app key already holds a
   WebSocket session`), another process still holds the KIS app-key session —
   see "Alternating with python at-kis-ws" below. The unit will not
   restart-loop: exit 42 is in `RestartPreventExitStatus` and the
   `StartLimitIntervalSec`/`StartLimitBurst` bound caps any other loop.

## Roll back

Rollback is restoring an earlier pin from the history **together with the host
`fillwire.toml` that pin accepts**, and restarting. The pin is not the whole
state: the unit bind-mounts the same host `/etc/fillwire/fillwire.toml` into
whichever image runs. An image from before the gatewayd-fetch release rejects
`approval_mode = "gatewayd-fetch"` (exit 78, which `RestartPreventExitStatus`
keeps from restarting) and rejects `[quotes] token_source` in its strict
`[quotes]` decoder (the quote reader stays disabled while fills still start).

The history is the `/var/lib/fillwire/fillwire.service.image.<UTC>` files — one
per replaced pin — so you can return to any recorded digest, not only the one
in `.previous` (after deploys A→B→C, the original A is still on disk). Pins
recorded before this release have no paired TOML: before starting, identify the
TOML that older digest accepts (for example the pre-enablement copy described
below) and put it in the snapshot directory as `fillwire.toml.<stamp>`.

1. Snapshot the **running pin and the running TOML under one identifier** before
   replacing either file. This is what makes a failed rollback undoable. The
   TOML snapshot is root-only, because the file names credential variables:

   ```sh
   stamp=$(date -u +%Y%m%dT%H%M%SZ)   # keep this value: the undo block needs it
   sudo install -d -m 700 /var/lib/fillwire/config-snapshots
   sudo cp /var/lib/fillwire/fillwire.service.image "/var/lib/fillwire/fillwire.service.image.$stamp"
   sudo install -m 600 /etc/fillwire/fillwire.toml "/var/lib/fillwire/config-snapshots/fillwire.toml.$stamp"
   echo "$stamp"
   ```

   Likewise, before enabling `approval_mode = "gatewayd-fetch"` or `[quotes]
   token_source`, save the TOML the current image accepts as
   `/var/lib/fillwire/config-snapshots/fillwire.toml.<stamp of the pin that
   accepts it>`.

2. List the history and pick the pin to restore, and its compatible TOML:

   ```sh
   ls -t /var/lib/fillwire /var/lib/fillwire/config-snapshots
   cat /var/lib/fillwire/fillwire.service.image.<target-stamp>
   # confirm the single IMAGE= line shows the digest you want, and that
   # config-snapshots/fillwire.toml.<target-stamp> exists and is the file that
   # digest accepts
   ```

3. Restore **both**, pull, then restart:

   ```sh
   sudo cp /var/lib/fillwire/fillwire.service.image.<target-stamp> /var/lib/fillwire/fillwire.service.image
   sudo install -m 600 /var/lib/fillwire/config-snapshots/fillwire.toml.<target-stamp> /etc/fillwire/fillwire.toml
   sudo docker pull "$(cut -d= -f2 /var/lib/fillwire/fillwire.service.image)"
   sudo systemctl restart fillwire.service
   ```

4. Verify **this invocation only**. A plain `journalctl -n 50` can still show a
   line from the process before the restart, so scope the logs to the unit's
   new invocation:

   ```sh
   invocation_id=$(systemctl show -p InvocationID --value fillwire.service)
   test -n "$invocation_id" || echo "no invocation id: do not continue"
   sudo systemctl show -p ActiveState,NRestarts,InvocationID fillwire.service
   # expected: ActiveState=active, NRestarts=0, the same InvocationID after a minute
   sudo journalctl -u fillwire.service "_SYSTEMD_INVOCATION_ID=$invocation_id" --no-pager | grep -E 'KIS websocket initial subscription active|quote reader (started|disabled)|quote reader subscriptions active|fillwire stopped'
   ```

   Expected: this invocation's `KIS websocket initial subscription active`.
   If quotes are enabled in the restored TOML, also expect this invocation's
   `quote reader started`, and **no** `quote reader disabled`. `started` only
   shows the lane was launched, before it has a token or a subscription. During
   a trading window also require `quote reader subscriptions active` with the
   configured subscription count and zero rejected (README "Verifying"); outside
   the window the idle line verifies startup only, so confirm subscriptions at
   the next open.

### Undoing a failed rollback

If the rolled-back unit fails verification, restore the snapshot step 1 wrote,
**pin and TOML together, before restarting**. Use the `stamp` step 1 printed
(the newest history file is that snapshot):

```sh
ls -t /var/lib/fillwire /var/lib/fillwire/config-snapshots
sudo cp /var/lib/fillwire/fillwire.service.image.<stamp-from-step-1> /var/lib/fillwire/fillwire.service.image
sudo install -m 600 /var/lib/fillwire/config-snapshots/fillwire.toml.<stamp-from-step-1> /etc/fillwire/fillwire.toml
sudo docker pull "$(cut -d= -f2 /var/lib/fillwire/fillwire.service.image)"
sudo systemctl restart fillwire.service
```

Then repeat step 4 with the new invocation ID. If that also fails, escalate —
do not keep cycling pins.

## Alternating with python at-kis-ws

KIS allows **one websocket session per app key**. fillwire and the python
`at-kis-ws` process must never run at the same time; whoever subscribes second
is rejected with OPSP8996 (fillwire exits 42). Always stop the holder before
starting the other — never restart both and hope.

Switch from `at-kis-ws` to fillwire:

`at-kis-ws` is a Docker container with a restart policy of `unless-stopped`,
not a systemd unit, so `systemctl` does not control it. `docker stop` marks it
stopped, which keeps the restart policy from bringing it back.

```sh
sudo docker stop <at-kis-ws container>
sudo docker ps --format '{{.Names}}'
# expected: no at-kis-ws line — do not continue while the container is running
sudo systemctl start fillwire.service
sudo journalctl -u fillwire.service -n 50 --no-pager
# expected: "KIS websocket initial subscription active", no exit-42 line
```

Switch back from fillwire to `at-kis-ws`:

```sh
sudo systemctl stop fillwire.service
systemctl is-active fillwire.service
# expected: inactive (or failed)
sudo docker start <at-kis-ws container>
```

If fillwire is already down with exit 42, do not `systemctl restart` it — the
restart cannot succeed while the other process holds the session. Stop the
holder first.

## One-time transition: native unit to container

fillwire previously ran as a native binary (`/usr/local/bin/fillwire`) under a
native `fillwire.service`. Perform this once, in a deploy window, with
`at-kis-ws` stopped or otherwise not holding the session:

1. Stop and back up the native unit and binary:

   ```sh
   sudo systemctl stop fillwire.service
   sudo mkdir -p /root/fillwire-native-backup
   sudo cp /etc/systemd/system/fillwire.service /root/fillwire-native-backup/fillwire.service
   sudo cp /usr/local/bin/fillwire /root/fillwire-native-backup/fillwire
   ```

2. Disable the native unit so it cannot autostart beside the container:

   ```sh
   sudo systemctl disable fillwire.service
   ```

3. Install host files (contents are host-specific; nothing here comes from
   this repository verbatim): the env file at `/etc/fillwire/fillwire.env`,
   the adapted TOML at `/etc/fillwire/fillwire.toml`, the state directory, and
   the initial pin:

   ```sh
   sudo mkdir -p /etc/fillwire /var/lib/fillwire
   sudo chmod 0600 /etc/fillwire/fillwire.env
   printf 'IMAGE=ghcr.io/mgh3326/fillwire@%s\n' '<digest>' | sudo tee /var/lib/fillwire/fillwire.service.image
   sudo docker pull ghcr.io/mgh3326/fillwire@<digest>
   ```

4. Install the container unit over the same unit name, adapted from
   `deploy/systemd/fillwire.service.example` (edit the `EnvironmentFile`,
   `--env-file`, and `--volume` paths to the host's real locations), then
   reload and start:

   ```sh
   sudoedit /etc/systemd/system/fillwire.service
   sudo systemd-analyze verify /etc/systemd/system/fillwire.service
   sudo systemctl daemon-reload
   sudo systemctl enable fillwire.service
   sudo systemctl start fillwire.service
   ```

5. Re-run the deploy step-4 verification.

To return to the native binary, stop the container **first** — the same
one-websocket-per-app-key rule as `at-kis-ws` applies, so the native
binary must not start while the container could still hold the session:

```sh
sudo systemctl stop fillwire.service
docker ps --format '{{.Names}}'
# expected: no fillwire line — the container must be gone before continuing
sudo cp /root/fillwire-native-backup/fillwire.service /etc/systemd/system/fillwire.service
sudo systemctl daemon-reload
sudo systemctl enable --now fillwire.service
```

## Quote reader (optional, off by default)

The quote reader (`[quotes]` in the TOML) is documented in the README section
"Quote reader (off by default)". That section covers the two enablement
prerequisites, turning it on, the symbol file mount, how to verify
`quotes:toss`, and turning it off. It uses the Toss Securities websocket, not
KIS, so it does not change the KIS one-session rule above. It reads the cached
Toss token read-only and opens exactly one of the two Toss connections the
account allows.

## Health surface

fillwire exposes **no** HTTP endpoint, port, or metrics surface — the README
lists an HTTP metrics endpoint as deliberately out of scope, and the code has
no listener (the only `net/http` use is the outbound ingest POST in
`internal/sink`). The distroless runtime image also has no shell or binary a
Dockerfile `HEALTHCHECK` could exec, so there is no in-container check to
define today. Liveness verification is therefore journal-based: unit `active`,
`NRestarts=0`, and the `KIS websocket initial subscription active` log line —
exactly the checks in deploy step 4. A future opt-in health listener (for
example `-health-listen 127.0.0.1:<port>` serving `GET /healthz`, 200 only
while the KIS subscription is active) would let the unit add an `ExecStartPost`
curl gate; that is a design note only and no code is added here.

## Task 180 re-observation plan (operator window only)

This is a plan, not an observation record. Before starting the window, the
desk updates the host /etc/fillwire/fillwire.toml to set approval_mode = ""
and approval_refresh_margin = "1h". A host TOML left at approval_mode =
"cache-only" will not self-issue at the 23-hour boundary. The desk also wires
FILLWIRE_ALERT_TELEGRAM_BOT_TOKEN and FILLWIRE_ALERT_TELEGRAM_CHAT_ID in
/etc/fillwire/fillwire.env at deploy. Keep real values out of this runbook.
During a scheduled window, stop at-kis-ws and verify it is inactive before
starting fillwire. Keep at-kis-ws stopped while fillwire owns the KIS session.
The desk performs the observation across the 23-hour Redis approval-cache boundary and at least one
Korean market session. Record the approval_rest_issue_call and lock contention
log counts, cache TTL, restart count, fill ingress, and Telegram delivery
outcome without recording key or credential values. A healthy boundary has
one approval issue per mode across competing instances, a refreshed key before
expiry, and no recurring approval churn. Escalate any exit 42, 78, or repeated
exit 1 with the bounded logs and timestamps.

Step 7 restore remains the existing switch-back procedure above: stop
fillwire, verify it is inactive, then start at-kis-ws. If fillwire's deployment
pin must also be restored, use the Roll back section's paired pin and TOML snapshot, restore of
both, pull, restart, and invocation-scoped verification. Never run both websocket owners
at once.
