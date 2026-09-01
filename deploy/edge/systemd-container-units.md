# NCP container systemd units

Fable Ops applies these examples on the target hosts. They replace the former
scp-managed binaries; this repository does not apply or restart any live unit.
Both services use the GHCR image and host networking, so their existing
loopback-only listener settings remain valid.

## Host preparation

Create the persistent directory before starting `broker-edge.service`. The
distroless image runs as UID/GID `65532`, so that account must be able to write
the SQLite file.

```sh
install -d -o 65532 -g 65532 -m 0750 /var/lib/broker-edge
```

Keep each existing environment file at its current host location. The examples
use `/etc/broker-edge/broker-edge.env` and `/etc/broker-edge/gatewayd.env`;
change only those paths if the NCP host already uses different ones. In the edge
environment file, set the SQLite path inside the mounted directory:

```sh
BROKER_EDGE_SQLITE_PATH=/var/lib/broker-edge/kis-mock-edge.sqlite
```

The `:main` tag follows the latest successful main build. Do not run it
directly in a unit: use the digest-pinning pull deployment documented in
[`docs/digest-pin-deploy.md`](../docs/digest-pin-deploy.md). The examples below
are retained as the former floating-tag reference; use the new
`deploy/systemd/*.service.example` files for a digest-pinned unit.

## `/etc/systemd/system/broker-edge.service`

This is the expanded form of `deploy/systemd/broker-edge.service.example`.
`/etc/broker-edge/broker-edge.env` remains a host-only placeholder, while the
image file is written by `scripts/deploy-pull.sh broker-edge.service`.

```ini
[Unit]
Description=broker-edge kis-mock-edge container
After=docker.service network-online.target
Wants=network-online.target
Requires=docker.service

[Service]
Type=simple
EnvironmentFile=/etc/broker-edge/broker-edge.env
EnvironmentFile=/var/lib/broker-edge/broker-edge.service.image
ExecStartPre=-/usr/bin/docker rm -f broker-edge
ExecStart=/usr/bin/docker run --rm --name broker-edge --network host --env-file /etc/broker-edge/broker-edge.env --volume /var/lib/broker-edge:/var/lib/broker-edge:rw ${IMAGE} kis-mock-edge
ExecStop=/usr/bin/docker stop --time 30 broker-edge
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

## `/etc/systemd/system/gatewayd.service`

This is the expanded form of `deploy/systemd/gatewayd.service.example`; its
image file is written by `scripts/deploy-pull.sh gatewayd.service`.

```ini
[Unit]
Description=broker-edge gatewayd container
After=docker.service network-online.target
Wants=network-online.target
Requires=docker.service

[Service]
Type=simple
EnvironmentFile=/etc/broker-edge/gatewayd.env
EnvironmentFile=/var/lib/broker-edge/gatewayd.service.image
ExecStartPre=-/usr/bin/docker rm -f gatewayd
ExecStart=/usr/bin/docker run --rm --name gatewayd --network host --env-file /etc/broker-edge/gatewayd.env ${IMAGE} gatewayd
ExecStop=/usr/bin/docker stop --time 30 gatewayd
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

After Ops has reviewed the environment-file paths, they can run `systemctl
daemon-reload` and enable the two units. Use `scripts/deploy-pull.sh` to pull,
pin, restart, and health-check them; do not place credentials in a unit file or
this repository.
