# Deploying the relay on a private VPN

The relay is one static Go binary with no state, no config file and no
privileges. The same binary and the same wire contract (see the
[README](../README.md)) run on a Linux VPS or a Mac mini. This guide covers a
private deployment where the relay is reachable only over a VPN (Tailscale
here; WireGuard works the same way) and TLS is terminated by a trusted proxy
on the same host.

Nothing in this guide changes relay behaviour. It uses the existing
`RELAY_ADDR` setting and the existing routes, room semantics and health
checks.

## Where the relay fits

| Milestone | Transport | Uses this relay? |
|-----------|-----------|------------------|
| First cross-network milestone | Android talks straight to the Mac's paired-v2 receiver (port 8787) over the tailnet | No |
| Later hosted transport | Both devices dial out to this relay over `wss://` | Yes |

The relay stays independently deployable so it can be stood up, checked and
left running before any client depends on it. Deploying it does not affect
paired-v2, and paired-v2 does not need it.

The clients' relay transport is not yet this relay's wire contract (different
path, subprotocol and framing), so a deployed relay is verified with
`relaycheck` below, not with the apps.

## Topology

```
device ──wss://relay-host.<tailnet>.ts.net──▶ TLS terminator ──http──▶ relay
         (tailnet only, trusted certificate)   (same host)      127.0.0.1:8080
```

- The relay listens on plaintext HTTP on **loopback only**:
  `RELAY_ADDR=127.0.0.1:8080`. The default `:8080` listens on every
  interface, which is right for a container behind a load balancer and wrong
  for a host with a LAN or public address.
- The TLS terminator listens **only on the tailnet** and presents a publicly
  trusted certificate for the host's `*.ts.net` name, so clients validate it
  with the normal system trust store. No custom CA, no pinning bypass.
- Tailnet ACLs decide which devices can reach the terminator.

Joining the tailnet is network reachability, not authentication. A room ID
is still a secret capability and peers still authenticate each other inside
the E2EE handshake.

### Why not bind the relay to the tailnet address directly?

`RELAY_ADDR=100.x.y.z:8080` works and keeps the listener off other
interfaces, but the relay does not speak TLS, and clients must use `wss://`.
Use it only when the terminator runs on a different tailnet host. In that
case the relay will fail to start until the tailnet address exists; the
service managers below restart it until it does.

## 1. Build and install the binary

```sh
make dist    # dist/relay-{linux-amd64,linux-arm64,darwin-arm64,darwin-amd64}
```

Copy the matching file to the host as `/usr/local/bin/sparshur-relay`
(mode `0755`, owned by root).

## 2. Run it as a service

### Linux VPS (systemd)

```sh
sudo install -m 0755 relay-linux-amd64 /usr/local/bin/sparshur-relay
sudo install -D -m 0644 deploy/relay.env.example /etc/sparshur-relay/relay.env
sudo install -m 0644 deploy/systemd/sparshur-relay.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now sparshur-relay
```

The unit runs as a throwaway `DynamicUser` with a read-only filesystem. Logs
are JSON on stdout: `journalctl -u sparshur-relay`.

### Mac mini (launchd)

Create a dedicated account for the relay. It must not be the account that
runs the Sparshur receiver app: the relay is a routing role and should have
no access to the recipient's Keychain or message history.

```sh
sudo sysadminctl -addUser _sparshurrelay -shell /usr/bin/false -home /var/empty
sudo install -m 0755 relay-darwin-arm64 /usr/local/bin/sparshur-relay
sudo install -d -o _sparshurrelay /usr/local/var/log/sparshur-relay
sudo install -m 0644 -o root -g wheel deploy/launchd/com.sparshur.relay.plist /Library/LaunchDaemons/
sudo launchctl bootstrap system /Library/LaunchDaemons/com.sparshur.relay.plist
```

launchd does not read env files; edit `EnvironmentVariables` in the plist to
change settings, then `sudo launchctl kickstart -k system/com.sparshur.relay`.
Set the mini to never sleep and to restart after power loss
(`sudo pmset -a sleep 0 autorestart 1`).

### Docker (either host)

Publish the port on loopback only; inside the container the default `:8080`
is correct.

```sh
docker run -d --restart unless-stopped -p 127.0.0.1:8080:8080 sparshur-relay
```

## 3. Terminate TLS on the tailnet

Enable MagicDNS and HTTPS certificates for the tailnet in the Tailscale
admin console first. Pick one terminator.

### Option A: `tailscale serve` (simplest)

```sh
tailscale serve --bg --https=443 http://127.0.0.1:8080
tailscale serve status
```

This serves `https://relay-host.<tailnet>.ts.net` to tailnet devices only.
Do not use `tailscale funnel`; that publishes to the internet. On a Mac the
CLI is `/Applications/Tailscale.app/Contents/MacOS/Tailscale`.

### Option B: Caddy

Use [`deploy/caddy/Caddyfile`](../deploy/caddy/Caddyfile) after replacing the
hostname and the `bind` address with this host's MagicDNS name and tailnet
IP. Caddy fetches the `*.ts.net` certificate from the local tailscaled; on
Linux allow that with `TS_PERMIT_CERT_UID=caddy` in `/etc/default/tailscaled`.

Either way the terminator must pass WebSocket upgrades through (both do by
default) and must not buffer or idle-timeout long-lived connections below
the relay's 25 s ping interval. For HTTP file transfers, the proxy must stream
request and response bodies, disable buffering for both, and impose no
body-size cap. `tailscale serve` streams bodies directly. Configure any proxy
I/O timeouts to accommodate the relay's transfer wait and idle settings. The supplied Caddyfile uses
`flush_interval -1` to flush streamed responses immediately.

## 4. Lock down ingress

- Confirm the relay is on loopback only:
  `sudo lsof -nP -iTCP:8080 -sTCP:LISTEN` shows `127.0.0.1:8080`.
- VPS: default-deny inbound on the public interface except the VPN's own
  port (and SSH if you use it). Port 443 must not be open publicly.
- Mac mini: no router port-forward to the mini.
- Tailnet ACL: allow only the paired devices to reach `relay-host:443`.

## 5. Verify

`relaycheck` drives the relay exactly as a device pair would: health probes,
room minting, WebSocket upgrade for both roles, presence messages, binary
forwarding both ways, `peer_left`, and a byte-verified 1 MiB HTTP transfer. It
validates the certificate against the system trust store and has no option to
skip that.

On the host, against the plaintext backend:

```sh
make check                      # http://127.0.0.1:8080
```

From another tailnet device (a laptop on mobile data is the honest test),
through the terminator:

```sh
go run ./cmd/relaycheck -require-tls -url https://relay-host.<tailnet>.ts.net
```

Expected output ends in `PASS`. Then check the negative cases:

| Check | Expected |
|-------|----------|
| Same command with Tailscale disconnected | Fails to connect |
| `curl http://<public-or-LAN-ip>:8080/healthz` from another machine | Connection refused or timeout |
| `curl https://<public-ip>/healthz` from outside the tailnet | Connection refused or timeout |
| `curl -s https://relay-host.<tailnet>.ts.net/readyz` on the tailnet | `{"status":"ok","stats":{...},"transfers":0}` |
| Stop the service, then run `relaycheck` | Fails; the service manager restarts the relay |

`/healthz` is the liveness probe and `/readyz` the readiness probe for any
uptime monitor. Both are unauthenticated and reveal room, peer and active
transfer counts.

### What is verified where

Covered by `make test` on every change: defaults and `RELAY_ADDR` handling
(`internal/config`), and the full wire contract through a TLS-terminating
reverse proxy with certificate validation (`internal/server`,
`TestWireContractBehindTLSTerminator`).

Needs a real host, because it depends on your tailnet: certificate issuance
for the `*.ts.net` name, tailnet-only reachability, ACLs, firewall rules and
service restart on boot. Section 5 is the checklist for those.

## Operations

- **Upgrade:** replace the binary and restart the service. On SIGTERM the
  relay closes every connection with `1001 going away` and exits within
  `RELAY_SHUTDOWN_TIMEOUT`; clients reconnect to the same room. While
  draining, `/readyz`, new WebSocket upgrades and new HTTP transfers return
  `503`. Waiting transfers wake immediately, while paired transfers drain
  under the HTTP shutdown deadline.
- **State:** none. Rooms exist only while a peer is connected, so restarts
  and host moves lose nothing. Moving from the mini to a VPS (or back) is a
  DNS/hostname change for clients, not a migration.
- **Logs:** JSON lines on stdout. Room IDs are truncated to eight
  characters, and transfer IDs use the same truncation. Completed transfers
  log byte count and duration. Payloads are never logged.
