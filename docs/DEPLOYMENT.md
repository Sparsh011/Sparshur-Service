# Deploying the relay on a private VPN

The relay runs as an unprivileged Go service on Linux or a Mac mini. See the
[README](../README.md) for configuration, enrollment and the wire contract. This guide covers a
private deployment where the relay is reachable only over a VPN (Tailscale
here; WireGuard works the same way) and TLS is terminated by a trusted proxy
on the same host.

Android and macOS connect to the authenticated `/v1/relay` endpoint over
WSS. Paired-v2 local HTTP uses port 8787 on the receiving Mac for the local
and direct private-VPN lessons.

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

Tailnet ACLs control network access. The relay separately authenticates both
devices against its enrollment file.

### Why not bind the relay to the tailnet address directly?

`RELAY_ADDR=100.x.y.z:8080` works and keeps the listener off other
interfaces, but the relay does not speak TLS, and clients must use `wss://`.
Use it only when the terminator runs on a different tailnet host. In that
case the relay will fail to start until the tailnet address exists; the
service managers below restart it until it does.

## 1. Build and install the binary

```sh
make dist    # dist/{relay,relayctl}-{linux-amd64,linux-arm64,darwin-arm64,darwin-amd64}
```

Copy the matching file to the host as `/usr/local/bin/sparshur-relay`
(mode `0755`, owned by root). Install the matching `relayctl` as
`/usr/local/bin/relayctl`.

## 2. Configure device enrollment

Copy the device ID and Base64 Ed25519 public key from Mac Settings →
Diagnostics → Public enrollment details and Android debug Settings → Mac
mini relay. Run the `relayctl` commands in the [README](../README.md) in a
private admin directory, with the two real public keys and a fresh lowercase
UUID for the route. This enrollment registers the devices for transport
authentication and routing. Encrypted messaging will use its own pairing
upgrade.

Set the three environment variables as follows, using your actual hostname
and a stable relay ID.

```text
RELAY_PUBLIC_URL=wss://relay-host.<tailnet>.ts.net/v1/relay
RELAY_ID=home-mini
RELAY_ENROLLMENT_FILE=/etc/sparshur-relay/enrollment.json
```

Use the same exact URL and ID, including any explicit port, as
`SPARSHUR_RELAY_URL` / `SPARSHUR_RELAY_ID` on the Mac and in the WSS URL /
Relay ID fields on Android.

Keep the deployed copy in a root-owned directory. Publish a replacement
atomically and give the relay account read access. Substitute
`_sparshurrelay` on macOS and `sparshur-relay` on Linux.

```sh
sudo install -d -m 0755 /etc/sparshur-relay
sudo install -m 0400 -o "$RELAY_USER" ./enrollment.json /etc/sparshur-relay/enrollment.json.new
sudo mv /etc/sparshur-relay/enrollment.json.new /etc/sparshur-relay/enrollment.json
```

After later changes, publish the replacement and send SIGHUP using
`sudo launchctl kill SIGHUP system/com.sparshur.relay` on macOS or
`sudo systemctl reload sparshur-relay` on Linux.

## 3. Run it as a service

### Linux VPS (systemd)

```sh
sudo useradd --system --no-create-home --shell /usr/sbin/nologin sparshur-relay
sudo install -m 0755 relay-linux-amd64 /usr/local/bin/sparshur-relay
sudo install -D -m 0644 deploy/relay.env.example /etc/sparshur-relay/relay.env
sudo install -m 0644 deploy/systemd/sparshur-relay.service /etc/systemd/system/
sudo systemctl daemon-reload
```

Set the authenticated relay variables in `/etc/sparshur-relay/relay.env` and
publish the enrollment file after creating the service user. Start the unit
with `sudo systemctl enable --now sparshur-relay`. The enrollment directory is
read-only in the unit. Read JSON logs with `journalctl -u sparshur-relay`.

### Mac mini (launchd)

Create a dedicated account for the relay. It must not be the account that
runs the Sparshur receiver app: the relay is a routing role and should have
no access to the recipient's Keychain or message history.

```sh
sudo sysadminctl -addUser _sparshurrelay -shell /usr/bin/false -home /var/empty
sudo install -m 0755 relay-darwin-arm64 /usr/local/bin/sparshur-relay
sudo install -d -o _sparshurrelay /usr/local/var/log/sparshur-relay
sudo install -m 0644 -o root -g wheel deploy/launchd/com.sparshur.relay.plist /Library/LaunchDaemons/
```

launchd does not read env files; edit `EnvironmentVariables` in the plist to
set the authenticated relay variables. Publish the enrollment after creating
`_sparshurrelay`, then run
`sudo launchctl bootstrap system /Library/LaunchDaemons/com.sparshur.relay.plist`.
After editing environment variables, boot out and bootstrap the job again.
Set the mini to never sleep and to restart after power loss
(`sudo pmset -a sleep 0 autorestart 1`).

### Docker (either host)

Publish the port on loopback only; inside the container the default `:8080`
is correct.

```sh
docker run -d --restart unless-stopped -p 127.0.0.1:8080:8080 sparshur-relay
```

## 4. Terminate TLS on the tailnet

Enable MagicDNS and HTTPS certificates for the tailnet in the Tailscale
admin console first. Pick one terminator.

### Option A: `tailscale serve` (simplest)

```sh
tailscale serve --bg --https=443 http://127.0.0.1:8080
tailscale serve status
```

This maps the root path to the loopback service and preserves `/v1/relay`
for `wss://relay-host.<tailnet>.ts.net/v1/relay`. Tailnet clients need TLS 1.3.
Do not use `tailscale funnel`; that publishes to the internet. On a Mac the
CLI is `/Applications/Tailscale.app/Contents/MacOS/Tailscale`.

### Option B: Caddy

Use [`deploy/caddy/Caddyfile`](../deploy/caddy/Caddyfile) after replacing the
hostname and the `bind` address with this host's MagicDNS name and tailnet
IP. Caddy fetches the `*.ts.net` certificate from the local tailscaled; on
Linux allow that with `TS_PERMIT_CERT_UID=caddy` in `/etc/default/tailscaled`.

Either way the terminator must pass WebSocket upgrades through (both do by
default) and must not buffer or idle-timeout long-lived connections below
the relay's 25 s ping interval.

## 5. Lock down ingress

- Confirm the relay is on loopback only:
  `sudo lsof -nP -iTCP:8080 -sTCP:LISTEN` shows `127.0.0.1:8080`.
- VPS: default-deny inbound on the public interface except the VPN's own
  port (and SSH if you use it). Port 443 must not be open publicly.
- Mac mini: no router port-forward to the mini.
- Tailnet ACL: allow only the paired devices to reach `relay-host:443`.

## 6. Verify

Run the authenticated transport probe from a tailnet device through the
terminator. It checks the selected subprotocol and rejection of a randomly
generated unenrolled device. The probe uses platform certificate trust.

```sh
go run ./cmd/relaycheck -bridge -require-tls -url "$RELAY_PUBLIC_URL"
```

Expected output ends in `PASS`. For the live connection check, put the real
Android phone on mobile data with the tailnet active, open the debug **Mac
mini relay** screen and connect. Check it says **Authenticated to relay**.
Start the Mac receiver and check its relay status in Settings → Diagnostics.
`/readyz` should report `"authenticated_sessions":2` while both devices stay
connected. Endpoint delivery and receipts require the end-to-end upgrade.

| Check | Expected |
|-------|----------|
| Authenticated probe with Tailscale disconnected | Fails to connect |
| `curl http://<public-or-LAN-ip>:8080/healthz` from another machine | Connection refused or timeout |
| `curl https://<public-ip>/healthz` from outside the tailnet | Connection refused or timeout |
| `curl -s https://relay-host.<tailnet>.ts.net/readyz` on the tailnet | Healthy status, room/peer counts and authenticated-session count |
| Remove an enrolled device and reload | That session closes with 4403 |

Run `make check` on the host to smoke-test the legacy room routes through the
loopback listener. Local TLS integration tests exercise the authenticated
endpoint with temporary enrollments and keys. The deployed certificate,
tailnet ACLs and mobile reachability require a live host.

## Operations

- **Upgrade:** replace the binary and restart the service. On SIGTERM the
  relay closes every connection with `1001 going away` and exits within
  `RELAY_SHUTDOWN_TIMEOUT`. While draining, `/readyz` and new WebSocket
  upgrades return `503`.
- **State.** Back up the enrollment file. Sessions and relay queues last
  only for the life of the process. Update the configured URL on all three
  if the public hostname changes.
- **Logs.** The service emits JSON on stdout without packet bodies,
  signatures, keys, nonces or challenges.
