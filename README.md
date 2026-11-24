# Sparshur Service

A WebSocket relay for Sparshur's Mac and Android apps. The authenticated endpoint
verifies enrolled device signing keys, enforces pairing routes and forwards opaque
packets between active sessions.

## Running

```sh
make run            # go run ./cmd/relay on :8080
make test           # go test -race ./...
docker build -t sparshur-relay . && docker run -p 8080:8080 sparshur-relay
```

## Endpoints

| Method | Path                               | Purpose                                   |
|--------|------------------------------------|-------------------------------------------|
| GET    | `/healthz`                         | Liveness probe                            |
| GET    | `/readyz`                          | Readiness probe with room, peer and authenticated session counts; `503` with `"status":"draining"` during shutdown |
| GET    | `/v1/relay`                        | Authenticated binary WebSocket (`bridge.relay.v1`) |
| POST   | `/v1/rooms`                        | Mint a random 256-bit room ID             |
| GET    | `/v1/rooms/{roomID}/ws?role=ROLE`  | WebSocket relay; `ROLE` is `mac` or `android` |

`roomID` must match `^[A-Za-z0-9_-]{22,128}$`. Devices may mint it themselves
(e.g. during QR pairing) or call `POST /v1/rooms`. Anyone who knows a room ID
can join it, so treat it as a secret and authenticate the peer inside the E2EE
handshake.

## Authenticated relay

Enable `/v1/relay` by setting all three authenticated relay variables. The Mac
and Android apps use the same [relay protocol](https://github.com/sparshchadha/sparshur/blob/HEAD/spec/PROTOCOL.md)
and sign the byte-exact `RELAY_PUBLIC_URL` and `RELAY_ID` during authentication.
`RELAY_ID` is 1–128 visible ASCII characters excluding `|`. `RELAY_PUBLIC_URL`
uses `wss`, a lowercase host and the `/v1/relay` path without credentials,
query or fragment. The relay accepts one 65,536-byte binary frame per
WebSocket message.

Use the public enrollment details in each app to register its device ID and
Base64 Ed25519 public key. Build the offline command with `make build`.

```sh
bin/relayctl -file ./enrollment.json add-device -id "$MAC_ID" -key "$MAC_PUBLIC_KEY" -label Mac
bin/relayctl -file ./enrollment.json add-device -id "$ANDROID_ID" -key "$ANDROID_PUBLIC_KEY" -label Android
bin/relayctl -file ./enrollment.json add-pairing -id "$PAIRING_ID" -a "$MAC_ID" -b "$ANDROID_ID"
bin/relayctl -file ./enrollment.json list
```

Use a fresh canonical lowercase UUID for the route ID. The file is written
atomically with mode `0600`. Updates are serialized with an adjacent `.lock`
file. Use `remove-pairing -id` to remove a route or
`remove-device -id` to remove a device and its routes. Send the service
`SIGHUP` after updating the file. Invalid reloads leave the previous enrollment
in effect. Removed or rekeyed devices close with `4403`, and a new authenticated
session replaces the prior one with `4409`. An unenrolled identity gets
`AUTH_REJECTED` and `4401`.

Authorized submissions receive `FORWARDED` after the recipient socket write
or `UNAVAILABLE` if delivery cannot complete. A forwarding response confirms
transport progress. The apps currently require the end-to-end upgrade to process
deliveries. Test enrollment refusal and WebSocket negotiation without private
keys using `go run ./cmd/relaycheck -bridge -require-tls -url "$RELAY_PUBLIC_URL"`.

## Legacy room wire protocol

- Each room holds at most one `mac` and one `android` connection.
- **Binary messages** from a client are forwarded verbatim to the other peer.
  Messages sent while the other peer is offline are dropped, not queued.
- **Text messages** are relay control messages, server → client only, as JSON:
  - `{"type":"welcome","room_id":"…","role":"mac","peer_present":false}` on
    connect; always the first message a connection receives
  - `{"type":"peer_joined","role":"android"}`
  - `{"type":"peer_left","role":"android"}`

  A client that sends a text message is closed with `1008 policy violation`.
- Control messages and forwarded messages reach each client in the order
  the relay processed them, so a `peer_left` is never delivered after the
  `peer_joined` of a later reconnect.
- Connecting with a role that is already present replaces the old connection
  (useful after a network change); the old one is closed with `4000`. The
  other peer sees no `peer_left`/`peer_joined` for a replacement.
- A message larger than `RELAY_MAX_MESSAGE_BYTES` closes the sender with
  `1009 message too big`. The limit applies to whole WebSocket messages
  (fragments are reassembled first); clients must also allow receiving
  messages of that size.
- A client that cannot keep up with its send queue is closed with `4001`.
- The relay pings every connection (`RELAY_PING_INTERVAL`) to keep NATs open
  and detect dead peers.
- On shutdown the relay stops accepting connections (`503`), closes every
  connection with `1001 going away`, and force-closes those that have not
  finished closing by `RELAY_SHUTDOWN_TIMEOUT`.

## Configuration

| Variable                 | Default | Description                                  |
|--------------------------|---------|----------------------------------------------|
| `RELAY_ADDR`             | `:8080` | Listen address                               |
| `RELAY_MAX_MESSAGE_BYTES`| `1048576` | Legacy room message limit (> 0)  |
| `RELAY_SEND_QUEUE_SIZE`  | `64`    | Messages buffered per connection (> 0)       |
| `RELAY_PING_INTERVAL`    | `25s`   | Legacy room ping interval; `0` disables pings  |
| `RELAY_WRITE_TIMEOUT`    | `10s`   | Per-write / ping timeout (> 0)               |
| `RELAY_SHUTDOWN_TIMEOUT` | `15s`   | Grace period on SIGTERM before force-closing (> 0) |
| `RELAY_ALLOWED_ORIGINS`  | (none)  | Comma-separated Origin host patterns for browser clients; native clients send no Origin and are always allowed |
| `RELAY_PUBLIC_URL`       | (unset) | Exact external WSS endpoint for auth signatures |
| `RELAY_ID`               | (unset) | Stable ID shared by relay and both clients |
| `RELAY_ENROLLMENT_FILE`  | (unset) | Path to offline enrollment JSON |
| `RELAY_LOG_LEVEL`        | `info`  | `debug`, `info`, `warn`, `error`             |

TLS is expected to be terminated in front of the relay (load balancer or
reverse proxy); clients should always connect with `wss://`.

## Deployment

The same binary runs on a Linux VPS or a Mac mini. For a private VPN
(Tailscale) deployment, set `RELAY_ADDR=127.0.0.1:8080` and terminate TLS on
the tailnet; see [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) and `deploy/`.

```sh
make dist                                   # cross-compile for linux and darwin
make check RELAY_URL=https://relay-host.example-tailnet.ts.net   # smoke-test a deployment
```

## Layout

```
cmd/relay/          entrypoint, signal handling, graceful shutdown
cmd/relayctl/       offline enrollment editing
cmd/relaycheck/     deployment smoke test (not part of the relay binary)
deploy/             systemd unit, launchd plist, Caddyfile, env example
internal/bridge/    authenticated transport, enrollment and routing
internal/config/    environment configuration
internal/relay/     rooms, peers, forwarding, control protocol
internal/server/    HTTP routes and WebSocket upgrade
```
