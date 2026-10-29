# Sparshur Service

A blind WebSocket relay that carries end-to-end encrypted traffic between a
MacBook and an Android device. The relay only pairs connections and forwards
bytes; it never sees plaintext or keys.

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
| GET    | `/readyz`                          | Readiness probe, includes room/peer count |
| POST   | `/v1/rooms`                        | Mint a random 256-bit room ID             |
| GET    | `/v1/rooms/{roomID}/ws?role=ROLE`  | WebSocket relay; `ROLE` is `mac` or `android` |

`roomID` must match `^[A-Za-z0-9_-]{22,128}$`. Devices may mint it themselves
(e.g. during QR pairing) or call `POST /v1/rooms`. Anyone who knows a room ID
can join it, so treat it as a secret and authenticate the peer inside the E2EE
handshake.

## Wire protocol

- Each room holds at most one `mac` and one `android` connection.
- **Binary frames** from a client are forwarded verbatim to the other peer.
  Frames sent while the other peer is offline are dropped.
- **Text frames** are relay control messages, server → client only, as JSON:
  - `{"type":"welcome","room_id":"…","role":"mac","peer_present":false}` on connect
  - `{"type":"peer_joined","role":"android"}`
  - `{"type":"peer_left","role":"android"}`

  A client that sends a text frame is closed with `1008 policy violation`.
- Connecting with a role that is already present replaces the old connection
  (useful after a network change); the old one is closed with `4000`. The
  other peer sees no `peer_left`/`peer_joined` for a replacement.
- A client that cannot keep up with its send queue is closed with `4001`.
- The relay pings every connection (`RELAY_PING_INTERVAL`) to keep NATs open
  and detect dead peers.

## Configuration

| Variable                 | Default | Description                                  |
|--------------------------|---------|----------------------------------------------|
| `RELAY_ADDR`             | `:8080` | Listen address                               |
| `RELAY_MAX_MESSAGE_BYTES`| `1048576` | Max size of one frame                      |
| `RELAY_SEND_QUEUE_SIZE`  | `64`    | Frames buffered per connection               |
| `RELAY_PING_INTERVAL`    | `25s`   | Keepalive ping interval; `0` disables pings  |
| `RELAY_WRITE_TIMEOUT`    | `10s`   | Per-write / ping timeout                     |
| `RELAY_SHUTDOWN_TIMEOUT` | `15s`   | Grace period on SIGTERM                      |
| `RELAY_ALLOWED_ORIGINS`  | (none)  | Comma-separated Origin host patterns for browser clients; native clients send no Origin and are always allowed |
| `RELAY_LOG_LEVEL`        | `info`  | `debug`, `info`, `warn`, `error`             |

TLS is expected to be terminated in front of the relay (load balancer or
reverse proxy); clients should always connect with `wss://`.

## Layout

```
cmd/relay/          entrypoint, signal handling, graceful shutdown
internal/config/    environment configuration
internal/relay/     rooms, peers, forwarding, control protocol
internal/server/    HTTP routes and WebSocket upgrade
```
