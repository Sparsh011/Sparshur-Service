# Sparshur Service

A blind WebSocket and streaming HTTP relay that carries end-to-end encrypted
traffic between a MacBook and an Android device. The relay only pairs connections and forwards
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
| GET    | `/readyz`                          | Readiness probe, includes room/peer and active transfer counts, `503` with `"status":"draining"` during shutdown |
| POST   | `/v1/rooms`                        | Mint a random 256-bit room ID             |
| GET    | `/v1/rooms/{roomID}/ws?role=ROLE`  | WebSocket relay; `ROLE` is `mac` or `android` |
| PUT    | `/v1/rooms/{roomID}/transfers/{transferID}` | Stream an encrypted upload to its receiver |
| GET    | `/v1/rooms/{roomID}/transfers/{transferID}` | Receive the matching encrypted upload |

`roomID` must match `^[A-Za-z0-9_-]{22,128}$`. Devices may mint it themselves
(e.g. during QR pairing) or call `POST /v1/rooms`. Anyone who knows a room ID
can join it, so treat it as a secret and authenticate the peer inside the E2EE
handshake.

## Wire protocol

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

## File transfers

The sender chooses a cryptographically random transfer ID of 22–128 base64url
characters (`^[A-Za-z0-9_-]{22,128}$`). It sends an encrypted offer over the
room WebSocket carrying the transfer ID, file name, MIME type, size and key.
The recipient starts a `GET` and the sender starts a `PUT` on the same transfer
path. Either side can arrive first and waits up to `RELAY_TRANSFER_WAIT` for
its peer. A transfer ID allows one sender and one receiver at a time.

The upload body contains client-encrypted chunks with an authenticated final
chunk. Clients must verify that final chunk before accepting or saving a
complete file. Type detection happens on the sending or receiving device.
The relay forwards ciphertext and never inspects the type, name or key.

The receiver gets `application/octet-stream`, with the upload's declared
`Content-Length` when present. Unknown lengths stream with HTTP framing.
The relay uses a pooled 256 KiB buffer per copy and writes each chunk directly
to the receiver. TCP backpressure bounds memory while the slower connection
determines throughput. Transfers use no disk storage. The default byte limit
is unlimited, and `RELAY_MAX_TRANSFERS` bounds waiting and paired transfers.

A successful `PUT` returns `200 {"bytes":N}` once the relay has delivered the
bytes. The peer confirms acceptance over the encrypted WebSocket after
verification. A broken or stalled upload aborts the download stream. Clients
must treat a truncated body or missing authenticated final chunk as failure.
Duplicate senders or receivers get `409`, rendezvous timeouts get `504`,
oversized uploads get `413`, and capacity or shutdown rejection gets `503`.
A failed receiver causes `502` on the upload or a connection error. Each
read and write has the `RELAY_TRANSFER_IDLE_TIMEOUT` idle deadline. On
shutdown, waiting transfers receive `503` and paired transfers have the
HTTP server's shutdown grace period to finish.

The current Android and Mac apps still need to adopt this contract, stream
their encrypted bodies, and lift their client-side 5 MiB limits. Their relay
endpoints and authentication also need alignment with this service. Local
Wi-Fi throughput depends on streaming over the direct paired-device path.

## Configuration

| Variable                 | Default | Description                                  |
|--------------------------|---------|----------------------------------------------|
| `RELAY_ADDR`             | `:8080` | Listen address                               |
| `RELAY_MAX_MESSAGE_BYTES`| `1048576` | Max size of one WebSocket message (> 0)  |
| `RELAY_SEND_QUEUE_SIZE`  | `64`    | Messages buffered per connection (> 0)       |
| `RELAY_PING_INTERVAL`    | `25s`   | Keepalive ping interval; `0` disables pings  |
| `RELAY_WRITE_TIMEOUT`    | `10s`   | Per-write / ping timeout (> 0)               |
| `RELAY_TRANSFER_WAIT` | `60s` | Wait for the matching sender or receiver (> 0) |
| `RELAY_TRANSFER_IDLE_TIMEOUT` | `30s` | Per-read and per-write transfer idle deadline (> 0) |
| `RELAY_MAX_TRANSFER_BYTES` | `0` | Maximum encrypted transfer bytes (0 allows unlimited, ≥ 0) |
| `RELAY_MAX_TRANSFERS` | `16` | Maximum waiting and paired transfers (> 0) |
| `RELAY_SHUTDOWN_TIMEOUT` | `15s`   | Grace period on SIGTERM before force-closing (> 0) |
| `RELAY_ALLOWED_ORIGINS`  | (none)  | Comma-separated Origin host patterns for browser clients; native clients send no Origin and are always allowed |
| `RELAY_LOG_LEVEL`        | `info`  | `debug`, `info`, `warn`, `error`             |

TLS is expected to be terminated in front of the relay (load balancer or
reverse proxy). Clients use `wss://` for WebSockets and `https://` for transfers.

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
cmd/relaycheck/     deployment smoke test (not part of the relay binary)
deploy/             systemd unit, launchd plist, Caddyfile, env example
internal/config/    environment configuration
internal/relay/     rooms, peers, forwarding, control protocol
internal/transfer/  HTTP rendezvous and bounded-memory streaming
internal/server/    HTTP routes and WebSocket upgrade
```
