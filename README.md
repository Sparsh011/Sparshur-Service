# Sparshur Service

A WebSocket and streaming HTTP relay for Sparshur's Mac and Android apps.
The authenticated WebSocket endpoint verifies enrolled device signing keys
and enforces pairing routes. The room transfer endpoints forward opaque
encrypted bytes between a sender and receiver.

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
| GET    | `/readyz`                          | Readiness probe with counts for rooms, peers, authenticated sessions and active transfers; `503` with `"status":"draining"` during shutdown |
| GET    | `/v1/relay`                        | Authenticated binary WebSocket (`bridge.relay.v1`) |
| POST   | `/v1/rooms`                        | Mint a random 256-bit room ID             |
| GET    | `/v1/rooms/{roomID}/ws?role=ROLE`  | WebSocket relay; `ROLE` is `mac` or `android` |
| PUT    | `/v1/rooms/{roomID}/transfers/{transferID}` | Stream an encrypted upload to its receiver |
| GET    | `/v1/rooms/{roomID}/transfers/{transferID}` | Receive the matching encrypted upload |

`roomID` must match `^[A-Za-z0-9_-]{22,128}$`. Devices may mint it themselves
(e.g. during QR pairing) or call `POST /v1/rooms`. Anyone who knows a room ID
can join it, so treat it as a secret and authenticate the peer inside the E2EE
handshake.

## Authenticated relay

Enable `/v1/relay` by setting all three authenticated relay variables. The Mac
and Android apps use the same [relay protocol](https://github.com/Sparsh011/Sparshur-Mac-Client/blob/HEAD/spec/PROTOCOL.md)
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

## File transfers

The sender chooses a cryptographically random transfer ID of 22–128 base64url
characters (`^[A-Za-z0-9_-]{22,128}$`). It sends an encrypted offer over the
room WebSocket carrying the transfer ID, file name, MIME type, size and a
per-transfer key. The recipient starts a `GET` and the sender starts a `PUT` on the same transfer
path. Either side can arrive first and waits up to `RELAY_TRANSFER_WAIT` for
its peer. A transfer ID allows one sender and one receiver at a time.

The upload body contains client-encrypted chunks with an authenticated final
chunk. Clients must verify that final chunk before accepting or saving a
complete file. Type detection happens on the sending or receiving device.
The relay forwards ciphertext without storing files or inspecting content or
type. Names, MIME types and keys stay inside the encrypted offer.

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
The transfer endpoints use these status codes.

| Status | Meaning |
|--------|---------|
| `200` | The relay delivered the encrypted bytes to the receiver |
| `400` | Invalid room or transfer ID |
| `409` | A sender or receiver already occupies that slot |
| `413` | Declared or streamed bytes exceed the configured limit |
| `502` | The receiver disconnected or failed during a write |
| `503` | Transfer capacity reached or the relay is shutting down |
| `504` | The peer did not arrive before the rendezvous deadline |

A sender read failure or idle timeout aborts both HTTP streams. Each read and
write has the `RELAY_TRANSFER_IDLE_TIMEOUT` idle deadline. On shutdown, waiting transfers receive `503` and paired transfers have the
HTTP server's shutdown grace period to finish.

The Android and Mac relay clients use the authenticated WebSocket above.
Using the room streaming endpoints in either app requires separate client
integration.

## Configuration

| Variable                 | Default | Description                                  |
|--------------------------|---------|----------------------------------------------|
| `RELAY_ADDR`             | `:8080` | Listen address                               |
| `RELAY_MAX_MESSAGE_BYTES`| `1048576` | Legacy room message limit (> 0)  |
| `RELAY_SEND_QUEUE_SIZE`  | `64`    | Messages buffered per connection (> 0)       |
| `RELAY_PING_INTERVAL`    | `25s`   | Legacy room ping interval; `0` disables pings  |
| `RELAY_WRITE_TIMEOUT`    | `10s`   | Per-write / ping timeout (> 0)               |
| `RELAY_TRANSFER_WAIT` | `60s` | Wait for the matching sender or receiver (> 0) |
| `RELAY_TRANSFER_IDLE_TIMEOUT` | `30s` | Per-read and per-write transfer idle deadline (> 0) |
| `RELAY_MAX_TRANSFER_BYTES` | `0` | Maximum encrypted transfer bytes (0 allows unlimited, ≥ 0) |
| `RELAY_MAX_TRANSFERS` | `16` | Maximum waiting and paired transfers (> 0) |
| `RELAY_SHUTDOWN_TIMEOUT` | `15s`   | Grace period on SIGTERM before force-closing (> 0) |
| `RELAY_ALLOWED_ORIGINS`  | (none)  | Comma-separated Origin host patterns for browser clients; native clients send no Origin and are always allowed |
| `RELAY_PUBLIC_URL`       | (unset) | Exact external WSS endpoint for auth signatures |
| `RELAY_ID`               | (unset) | Stable ID shared by relay and both clients |
| `RELAY_ENROLLMENT_FILE`  | (unset) | Path to offline enrollment JSON |
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
cmd/relayctl/       offline enrollment editing
cmd/relaycheck/     deployment smoke test (not part of the relay binary)
deploy/             systemd unit, launchd plist, Caddyfile, env example
internal/bridge/    authenticated transport, enrollment and routing
internal/config/    environment configuration
internal/relay/     rooms, peers, forwarding, control protocol
internal/transfer/  HTTP rendezvous and bounded-memory streaming
internal/server/    HTTP routes and WebSocket upgrade
```
