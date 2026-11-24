.PHONY: run build test vet fmt docker dist check

run:
	go run ./cmd/relay

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/relay ./cmd/relay
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/relayctl ./cmd/relayctl

test:
	go test -race -count=1 ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

docker:
	docker build -t sparshur-relay .

# Cross-compile the same static binary for every supported host:
# Linux VPS (amd64/arm64) and Mac mini (Apple silicon/Intel).
DIST_FLAGS := -trimpath -ldflags="-s -w"

dist:
	CGO_ENABLED=0 GOOS=linux  GOARCH=amd64 go build $(DIST_FLAGS) -o dist/relay-linux-amd64  ./cmd/relay
	CGO_ENABLED=0 GOOS=linux  GOARCH=arm64 go build $(DIST_FLAGS) -o dist/relay-linux-arm64  ./cmd/relay
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build $(DIST_FLAGS) -o dist/relay-darwin-arm64 ./cmd/relay
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build $(DIST_FLAGS) -o dist/relay-darwin-amd64 ./cmd/relay
	CGO_ENABLED=0 GOOS=linux  GOARCH=amd64 go build $(DIST_FLAGS) -o dist/relayctl-linux-amd64  ./cmd/relayctl
	CGO_ENABLED=0 GOOS=linux  GOARCH=arm64 go build $(DIST_FLAGS) -o dist/relayctl-linux-arm64  ./cmd/relayctl
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build $(DIST_FLAGS) -o dist/relayctl-darwin-arm64 ./cmd/relayctl
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build $(DIST_FLAGS) -o dist/relayctl-darwin-amd64 ./cmd/relayctl

# Smoke-test a running relay end to end, e.g.
#   make check RELAY_URL=https://relay-host.example-tailnet.ts.net
RELAY_URL ?= http://127.0.0.1:8080

check:
	go run ./cmd/relaycheck -url $(RELAY_URL)
