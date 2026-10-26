.PHONY: run build test vet fmt docker

run:
	go run ./cmd/relay

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/relay ./cmd/relay

test:
	go test -race -count=1 ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

docker:
	docker build -t sparshur-relay .
