BINARY  := uplink
PKG     := github.com/timw255/uplink
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: all build run test lint fmt tidy clean

all: build

build:
	go build -trimpath -ldflags '$(LDFLAGS)' -o bin/$(BINARY) ./cmd/uplink

run: build
	./bin/$(BINARY) --config configs/example.yaml

test:
	go test -race -count=1 ./...

lint:
	golangci-lint run ./...

fmt:
	gofmt -s -w .
	goimports -w .

tidy:
	go mod tidy

clean:
	rm -rf bin dist data
