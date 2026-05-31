.PHONY: build docker-down docker-up fmt run test test-integration vet

build:
	mkdir -p bin
	go build -o bin/erp-mock ./cmd/erp-mock

run:
	go run ./cmd/erp-mock

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './vendor/*')

test:
	go test ./...

test-integration:
	go test -tags=integration -count=1 -timeout=5m ./internal/messaging/rabbitmq/...

vet:
	go vet ./...

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down
