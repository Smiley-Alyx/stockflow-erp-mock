# Stockflow ERP Mock

`stockflow-erp-mock` is an external sandbox service that emulates an ERP/WMS
inventory reservation integration for the `stockflow-market` case study. It is
**not** a real ERP: stock, reservations, and warehouse state are simulated for
local demos and integration testing.

The service is implemented in Go. It communicates with the marketplace through
RabbitMQ and AsyncAPI contracts, and exposes an HTTP API for health checks,
stock inspection, and debug tooling.

## StockFlow ecosystem

Part of the StockFlow ecosystem:

- [stockflow-market](https://github.com/Smiley-Alyx/stockflow-market) — marketplace backend case study
- [stockflow-erp-mock](https://github.com/Smiley-Alyx/stockflow-erp-mock) — external ERP / inventory integration mock (this repository)
- [stockflow-payment-mock](https://github.com/Smiley-Alyx/stockflow-payment-mock) — external payment provider mock
- [stockflow-delivery-mock](https://github.com/Smiley-Alyx/stockflow-delivery-mock) — external delivery provider mock

`stockflow-market` orchestrates checkout and order fulfillment. Each external mock
implements one provider boundary over RabbitMQ with AsyncAPI contracts, shared
header conventions (`correlation_id`, `idempotency_key`, `causation_id`), and
retry/DLQ handling:

| Service | Exchange | Responsibility |
| --- | --- | --- |
| **stockflow-erp-mock** (this repo) | `stockflow.inventory` | Reserve and release stock in the external ERP sandbox |
| [stockflow-payment-mock](https://github.com/Smiley-Alyx/stockflow-payment-mock) | `stockflow.payment` | Authorize, capture, and refund card payments |
| [stockflow-delivery-mock](https://github.com/Smiley-Alyx/stockflow-delivery-mock) | `stockflow.delivery` | Create shipments and publish tracking status events |

A typical checkout in the case study chains these boundaries: the marketplace
reserves inventory, requests payment authorization (and later capture), then
requests shipment creation once the order is paid. The same `correlation_id`
ties messages across all three integrations so the market can reconstruct the
full order timeline.

See [`docs/architecture.md`](docs/architecture.md#stockflow-ecosystem) for the
end-to-end diagram and links to sibling repositories.

## Documentation

Portfolio-grade integration documentation:

| Document | Description |
| --- | --- |
| [`docs/architecture.md`](docs/architecture.md) | System design, layers, deployment, trade-offs |
| [`docs/integration-flow.md`](docs/integration-flow.md) | Message flows, RabbitMQ topology, idempotency |
| [`docs/failure-modes.md`](docs/failure-modes.md) | Sandbox failure injection and test matrix |
| [`docs/demo.md`](docs/demo.md) | Hands-on walkthrough and presentation narrative |

## Local run

```bash
make run
```

The HTTP server listens on `:8080` by default.

## AsyncAPI contract

The versioned RabbitMQ integration contract is available in
[`contracts/asyncapi.yaml`](contracts/asyncapi.yaml). Payload schemas are stored
in [`contracts/messages`](contracts/messages).

## Docker Compose

Start the service with a local RabbitMQ instance:

```bash
make docker-up
```

The RabbitMQ management UI is available at `http://localhost:15672`.
Use `stockflow` as both username and password for the local environment.

Stop the containers:

```bash
make docker-down
```

## Configuration

| Environment variable | Default | Description |
| --- | --- | --- |
| `ERP_HTTP_ADDRESS` | `:8080` | HTTP server listen address |
| `ERP_LOG_LEVEL` | `info` | Structured log level |
| `ERP_RABBITMQ_URL` | `amqp://stockflow:stockflow@localhost:5672/` | RabbitMQ connection URL |
| `ERP_RABBITMQ_CONSUMER_TAG` | `stockflow-erp-mock` | RabbitMQ consumer tag |
| `ERP_RABBITMQ_MAX_RETRY_COUNT` | `3` | Maximum number of delayed processing retries |
| `ERP_RABBITMQ_PREFETCH_COUNT` | `10` | Maximum number of unacknowledged messages |
| `ERP_RABBITMQ_PUBLISH_TIMEOUT` | `5s` | Publisher confirmation timeout |
| `ERP_RABBITMQ_RETRY_DELAY` | `2s` | Delay before another processing attempt |
| `ERP_SHUTDOWN_TIMEOUT` | `10s` | Graceful shutdown timeout |

## RabbitMQ reliability topology

Transient processing failures are published to
`stockflow.erp-mock.inventory.reservation.requested.v1.retry`. The retry queue
uses a fixed TTL and dead-letters messages back to the main inventory exchange.
Malformed messages and messages that exhaust retries are routed to
`stockflow.erp-mock.inventory.reservation.requested.v1.dlq`.
The reservation release queue uses the same retry and dead-letter policy.
Dead-letter messages can be returned to processing manually through the HTTP
admin endpoint. Requeue limits are bounded to keep local administration
operations predictable.

## Metrics

The Prometheus endpoint exposes message processing counters and duration,
reservation outcome counters, idempotency hits, current stock, active
reservations, and dead-letter queue depth.

## HTTP endpoints

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/health` | Liveness probe |
| `GET` | `/ready` | Readiness probe |
| `GET` | `/metrics` | Prometheus metrics |
| `GET` | `/stock` | List available and reserved stock |
| `POST` | `/stock` | Create or update available stock |
| `GET` | `/reservations` | List reservations |
| `GET` | `/reservations/{id}` | Get a reservation |
| `POST` | `/debug/failure-mode` | Configure a sandbox failure mode |
| `POST` | `/debug/dlq/requeue` | Requeue bounded dead-letter messages |

The DLQ requeue endpoint accepts one of the logical queue names:
`reservation_requests` or `reservation_release_requests`.

Sandbox failure simulation modes and request examples are documented in
[`docs/failure-modes.md`](docs/failure-modes.md). For a guided walkthrough,
see [`docs/demo.md`](docs/demo.md).

## Portfolio scope

This repository is part of the [StockFlow ecosystem](#stockflow-ecosystem): a
highload-oriented marketplace backend case study with external service mocks.
It demonstrates:

- inventory reservation and release over RabbitMQ
- idempotent message handling with retry and DLQ
- failure simulation for chaos testing
- Prometheus metrics and structured observability
- AsyncAPI contracts and integration tests
