# Stockflow ERP Mock

`stockflow-erp-mock` is an external sandbox service that emulates inventory
reservation flows of an ERP/WMS integration. The service is implemented in Go
and exposes an HTTP interface for health checks and local administration.

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
[`docs/failure-modes.md`](docs/failure-modes.md).
