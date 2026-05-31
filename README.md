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
| `ERP_RABBITMQ_PREFETCH_COUNT` | `10` | Maximum number of unacknowledged messages |
| `ERP_RABBITMQ_PUBLISH_TIMEOUT` | `5s` | Publisher confirmation timeout |
| `ERP_SHUTDOWN_TIMEOUT` | `10s` | Graceful shutdown timeout |

## HTTP endpoints

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/health` | Liveness probe |
| `GET` | `/ready` | Readiness probe |
| `GET` | `/stock` | List available and reserved stock |
| `POST` | `/stock` | Create or update available stock |
| `GET` | `/reservations` | List reservations |
| `GET` | `/reservations/{id}` | Get a reservation |
| `POST` | `/debug/failure-mode` | Configure a sandbox failure mode |
