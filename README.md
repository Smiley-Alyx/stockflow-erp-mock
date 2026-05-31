# Stockflow ERP Mock

`stockflow-erp-mock` is an external sandbox service that emulates inventory
reservation flows of an ERP/WMS integration. The service is implemented in Go
and exposes an HTTP interface for health checks and local administration.

## Local run

```bash
make run
```

The HTTP server listens on `:8080` by default.

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
| `ERP_SHUTDOWN_TIMEOUT` | `10s` | Graceful shutdown timeout |

## HTTP endpoints

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/health` | Liveness probe |
| `GET` | `/ready` | Readiness probe |
