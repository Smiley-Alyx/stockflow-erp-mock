# Stockflow ERP Mock

`stockflow-erp-mock` is an external sandbox service that emulates inventory
reservation flows of an ERP/WMS integration. The service is implemented in Go
and exposes an HTTP interface for health checks and local administration.

## Local run

```bash
go run ./cmd/stockflow-erp-mock
```

The HTTP server listens on `:8080` by default.

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

