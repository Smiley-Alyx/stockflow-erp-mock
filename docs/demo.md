# Demo walkthrough

Step-by-step scenarios for exploring the ERP mock locally. All examples assume
Docker Compose is running:

```bash
make docker-up
```

Part of the StockFlow ecosystem:
[stockflow-market](https://github.com/Smiley-Alyx/stockflow-market),
[stockflow-erp-mock](https://github.com/Smiley-Alyx/stockflow-erp-mock),
[stockflow-payment-mock](https://github.com/Smiley-Alyx/stockflow-payment-mock),
[stockflow-delivery-mock](https://github.com/Smiley-Alyx/stockflow-delivery-mock).

This repository covers the **inventory** boundary only. For a full checkout demo,
run the sibling mocks alongside [stockflow-market](https://github.com/Smiley-Alyx/stockflow-market)
and follow their READMEs for payment authorization and shipment creation.
Inventory scenarios here can be driven manually via RabbitMQ or through the
market once checkout integration is wired.

| Service | Local HTTP (typical) |
| --- | --- |
| [stockflow-market](https://github.com/Smiley-Alyx/stockflow-market) | gateway (see market README) |
| **stockflow-erp-mock** (this repo) | `http://localhost:8080` |
| [stockflow-payment-mock](https://github.com/Smiley-Alyx/stockflow-payment-mock) | `http://localhost:8081` |
| [stockflow-delivery-mock](https://github.com/Smiley-Alyx/stockflow-delivery-mock) | `http://localhost:8082` |

ERP mock endpoints for the scenarios below:

| Service | URL |
| --- | --- |
| ERP mock HTTP | http://localhost:8080 |
| RabbitMQ AMQP | localhost:5672 |
| RabbitMQ management | http://localhost:15672 (stockflow / stockflow) |

Estimated time: **15–20 minutes**.

## Prerequisites

- Docker and Docker Compose
- `curl` and `jq` (optional, for formatted JSON)
- Go 1.24+ (for local tests)

## 1. Start the sandbox

```bash
make docker-up
```

This builds the ERP mock image and starts the service with RabbitMQ. See the
service URL table above for ports.

Verify health:

```bash
curl -s http://localhost:8080/health | jq
curl -s http://localhost:8080/ready | jq
```

## 2. Inspect seed stock

```bash
curl -s http://localhost:8080/stock | jq
```

Expected seed data:

| SKU | Available |
| --- | ---: |
| `sku-red-mug` | 120 |
| `sku-blue-notebook` | 80 |
| `sku-black-bag` | 40 |

Adjust stock for demos:

```bash
curl -s -X POST http://localhost:8080/stock \
  -H 'content-type: application/json' \
  --data '{"sku":"sku-red-mug","available_quantity":50}' | jq
```

## 3. Happy path (conceptual)

In a full StockFlow integration, [stockflow-market](https://github.com/Smiley-Alyx/stockflow-market)
publishes to RabbitMQ. The inventory flow:

```mermaid
sequenceDiagram
    participant You as Developer
    participant RMQ as RabbitMQ
    participant ERP as ERP mock

    You->>RMQ: reservation.requested.v1
    RMQ->>ERP: consume
    ERP->>RMQ: reservation.confirmed.v1
    You->>ERP: GET /reservations
    You->>RMQ: release.requested.v1
    RMQ->>ERP: consume
    ERP->>RMQ: reservation.released.v1
```

To publish messages manually, use the RabbitMQ management UI (**Queues →
stockflow.inventory → Publish message**) or an AMQP client. Message shape:

**Headers:**

```json
{
  "message_id": "58d867f6-69e0-4f2f-b1ee-d587aaa48b6e",
  "correlation_id": "bb8d8f75-5210-4038-98cc-f2237d192ff8",
  "causation_id": "fa6c60fa-6f72-4d96-9e40-8fc997d72f1e",
  "idempotency_key": "reservation:demo-001:create",
  "occurred_at": "2026-05-31T09:00:00Z",
  "schema_version": 1,
  "retry_count": 0
}
```

**Payload** (routing key `inventory.reservation.requested.v1`):

```json
{
  "reservation_id": "demo-001",
  "order_id": "ord-demo-001",
  "sku": "sku-red-mug",
  "quantity": 2
}
```

After publishing, verify via HTTP:

```bash
curl -s http://localhost:8080/reservations/demo-001 | jq
curl -s http://localhost:8080/stock | jq
```

Release with routing key `inventory.reservation.release.requested.v1`:

```json
{
  "reservation_id": "demo-001",
  "reason": "order_cancelled"
}
```

Use idempotency key `reservation:demo-001:release`.

## 4. Observe metrics

```bash
curl -s http://localhost:8080/metrics | grep stockflow_erp_mock
```

Useful series:

| Metric | Meaning |
| --- | --- |
| `stockflow_erp_mock_confirmed_reservations_total` | Successful reservations |
| `stockflow_erp_mock_rejected_reservations_total` | Business rejections |
| `stockflow_erp_mock_idempotency_hits_total` | Safe duplicate processing |
| `stockflow_erp_mock_dlq_depth` | Messages waiting in DLQ |
| `stockflow_erp_mock_current_stock` | Available units per SKU |

## 5. Failure mode demos

### Business rejection

```bash
curl -s -X POST http://localhost:8080/debug/failure-mode \
  -H 'content-type: application/json' \
  --data '{"mode":"always_reject"}' | jq
```

Publish a reservation request. The mock responds with `rejected` / `FAILURE_MODE`
without changing stock. Reset:

```bash
curl -s -X POST http://localhost:8080/debug/failure-mode \
  -H 'content-type: application/json' \
  --data '{"mode":"normal"}' | jq
```

### Publish failure and retry

```bash
curl -s -X POST http://localhost:8080/debug/failure-mode \
  -H 'content-type: application/json' \
  --data '{"mode":"publish_failure"}' | jq
```

Publish a reservation. The handler succeeds but outcome publication fails → message
enters the retry queue. After TTL, redelivery hits idempotency → outcome publishes.
Watch metrics:

```bash
curl -s http://localhost:8080/metrics | grep -E 'idempotency_hits|failed_messages'
```

See [`failure-modes.md`](failure-modes.md) for the full mode catalogue.

### DLQ requeue

If messages exhaust retries, inspect DLQ depth:

```bash
curl -s http://localhost:8080/metrics | grep dlq_depth
```

Requeue up to 10 reservation request messages:

```bash
curl -s -X POST http://localhost:8080/debug/dlq/requeue \
  -H 'content-type: application/json' \
  --data '{"queue":"reservation_requests","limit":10}' | jq
```

## 6. Run automated tests

Unit tests (no Docker required):

```bash
make test
make vet
```

Integration tests (Docker required — Testcontainers starts RabbitMQ):

```bash
make test-integration
```

Or against the Compose RabbitMQ:

```bash
ERP_RABBITMQ_URL=amqp://stockflow:stockflow@localhost:5672/ \
  go test -tags=integration -count=1 ./internal/messaging/rabbitmq/...
```

CI runs unit tests, lint, and image build on every push — see
[`.github/workflows/ci.yml`](../.github/workflows/ci.yml).

## 7. Tear down

```bash
make docker-down
```

## Demo narrative (for portfolio presentations)

When presenting this project, the recommended story arc:

1. **Ecosystem** — StockFlow marketplace + ERP, payment, and delivery mocks
   ([architecture](architecture.md#stockflow-ecosystem)).
2. **Problem** — marketplace needs ERP stock reservation without a real ERP in dev.
3. **Contract** — show AsyncAPI and versioned routing keys (`v1`).
4. **Reliability** — diagram retry/DLQ topology from [`integration-flow.md`](integration-flow.md).
5. **Correctness** — demonstrate idempotency under `publish_failure`.
6. **Operability** — show Prometheus metrics and DLQ requeue.
7. **Quality** — mention unit + integration test split and CI pipeline.

Key trade-offs to mention verbally:

- in-memory state → fast sandbox, no persistence;
- application idempotency → safe retries, not cross-restart;
- failure modes → realistic chaos without external tooling.

## Related docs

- [Architecture](architecture.md) — system design and trade-offs
- [Integration flow](integration-flow.md) — message and topology reference
- [Failure modes](failure-modes.md) — chaos mode details
- [Payment demo](https://github.com/Smiley-Alyx/stockflow-payment-mock/blob/main/docs/demo.md) — sibling payment boundary
- [Delivery demo](https://github.com/Smiley-Alyx/stockflow-delivery-mock/blob/main/docs/demo.md) — sibling delivery boundary
