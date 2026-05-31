# Failure modes

The HTTP admin API can enable one sandbox failure mode at a time. Settings are
kept in memory and reset to `normal` when the service restarts.

```bash
curl -X POST http://localhost:8080/debug/failure-mode \
  -H 'content-type: application/json' \
  --data '{"mode":"always_reject"}'
```

## Available modes

| Mode | Effect |
| --- | --- |
| `normal` | Process messages without simulated failures. |
| `always_reject` | Reject every reservation request with `FAILURE_MODE` without changing stock. |
| `random_reject` | Reject reservation requests according to `random_reject_probability`. |
| `processing_delay` | Delay reservation and release request handling by `processing_delay_ms`. |
| `publish_failure` | Fail reservation result publication before RabbitMQ I/O. Retry and DLQ routing remain active. |
| `duplicate_response` | Publish each reservation result twice while applying the inventory operation once. |

Reservation outcomes produced by rejection modes are idempotent. Reusing the
same `idempotency_key` returns the first logical outcome even if the active mode
has changed.

## Random rejection

```bash
curl -X POST http://localhost:8080/debug/failure-mode \
  -H 'content-type: application/json' \
  --data '{"mode":"random_reject","random_reject_probability":0.25}'
```

The probability must be greater than `0` and less than or equal to `1`.

## Processing delay

```bash
curl -X POST http://localhost:8080/debug/failure-mode \
  -H 'content-type: application/json' \
  --data '{"mode":"processing_delay","processing_delay_ms":500}'
```

Delayed handling observes context cancellation so service shutdown is not
blocked by an active timer.

## Reset

```bash
curl -X POST http://localhost:8080/debug/failure-mode \
  -H 'content-type: application/json' \
  --data '{"mode":"normal"}'
```
