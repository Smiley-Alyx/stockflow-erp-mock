package rabbitmq

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/app"
	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/domain/inventory"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestNewReservationResultMessageMapsConfirmedResult(t *testing.T) {
	occurredAt := time.Date(2026, time.May, 31, 9, 0, 1, 0, time.UTC)
	result := reservationResultFixture()

	routingKey, publishing, err := newReservationResultMessage(
		result,
		"3e1b4ca3-d256-49db-915a-b93807cc4e88",
		occurredAt,
	)

	if err != nil {
		t.Fatalf("newReservationResultMessage() error = %v", err)
	}
	if routingKey != ReservationConfirmedRoutingKey {
		t.Errorf("routingKey = %q, want %q", routingKey, ReservationConfirmedRoutingKey)
	}
	if publishing.DeliveryMode != amqp.Persistent {
		t.Errorf("DeliveryMode = %d, want %d", publishing.DeliveryMode, amqp.Persistent)
	}
	if publishing.Headers["correlation_id"] != result.Request.Metadata.CorrelationID {
		t.Errorf("correlation_id = %q", publishing.Headers["correlation_id"])
	}
	if publishing.Headers["causation_id"] != result.Request.Metadata.MessageID {
		t.Errorf("causation_id = %q", publishing.Headers["causation_id"])
	}
	if publishing.Headers["idempotency_key"] != "reservation:res-10001:create:confirmed" {
		t.Errorf("idempotency_key = %q", publishing.Headers["idempotency_key"])
	}

	var payload reservationConfirmedPayload
	if err := json.Unmarshal(publishing.Body, &payload); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if payload.ReservationID != result.Request.ReservationID {
		t.Errorf("ReservationID = %q, want %q", payload.ReservationID, result.Request.ReservationID)
	}
}

func TestNewReservationResultMessageMapsRejectedResult(t *testing.T) {
	result := reservationResultFixture()
	result.Decision = app.ReservationDecisionRejected
	result.RejectionReason = app.ReservationRejectionReasonInsufficientStock

	routingKey, publishing, err := newReservationResultMessage(
		result,
		"8d4d7046-8d5f-463b-b25d-e12d44611124",
		time.Date(2026, time.May, 31, 9, 0, 1, 0, time.UTC),
	)

	if err != nil {
		t.Fatalf("newReservationResultMessage() error = %v", err)
	}
	if routingKey != ReservationRejectedRoutingKey {
		t.Errorf("routingKey = %q, want %q", routingKey, ReservationRejectedRoutingKey)
	}

	var payload reservationRejectedPayload
	if err := json.Unmarshal(publishing.Body, &payload); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if payload.Reason != app.ReservationRejectionReasonInsufficientStock {
		t.Errorf("Reason = %q, want %q", payload.Reason, app.ReservationRejectionReasonInsufficientStock)
	}
}

func TestNewReservationResultMessageRejectsUnsupportedDecision(t *testing.T) {
	result := reservationResultFixture()
	result.Decision = "unknown"

	if _, _, err := newReservationResultMessage(result, "message-id", time.Now()); err == nil {
		t.Fatal("newReservationResultMessage() error = nil, want an error")
	}
}

func TestNewUUID(t *testing.T) {
	messageID, err := newUUID()
	if err != nil {
		t.Fatalf("newUUID() error = %v", err)
	}
	if !uuidPattern.MatchString(messageID) {
		t.Errorf("newUUID() = %q, want UUID", messageID)
	}
}

func reservationResultFixture() app.ReservationResult {
	return app.ReservationResult{
		Decision: app.ReservationDecisionConfirmed,
		Request: app.ReservationRequest{
			Metadata: app.MessageMetadata{
				MessageID:      "58d867f6-69e0-4f2f-b1ee-d587aaa48b6e",
				CorrelationID:  "bb8d8f75-5210-4038-98cc-f2237d192ff8",
				IdempotencyKey: "reservation:res-10001:create",
			},
			ReservationID: "res-10001",
			OrderID:       "ord-10001",
			SKU:           "sku-red-mug",
			Quantity:      2,
		},
		Reservation: inventory.Reservation{
			ID:        "res-10001",
			SKU:       "sku-red-mug",
			Quantity:  2,
			Status:    inventory.ReservationStatusActive,
			CreatedAt: time.Date(2026, time.May, 31, 9, 0, 1, 0, time.UTC),
		},
	}
}
