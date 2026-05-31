//go:build integration

package rabbitmq

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/app"
	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/domain/inventory"
)

const integrationFlowTimeout = 15 * time.Second

func TestIntegrationReservationConfirmedFlow(t *testing.T) {
	env := newIntegrationEnvironment(t)
	resultQueue := env.bindResultQueue(ReservationConfirmedRoutingKey)

	const (
		sku           = "sku-red-mug"
		reservationID = "res-integration-confirmed"
		quantity      = 2
	)

	before := stockQuantity(env, sku)
	headers := newIntegrationHeaders("reservation:" + reservationID + ":create")

	env.publishReservationRequested(headers, integrationReservationRequestedPayload{
		ReservationID: reservationID,
		OrderID:       "ord-integration-confirmed",
		SKU:           sku,
		Quantity:      quantity,
	})

	delivery := env.waitForDelivery(resultQueue, integrationFlowTimeout)

	if delivery.Headers["correlation_id"] != headers.CorrelationID {
		t.Errorf("correlation_id = %v, want %q", delivery.Headers["correlation_id"], headers.CorrelationID)
	}
	if delivery.Headers["causation_id"] != headers.MessageID {
		t.Errorf("causation_id = %v, want %q", delivery.Headers["causation_id"], headers.MessageID)
	}
	if delivery.Headers["idempotency_key"] != headers.IdempotencyKey+":confirmed" {
		t.Errorf("idempotency_key = %v, want %q", delivery.Headers["idempotency_key"], headers.IdempotencyKey+":confirmed")
	}

	var payload reservationConfirmedPayload
	if err := json.Unmarshal(delivery.Body, &payload); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if payload.ReservationID != reservationID {
		t.Errorf("ReservationID = %q, want %q", payload.ReservationID, reservationID)
	}
	if payload.Quantity != quantity {
		t.Errorf("Quantity = %d, want %d", payload.Quantity, quantity)
	}

	after := stockQuantity(env, sku)
	if after.AvailableQuantity != before.AvailableQuantity-quantity {
		t.Errorf(
			"AvailableQuantity = %d, want %d",
			after.AvailableQuantity,
			before.AvailableQuantity-quantity,
		)
	}
	if after.ReservedQuantity != before.ReservedQuantity+quantity {
		t.Errorf(
			"ReservedQuantity = %d, want %d",
			after.ReservedQuantity,
			before.ReservedQuantity+quantity,
		)
	}

	reservation, err := env.repository.GetReservation(t.Context(), reservationID)
	if err != nil {
		t.Fatalf("GetReservation() error = %v", err)
	}
	if reservation.Status != inventory.ReservationStatusActive {
		t.Errorf("Status = %q, want %q", reservation.Status, inventory.ReservationStatusActive)
	}
}

func TestIntegrationReservationRejectedInsufficientStock(t *testing.T) {
	env := newIntegrationEnvironment(t)
	resultQueue := env.bindResultQueue(ReservationRejectedRoutingKey)

	const (
		sku           = "sku-red-mug"
		reservationID = "res-integration-rejected"
	)

	before := stockQuantity(env, sku)
	headers := newIntegrationHeaders("reservation:" + reservationID + ":create")

	env.publishReservationRequested(headers, integrationReservationRequestedPayload{
		ReservationID: reservationID,
		OrderID:       "ord-integration-rejected",
		SKU:           sku,
		Quantity:      before.AvailableQuantity + 1,
	})

	delivery := env.waitForDelivery(resultQueue, integrationFlowTimeout)

	var payload reservationRejectedPayload
	if err := json.Unmarshal(delivery.Body, &payload); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if payload.Reason != app.ReservationRejectionReasonInsufficientStock {
		t.Errorf("Reason = %q, want %q", payload.Reason, app.ReservationRejectionReasonInsufficientStock)
	}

	after := stockQuantity(env, sku)
	if after.AvailableQuantity != before.AvailableQuantity {
		t.Errorf("AvailableQuantity = %d, want %d", after.AvailableQuantity, before.AvailableQuantity)
	}
	if after.ReservedQuantity != before.ReservedQuantity {
		t.Errorf("ReservedQuantity = %d, want %d", after.ReservedQuantity, before.ReservedQuantity)
	}
}

func TestIntegrationReservationIdempotency(t *testing.T) {
	env := newIntegrationEnvironment(t)
	resultQueue := env.bindResultQueue(ReservationConfirmedRoutingKey)

	const (
		sku           = "sku-blue-notebook"
		reservationID = "res-integration-idempotent"
		quantity      = 3
	)

	before := stockQuantity(env, sku)
	headers := newIntegrationHeaders("reservation:" + reservationID + ":create")
	request := integrationReservationRequestedPayload{
		ReservationID: reservationID,
		OrderID:       "ord-integration-idempotent",
		SKU:           sku,
		Quantity:      quantity,
	}

	env.publishReservationRequested(headers, request)
	firstDelivery := env.waitForDelivery(resultQueue, integrationFlowTimeout)

	retryHeaders := headers
	retryHeaders.MessageID = headers.CausationID
	retryHeaders.CausationID = headers.MessageID
	env.publishReservationRequested(retryHeaders, request)
	secondDelivery := env.waitForDelivery(resultQueue, integrationFlowTimeout)

	var firstPayload reservationConfirmedPayload
	if err := json.Unmarshal(firstDelivery.Body, &firstPayload); err != nil {
		t.Fatalf("Unmarshal first payload: %v", err)
	}
	var secondPayload reservationConfirmedPayload
	if err := json.Unmarshal(secondDelivery.Body, &secondPayload); err != nil {
		t.Fatalf("Unmarshal second payload: %v", err)
	}
	if firstPayload.ReservationID != secondPayload.ReservationID {
		t.Errorf("ReservationID mismatch: %q vs %q", firstPayload.ReservationID, secondPayload.ReservationID)
	}

	after := stockQuantity(env, sku)
	if after.AvailableQuantity != before.AvailableQuantity-quantity {
		t.Errorf(
			"AvailableQuantity = %d, want %d",
			after.AvailableQuantity,
			before.AvailableQuantity-quantity,
		)
	}
	if after.ReservedQuantity != before.ReservedQuantity+quantity {
		t.Errorf(
			"ReservedQuantity = %d, want %d",
			after.ReservedQuantity,
			before.ReservedQuantity+quantity,
		)
	}
}

func TestIntegrationReservationReleaseFlow(t *testing.T) {
	env := newIntegrationEnvironment(t)
	confirmedQueue := env.bindResultQueue(ReservationConfirmedRoutingKey)
	releasedQueue := env.bindResultQueue(ReservationReleasedRoutingKey)

	const (
		sku           = "sku-black-bag"
		reservationID = "res-integration-release"
		quantity      = 2
	)

	before := stockQuantity(env, sku)
	createHeaders := newIntegrationHeaders("reservation:" + reservationID + ":create")

	env.publishReservationRequested(createHeaders, integrationReservationRequestedPayload{
		ReservationID: reservationID,
		OrderID:       "ord-integration-release",
		SKU:           sku,
		Quantity:      quantity,
	})
	if delivery := env.waitForDelivery(confirmedQueue, integrationFlowTimeout); delivery.Body == nil {
		t.Fatal("expected reservation confirmation")
	}

	reserved := stockQuantity(env, sku)
	if reserved.AvailableQuantity != before.AvailableQuantity-quantity {
		t.Errorf(
			"AvailableQuantity after reserve = %d, want %d",
			reserved.AvailableQuantity,
			before.AvailableQuantity-quantity,
		)
	}

	releaseHeaders := newIntegrationHeaders("reservation:" + reservationID + ":release")
	releaseHeaders.CorrelationID = createHeaders.CorrelationID
	releaseHeaders.CausationID = createHeaders.MessageID

	env.publishReservationReleaseRequested(releaseHeaders, integrationReservationReleaseRequestedPayload{
		ReservationID: reservationID,
		Reason:        "order_cancelled",
	})

	releaseDelivery := env.waitForDelivery(releasedQueue, integrationFlowTimeout)

	var payload reservationReleasedPayload
	if err := json.Unmarshal(releaseDelivery.Body, &payload); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if payload.ReservationID != reservationID {
		t.Errorf("ReservationID = %q, want %q", payload.ReservationID, reservationID)
	}

	after := stockQuantity(env, sku)
	if after.AvailableQuantity != before.AvailableQuantity {
		t.Errorf("AvailableQuantity = %d, want %d", after.AvailableQuantity, before.AvailableQuantity)
	}
	if after.ReservedQuantity != before.ReservedQuantity {
		t.Errorf("ReservedQuantity = %d, want %d", after.ReservedQuantity, before.ReservedQuantity)
	}

	reservation, err := env.repository.GetReservation(t.Context(), reservationID)
	if err != nil {
		t.Fatalf("GetReservation() error = %v", err)
	}
	if reservation.Status != inventory.ReservationStatusReleased {
		t.Errorf("Status = %q, want %q", reservation.Status, inventory.ReservationStatusReleased)
	}
}

func TestIntegrationReservationReleaseNotFound(t *testing.T) {
	env := newIntegrationEnvironment(t)
	resultQueue := env.bindResultQueue(ReservationReleaseFailedRoutingKey)

	const reservationID = "res-integration-missing"
	headers := newIntegrationHeaders("reservation:" + reservationID + ":release")

	env.publishReservationReleaseRequested(headers, integrationReservationReleaseRequestedPayload{
		ReservationID: reservationID,
		Reason:        "order_cancelled",
	})

	delivery := env.waitForDelivery(resultQueue, integrationFlowTimeout)

	var payload reservationReleaseFailedPayload
	if err := json.Unmarshal(delivery.Body, &payload); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if payload.Reason != app.ReservationReleaseFailureReasonNotFound {
		t.Errorf("Reason = %q, want %q", payload.Reason, app.ReservationReleaseFailureReasonNotFound)
	}
}
