package app

import (
	"context"
	"testing"

	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/domain/inventory"
	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/storage/memory"
)

func TestInventoryReservationReleaseHandlerReleasesReservation(t *testing.T) {
	handler, repository := newTestInventoryReservationReleaseHandler(t)

	result, err := handler.HandleReservationReleaseRequested(context.Background(), ReservationReleaseRequest{
		Metadata:      MessageMetadata{IdempotencyKey: "reservation:reservation-1:release"},
		ReservationID: "reservation-1",
		Reason:        "order_cancelled",
	})

	if err != nil {
		t.Fatalf("HandleReservationReleaseRequested() error = %v", err)
	}
	if result.Decision != ReservationReleaseDecisionReleased {
		t.Errorf("Decision = %q, want %q", result.Decision, ReservationReleaseDecisionReleased)
	}

	item, err := repository.GetStock(context.Background(), "sku-1")
	if err != nil {
		t.Fatalf("GetStock() error = %v", err)
	}
	if item.AvailableQuantity != 10 || item.ReservedQuantity != 0 {
		t.Errorf("GetStock() = %+v, want available 10 and reserved 0", item)
	}
}

func TestInventoryReservationReleaseHandlerReturnsCachedResult(t *testing.T) {
	handler, _ := newTestInventoryReservationReleaseHandler(t)
	request := ReservationReleaseRequest{
		Metadata:      MessageMetadata{MessageID: "message-1", IdempotencyKey: "reservation:reservation-1:release"},
		ReservationID: "reservation-1",
		Reason:        "order_cancelled",
	}
	if _, err := handler.HandleReservationReleaseRequested(context.Background(), request); err != nil {
		t.Fatalf("first HandleReservationReleaseRequested() error = %v", err)
	}

	request.Metadata.MessageID = "message-2"
	result, err := handler.HandleReservationReleaseRequested(context.Background(), request)
	if err != nil {
		t.Fatalf("second HandleReservationReleaseRequested() error = %v", err)
	}
	if !result.IdempotencyHit {
		t.Error("IdempotencyHit = false, want true")
	}
	if result.Decision != ReservationReleaseDecisionReleased {
		t.Errorf("Decision = %q, want %q", result.Decision, ReservationReleaseDecisionReleased)
	}
	if result.Request.Metadata.MessageID != "message-2" {
		t.Errorf("MessageID = %q, want %q", result.Request.Metadata.MessageID, "message-2")
	}
}

func TestInventoryReservationReleaseHandlerReturnsFailedResult(t *testing.T) {
	repository, err := memory.NewInventoryRepository(inventory.NewService(), nil)
	if err != nil {
		t.Fatalf("NewInventoryRepository() error = %v", err)
	}
	handler := NewInventoryReservationReleaseHandler(
		repository,
		memory.NewIdempotencyStore[ReservationReleaseResult](),
	)

	result, err := handler.HandleReservationReleaseRequested(context.Background(), ReservationReleaseRequest{
		Metadata:      MessageMetadata{IdempotencyKey: "reservation:missing:release"},
		ReservationID: "missing",
	})

	if err != nil {
		t.Fatalf("HandleReservationReleaseRequested() error = %v", err)
	}
	if result.Decision != ReservationReleaseDecisionFailed {
		t.Errorf("Decision = %q, want %q", result.Decision, ReservationReleaseDecisionFailed)
	}
	if result.FailureReason != ReservationReleaseFailureReasonNotFound {
		t.Errorf("FailureReason = %q, want %q", result.FailureReason, ReservationReleaseFailureReasonNotFound)
	}
}

func newTestInventoryReservationReleaseHandler(
	t *testing.T,
) (*InventoryReservationReleaseHandler, *memory.InventoryRepository) {
	t.Helper()

	repository, err := memory.NewInventoryRepository(
		inventory.NewService(),
		[]memory.StockSeed{{SKU: "sku-1", AvailableQuantity: 10}},
	)
	if err != nil {
		t.Fatalf("NewInventoryRepository() error = %v", err)
	}
	if _, err := repository.Reserve(context.Background(), "reservation-1", "sku-1", 4); err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}

	return NewInventoryReservationReleaseHandler(
		repository,
		memory.NewIdempotencyStore[ReservationReleaseResult](),
	), repository
}
