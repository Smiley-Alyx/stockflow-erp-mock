package app

import (
	"context"
	"testing"

	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/domain/inventory"
	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/storage/memory"
)

func TestInventoryReservationHandlerConfirmsReservation(t *testing.T) {
	handler := newTestInventoryReservationHandler(t, []memory.StockSeed{{SKU: "sku-1", AvailableQuantity: 10}})

	result, err := handler.HandleReservationRequested(context.Background(), ReservationRequest{
		ReservationID: "reservation-1",
		OrderID:       "order-1",
		SKU:           "sku-1",
		Quantity:      4,
	})

	if err != nil {
		t.Fatalf("HandleReservationRequested() error = %v", err)
	}
	if result.Decision != ReservationDecisionConfirmed {
		t.Errorf("Decision = %q, want %q", result.Decision, ReservationDecisionConfirmed)
	}
	if result.Reservation.Status != inventory.ReservationStatusActive {
		t.Errorf("Reservation.Status = %q, want %q", result.Reservation.Status, inventory.ReservationStatusActive)
	}
}

func TestInventoryReservationHandlerRejectsInsufficientStock(t *testing.T) {
	handler := newTestInventoryReservationHandler(t, []memory.StockSeed{{SKU: "sku-1", AvailableQuantity: 3}})

	result, err := handler.HandleReservationRequested(context.Background(), ReservationRequest{
		ReservationID: "reservation-1",
		OrderID:       "order-1",
		SKU:           "sku-1",
		Quantity:      4,
	})

	if err != nil {
		t.Fatalf("HandleReservationRequested() error = %v", err)
	}
	if result.Decision != ReservationDecisionRejected {
		t.Errorf("Decision = %q, want %q", result.Decision, ReservationDecisionRejected)
	}
	if result.RejectionReason != ReservationRejectionReasonInsufficientStock {
		t.Errorf("RejectionReason = %q, want %q", result.RejectionReason, ReservationRejectionReasonInsufficientStock)
	}
}

func newTestInventoryReservationHandler(t *testing.T, seed []memory.StockSeed) *InventoryReservationHandler {
	t.Helper()

	repository, err := memory.NewInventoryRepository(inventory.NewService(), seed)
	if err != nil {
		t.Fatalf("NewInventoryRepository() error = %v", err)
	}

	return NewInventoryReservationHandler(repository)
}
