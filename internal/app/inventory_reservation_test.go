package app

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/domain/inventory"
	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/storage/memory"
)

func TestInventoryReservationHandlerConfirmsReservation(t *testing.T) {
	handler, _ := newTestInventoryReservationHandler(t, []memory.StockSeed{{SKU: "sku-1", AvailableQuantity: 10}})

	result, err := handler.HandleReservationRequested(context.Background(), ReservationRequest{
		Metadata:      MessageMetadata{IdempotencyKey: "reservation:reservation-1:create"},
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
	handler, _ := newTestInventoryReservationHandler(t, []memory.StockSeed{{SKU: "sku-1", AvailableQuantity: 3}})

	result, err := handler.HandleReservationRequested(context.Background(), ReservationRequest{
		Metadata:      MessageMetadata{IdempotencyKey: "reservation:reservation-1:create"},
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

func TestInventoryReservationHandlerReturnsCachedResult(t *testing.T) {
	handler, repository := newTestInventoryReservationHandler(t, []memory.StockSeed{{SKU: "sku-1", AvailableQuantity: 10}})
	request := ReservationRequest{
		Metadata:      MessageMetadata{MessageID: "message-1", IdempotencyKey: "reservation:reservation-1:create"},
		ReservationID: "reservation-1",
		OrderID:       "order-1",
		SKU:           "sku-1",
		Quantity:      4,
	}
	if _, err := handler.HandleReservationRequested(context.Background(), request); err != nil {
		t.Fatalf("first HandleReservationRequested() error = %v", err)
	}

	request.Metadata.MessageID = "message-2"
	result, err := handler.HandleReservationRequested(context.Background(), request)
	if err != nil {
		t.Fatalf("second HandleReservationRequested() error = %v", err)
	}

	if !result.IdempotencyHit {
		t.Error("IdempotencyHit = false, want true")
	}
	if result.Request.Metadata.MessageID != "message-2" {
		t.Errorf("MessageID = %q, want %q", result.Request.Metadata.MessageID, "message-2")
	}

	item, err := repository.GetStock(context.Background(), "sku-1")
	if err != nil {
		t.Fatalf("GetStock() error = %v", err)
	}
	if item.AvailableQuantity != 6 || item.ReservedQuantity != 4 {
		t.Errorf("GetStock() = %+v, want available 6 and reserved 4", item)
	}
}

func TestInventoryReservationHandlerRejectsIdempotencyConflict(t *testing.T) {
	handler, _ := newTestInventoryReservationHandler(t, []memory.StockSeed{{SKU: "sku-1", AvailableQuantity: 10}})
	request := ReservationRequest{
		Metadata:      MessageMetadata{IdempotencyKey: "reservation:reservation-1:create"},
		ReservationID: "reservation-1",
		OrderID:       "order-1",
		SKU:           "sku-1",
		Quantity:      4,
	}
	if _, err := handler.HandleReservationRequested(context.Background(), request); err != nil {
		t.Fatalf("first HandleReservationRequested() error = %v", err)
	}

	request.Quantity = 5
	if _, err := handler.HandleReservationRequested(context.Background(), request); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("second HandleReservationRequested() error = %v, want ErrIdempotencyConflict", err)
	}
}

func TestInventoryReservationHandlerCoordinatesConcurrentDuplicates(t *testing.T) {
	const attempts = 100

	handler, repository := newTestInventoryReservationHandler(t, []memory.StockSeed{{SKU: "sku-1", AvailableQuantity: 10}})
	request := ReservationRequest{
		Metadata:      MessageMetadata{IdempotencyKey: "reservation:reservation-1:create"},
		ReservationID: "reservation-1",
		OrderID:       "order-1",
		SKU:           "sku-1",
		Quantity:      4,
	}
	var idempotencyHits atomic.Int64
	var waitGroup sync.WaitGroup

	for range attempts {
		waitGroup.Add(1)

		go func() {
			defer waitGroup.Done()

			result, err := handler.HandleReservationRequested(context.Background(), request)
			if err != nil {
				t.Errorf("HandleReservationRequested() error = %v", err)
				return
			}
			if result.IdempotencyHit {
				idempotencyHits.Add(1)
			}
		}()
	}

	waitGroup.Wait()

	if idempotencyHits.Load() != attempts-1 {
		t.Errorf("idempotency hits = %d, want %d", idempotencyHits.Load(), attempts-1)
	}

	item, err := repository.GetStock(context.Background(), "sku-1")
	if err != nil {
		t.Fatalf("GetStock() error = %v", err)
	}
	if item.AvailableQuantity != 6 || item.ReservedQuantity != 4 {
		t.Errorf("GetStock() = %+v, want available 6 and reserved 4", item)
	}
}

func newTestInventoryReservationHandler(
	t *testing.T,
	seed []memory.StockSeed,
) (*InventoryReservationHandler, *memory.InventoryRepository) {
	t.Helper()

	repository, err := memory.NewInventoryRepository(inventory.NewService(), seed)
	if err != nil {
		t.Fatalf("NewInventoryRepository() error = %v", err)
	}

	return NewInventoryReservationHandler(repository, memory.NewIdempotencyStore[ReservationResult]()), repository
}
