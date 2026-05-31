package memory

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/domain/inventory"
)

func TestNewInventoryRepositorySeedsStock(t *testing.T) {
	repository := mustInventoryRepository(t, DefaultStockSeed())

	items, err := repository.ListStock(context.Background())
	if err != nil {
		t.Fatalf("ListStock() error = %v", err)
	}

	if len(items) != 3 {
		t.Fatalf("len(items) = %d, want %d", len(items), 3)
	}
	if items[0].SKU != "sku-black-bag" {
		t.Errorf("items[0].SKU = %q, want %q", items[0].SKU, "sku-black-bag")
	}
}

func TestNewInventoryRepositoryRejectsDuplicateSeed(t *testing.T) {
	_, err := NewInventoryRepository(inventory.NewService(), []StockSeed{
		{SKU: "sku-1", AvailableQuantity: 10},
		{SKU: " sku-1 ", AvailableQuantity: 20},
	})

	if !errors.Is(err, inventory.ErrInvalidArgument) {
		t.Fatalf("NewInventoryRepository() error = %v, want ErrInvalidArgument", err)
	}
}

func TestInventoryRepositorySetStock(t *testing.T) {
	repository := mustInventoryRepository(t, nil)

	item, err := repository.SetStock(context.Background(), " sku-1 ", 10)
	if err != nil {
		t.Fatalf("SetStock() error = %v", err)
	}

	if item.SKU != "sku-1" {
		t.Errorf("SKU = %q, want %q", item.SKU, "sku-1")
	}
	if item.AvailableQuantity != 10 {
		t.Errorf("AvailableQuantity = %d, want %d", item.AvailableQuantity, 10)
	}

	storedItem, err := repository.GetStock(context.Background(), "sku-1")
	if err != nil {
		t.Fatalf("GetStock() error = %v", err)
	}
	if storedItem != item {
		t.Errorf("GetStock() = %+v, want %+v", storedItem, item)
	}
}

func TestInventoryRepositorySetStockPreservesReservedQuantity(t *testing.T) {
	repository := mustInventoryRepository(t, []StockSeed{{SKU: "sku-1", AvailableQuantity: 10}})
	if _, err := repository.Reserve(context.Background(), "reservation-1", "sku-1", 4); err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}

	item, err := repository.SetStock(context.Background(), "sku-1", 20)
	if err != nil {
		t.Fatalf("SetStock() error = %v", err)
	}

	if item.AvailableQuantity != 20 {
		t.Errorf("AvailableQuantity = %d, want %d", item.AvailableQuantity, 20)
	}
	if item.ReservedQuantity != 4 {
		t.Errorf("ReservedQuantity = %d, want %d", item.ReservedQuantity, 4)
	}
}

func TestInventoryRepositoryReserveAndRelease(t *testing.T) {
	repository := mustInventoryRepository(t, []StockSeed{{SKU: "sku-1", AvailableQuantity: 10}})

	reservation, err := repository.Reserve(context.Background(), "reservation-1", "sku-1", 4)
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	if reservation.Status != inventory.ReservationStatusActive {
		t.Errorf("Status = %q, want %q", reservation.Status, inventory.ReservationStatusActive)
	}

	item, err := repository.GetStock(context.Background(), "sku-1")
	if err != nil {
		t.Fatalf("GetStock() error = %v", err)
	}
	if item.AvailableQuantity != 6 || item.ReservedQuantity != 4 {
		t.Errorf("GetStock() = %+v, want available 6 and reserved 4", item)
	}

	releasedReservation, err := repository.Release(context.Background(), reservation.ID)
	if err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if releasedReservation.Status != inventory.ReservationStatusReleased {
		t.Errorf("Status = %q, want %q", releasedReservation.Status, inventory.ReservationStatusReleased)
	}

	item, err = repository.GetStock(context.Background(), "sku-1")
	if err != nil {
		t.Fatalf("GetStock() error = %v", err)
	}
	if item.AvailableQuantity != 10 || item.ReservedQuantity != 0 {
		t.Errorf("GetStock() = %+v, want available 10 and reserved 0", item)
	}
}

func TestInventoryRepositoryRejectsDuplicateReservation(t *testing.T) {
	repository := mustInventoryRepository(t, []StockSeed{{SKU: "sku-1", AvailableQuantity: 10}})
	if _, err := repository.Reserve(context.Background(), "reservation-1", "sku-1", 4); err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}

	_, err := repository.Reserve(context.Background(), "reservation-1", "sku-1", 4)

	if !errors.Is(err, inventory.ErrReservationAlreadyExists) {
		t.Fatalf("Reserve() error = %v, want ErrReservationAlreadyExists", err)
	}
	item, err := repository.GetStock(context.Background(), "sku-1")
	if err != nil {
		t.Fatalf("GetStock() error = %v", err)
	}
	if item.AvailableQuantity != 6 {
		t.Errorf("AvailableQuantity = %d, want %d", item.AvailableQuantity, 6)
	}
}

func TestInventoryRepositoryReturnsNotFoundErrors(t *testing.T) {
	repository := mustInventoryRepository(t, nil)

	if _, err := repository.GetStock(context.Background(), "sku-1"); !errors.Is(err, inventory.ErrStockItemNotFound) {
		t.Fatalf("GetStock() error = %v, want ErrStockItemNotFound", err)
	}
	if _, err := repository.GetReservation(context.Background(), "reservation-1"); !errors.Is(err, inventory.ErrReservationNotFound) {
		t.Fatalf("GetReservation() error = %v, want ErrReservationNotFound", err)
	}
	if _, err := repository.Release(context.Background(), "reservation-1"); !errors.Is(err, inventory.ErrReservationNotFound) {
		t.Fatalf("Release() error = %v, want ErrReservationNotFound", err)
	}
}

func TestInventoryRepositoryHonorsCancelledContext(t *testing.T) {
	repository := mustInventoryRepository(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := repository.ListStock(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("ListStock() error = %v, want context.Canceled", err)
	}
}

func TestInventoryRepositoryPreventsConcurrentOverselling(t *testing.T) {
	const (
		availableQuantity = 100
		attempts          = 200
	)

	repository := mustInventoryRepository(t, []StockSeed{{SKU: "sku-1", AvailableQuantity: availableQuantity}})
	var successfulReservations atomic.Int64
	var insufficientStockErrors atomic.Int64
	var waitGroup sync.WaitGroup

	for index := range attempts {
		waitGroup.Add(1)

		go func() {
			defer waitGroup.Done()

			_, err := repository.Reserve(
				context.Background(),
				fmt.Sprintf("reservation-%d", index),
				"sku-1",
				1,
			)
			switch {
			case err == nil:
				successfulReservations.Add(1)
			case errors.Is(err, inventory.ErrInsufficientStock):
				insufficientStockErrors.Add(1)
			default:
				t.Errorf("Reserve() error = %v", err)
			}
		}()
	}

	waitGroup.Wait()

	if successfulReservations.Load() != availableQuantity {
		t.Errorf("successful reservations = %d, want %d", successfulReservations.Load(), availableQuantity)
	}
	if insufficientStockErrors.Load() != attempts-availableQuantity {
		t.Errorf("insufficient stock errors = %d, want %d", insufficientStockErrors.Load(), attempts-availableQuantity)
	}

	item, err := repository.GetStock(context.Background(), "sku-1")
	if err != nil {
		t.Fatalf("GetStock() error = %v", err)
	}
	if item.AvailableQuantity != 0 || item.ReservedQuantity != availableQuantity {
		t.Errorf("GetStock() = %+v, want available 0 and reserved %d", item, availableQuantity)
	}

	reservations, err := repository.ListReservations(context.Background())
	if err != nil {
		t.Fatalf("ListReservations() error = %v", err)
	}
	if len(reservations) != availableQuantity {
		t.Errorf("len(reservations) = %d, want %d", len(reservations), availableQuantity)
	}
}

func mustInventoryRepository(t *testing.T, seed []StockSeed) *InventoryRepository {
	t.Helper()

	repository, err := NewInventoryRepository(inventory.NewService(), seed)
	if err != nil {
		t.Fatalf("NewInventoryRepository() error = %v", err)
	}

	return repository
}
