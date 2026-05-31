package inventory

import (
	"errors"
	"testing"
	"time"
)

func TestNewStockItem(t *testing.T) {
	t.Run("creates stock item", func(t *testing.T) {
		item, err := NewStockItem(" sku-1 ", 10)
		if err != nil {
			t.Fatalf("NewStockItem() error = %v", err)
		}

		if item.SKU != "sku-1" {
			t.Errorf("SKU = %q, want %q", item.SKU, "sku-1")
		}
		if item.AvailableQuantity != 10 {
			t.Errorf("AvailableQuantity = %d, want %d", item.AvailableQuantity, 10)
		}
	})

	tests := []struct {
		name              string
		sku               string
		availableQuantity int
	}{
		{name: "empty SKU", sku: " ", availableQuantity: 10},
		{name: "negative quantity", sku: "sku-1", availableQuantity: -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewStockItem(tt.sku, tt.availableQuantity); err == nil {
				t.Fatal("NewStockItem() error = nil, want an error")
			}
		})
	}
}

func TestServiceReserve(t *testing.T) {
	now := time.Date(2026, time.May, 31, 10, 0, 0, 0, time.FixedZone("UTC+2", 2*60*60))
	service := newServiceWithTime(now)
	item := mustStockItem(t, "sku-1", 10)

	reservation, err := service.Reserve(item, " reservation-1 ", 4)
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}

	if item.AvailableQuantity != 6 {
		t.Errorf("AvailableQuantity = %d, want %d", item.AvailableQuantity, 6)
	}
	if item.ReservedQuantity != 4 {
		t.Errorf("ReservedQuantity = %d, want %d", item.ReservedQuantity, 4)
	}
	if reservation.ID != "reservation-1" {
		t.Errorf("ID = %q, want %q", reservation.ID, "reservation-1")
	}
	if reservation.SKU != "sku-1" {
		t.Errorf("SKU = %q, want %q", reservation.SKU, "sku-1")
	}
	if reservation.Status != ReservationStatusActive {
		t.Errorf("Status = %q, want %q", reservation.Status, ReservationStatusActive)
	}
	if reservation.CreatedAt != now.UTC() {
		t.Errorf("CreatedAt = %v, want %v", reservation.CreatedAt, now.UTC())
	}
}

func TestServiceReserveRejectsInsufficientStock(t *testing.T) {
	service := NewService()
	item := mustStockItem(t, "sku-1", 3)

	_, err := service.Reserve(item, "reservation-1", 4)

	var insufficientStockError *InsufficientStockError
	if !errors.As(err, &insufficientStockError) {
		t.Fatalf("Reserve() error = %v, want InsufficientStockError", err)
	}
	if !errors.Is(err, ErrInsufficientStock) {
		t.Fatalf("Reserve() error = %v, want ErrInsufficientStock", err)
	}
	if item.AvailableQuantity != 3 {
		t.Errorf("AvailableQuantity = %d, want %d", item.AvailableQuantity, 3)
	}
	if item.ReservedQuantity != 0 {
		t.Errorf("ReservedQuantity = %d, want %d", item.ReservedQuantity, 0)
	}
}

func TestServiceReserveRejectsInvalidArguments(t *testing.T) {
	service := NewService()

	tests := []struct {
		name          string
		item          *StockItem
		reservationID string
		quantity      int
	}{
		{name: "nil stock item", item: nil, reservationID: "reservation-1", quantity: 1},
		{name: "empty reservation ID", item: mustStockItem(t, "sku-1", 10), reservationID: " ", quantity: 1},
		{name: "zero quantity", item: mustStockItem(t, "sku-1", 10), reservationID: "reservation-1", quantity: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := service.Reserve(tt.item, tt.reservationID, tt.quantity); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("Reserve() error = %v, want ErrInvalidArgument", err)
			}
		})
	}
}

func TestServiceRelease(t *testing.T) {
	now := time.Date(2026, time.May, 31, 10, 0, 0, 0, time.UTC)
	releasedAt := now.Add(time.Minute)
	service := newServiceWithTimes(now, releasedAt)
	item := mustStockItem(t, "sku-1", 10)
	reservation, err := service.Reserve(item, "reservation-1", 4)
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}

	if err := service.Release(item, reservation); err != nil {
		t.Fatalf("Release() error = %v", err)
	}

	if item.AvailableQuantity != 10 {
		t.Errorf("AvailableQuantity = %d, want %d", item.AvailableQuantity, 10)
	}
	if item.ReservedQuantity != 0 {
		t.Errorf("ReservedQuantity = %d, want %d", item.ReservedQuantity, 0)
	}
	if reservation.Status != ReservationStatusReleased {
		t.Errorf("Status = %q, want %q", reservation.Status, ReservationStatusReleased)
	}
	if reservation.ReleasedAt == nil || *reservation.ReleasedAt != releasedAt {
		t.Errorf("ReleasedAt = %v, want %v", reservation.ReleasedAt, releasedAt)
	}
}

func TestServiceReleaseRejectsRepeatedRelease(t *testing.T) {
	service := NewService()
	item := mustStockItem(t, "sku-1", 10)
	reservation, err := service.Reserve(item, "reservation-1", 4)
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	if err := service.Release(item, reservation); err != nil {
		t.Fatalf("Release() error = %v", err)
	}

	err = service.Release(item, reservation)

	if !errors.Is(err, ErrReservationNotActive) {
		t.Fatalf("Release() error = %v, want ErrReservationNotActive", err)
	}
	if item.AvailableQuantity != 10 {
		t.Errorf("AvailableQuantity = %d, want %d", item.AvailableQuantity, 10)
	}
}

func TestServiceReleaseRejectsStockInvariantViolation(t *testing.T) {
	service := NewService()
	item := mustStockItem(t, "sku-1", 10)
	reservation := &Reservation{
		ID:        "reservation-1",
		SKU:       "sku-1",
		Quantity:  4,
		Status:    ReservationStatusActive,
		CreatedAt: time.Now(),
	}

	err := service.Release(item, reservation)

	if !errors.Is(err, ErrStockInvariantViolation) {
		t.Fatalf("Release() error = %v, want ErrStockInvariantViolation", err)
	}
	if item.AvailableQuantity != 10 {
		t.Errorf("AvailableQuantity = %d, want %d", item.AvailableQuantity, 10)
	}
	if reservation.Status != ReservationStatusActive {
		t.Errorf("Status = %q, want %q", reservation.Status, ReservationStatusActive)
	}
}

func mustStockItem(t *testing.T, sku string, quantity int) *StockItem {
	t.Helper()

	item, err := NewStockItem(sku, quantity)
	if err != nil {
		t.Fatalf("NewStockItem() error = %v", err)
	}

	return item
}

func newServiceWithTime(now time.Time) *Service {
	return newServiceWithTimes(now)
}

func newServiceWithTimes(times ...time.Time) *Service {
	index := 0

	return &Service{
		now: func() time.Time {
			currentTime := times[index]
			index++
			return currentTime
		},
	}
}
