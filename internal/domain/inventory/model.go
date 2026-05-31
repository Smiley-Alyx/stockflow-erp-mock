package inventory

import (
	"fmt"
	"strings"
	"time"
)

type ReservationStatus string

const (
	ReservationStatusActive   ReservationStatus = "active"
	ReservationStatusReleased ReservationStatus = "released"
)

type StockItem struct {
	SKU               string
	AvailableQuantity int
	ReservedQuantity  int
}

func NewStockItem(sku string, availableQuantity int) (*StockItem, error) {
	item := &StockItem{
		SKU:               strings.TrimSpace(sku),
		AvailableQuantity: availableQuantity,
	}
	if err := item.validate(); err != nil {
		return nil, err
	}

	return item, nil
}

func (i *StockItem) validate() error {
	if i == nil {
		return fmt.Errorf("%w: stock item is required", ErrInvalidArgument)
	}
	if strings.TrimSpace(i.SKU) == "" {
		return fmt.Errorf("%w: stock item SKU is required", ErrInvalidArgument)
	}
	if i.AvailableQuantity < 0 {
		return fmt.Errorf("%w: available quantity must not be negative", ErrStockInvariantViolation)
	}
	if i.ReservedQuantity < 0 {
		return fmt.Errorf("%w: reserved quantity must not be negative", ErrStockInvariantViolation)
	}

	return nil
}

type Reservation struct {
	ID         string
	SKU        string
	Quantity   int
	Status     ReservationStatus
	CreatedAt  time.Time
	ReleasedAt *time.Time
}

func (r *Reservation) validate() error {
	if r == nil {
		return fmt.Errorf("%w: reservation is required", ErrInvalidArgument)
	}
	if strings.TrimSpace(r.ID) == "" {
		return fmt.Errorf("%w: reservation ID is required", ErrInvalidArgument)
	}
	if strings.TrimSpace(r.SKU) == "" {
		return fmt.Errorf("%w: reservation SKU is required", ErrInvalidArgument)
	}
	if r.Quantity <= 0 {
		return fmt.Errorf("%w: reservation quantity must be positive", ErrInvalidArgument)
	}

	switch r.Status {
	case ReservationStatusActive, ReservationStatusReleased:
		return nil
	default:
		return fmt.Errorf("%w: unsupported reservation status %q", ErrInvalidArgument, r.Status)
	}
}
