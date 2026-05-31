package inventory

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidArgument         = errors.New("invalid argument")
	ErrInsufficientStock       = errors.New("insufficient stock")
	ErrReservationNotActive    = errors.New("reservation is not active")
	ErrStockInvariantViolation = errors.New("stock invariant violation")
)

type InsufficientStockError struct {
	SKU       string
	Requested int
	Available int
}

func (e *InsufficientStockError) Error() string {
	return fmt.Sprintf(
		"%s: sku %q requested %d units, %d units available",
		ErrInsufficientStock,
		e.SKU,
		e.Requested,
		e.Available,
	)
}

func (e *InsufficientStockError) Unwrap() error {
	return ErrInsufficientStock
}
