package inventory

import (
	"fmt"
	"strings"
	"time"
)

type Service struct {
	now func() time.Time
}

func NewService() *Service {
	return &Service{now: time.Now}
}

func (s *Service) Reserve(item *StockItem, reservationID string, quantity int) (*Reservation, error) {
	if err := item.validate(); err != nil {
		return nil, err
	}

	reservationID = strings.TrimSpace(reservationID)
	if reservationID == "" {
		return nil, fmt.Errorf("%w: reservation ID is required", ErrInvalidArgument)
	}
	if quantity <= 0 {
		return nil, fmt.Errorf("%w: reservation quantity must be positive", ErrInvalidArgument)
	}
	if item.AvailableQuantity < quantity {
		return nil, &InsufficientStockError{
			SKU:       item.SKU,
			Requested: quantity,
			Available: item.AvailableQuantity,
		}
	}

	item.AvailableQuantity -= quantity
	item.ReservedQuantity += quantity

	return &Reservation{
		ID:        reservationID,
		SKU:       item.SKU,
		Quantity:  quantity,
		Status:    ReservationStatusActive,
		CreatedAt: s.currentTime(),
	}, nil
}

func (s *Service) Release(item *StockItem, reservation *Reservation) error {
	if err := item.validate(); err != nil {
		return err
	}
	if err := reservation.validate(); err != nil {
		return err
	}
	if reservation.Status != ReservationStatusActive {
		return ErrReservationNotActive
	}
	if item.SKU != reservation.SKU {
		return fmt.Errorf(
			"%w: reservation SKU %q does not match stock item SKU %q",
			ErrInvalidArgument,
			reservation.SKU,
			item.SKU,
		)
	}
	if item.ReservedQuantity < reservation.Quantity {
		return fmt.Errorf(
			"%w: sku %q has %d reserved units, reservation %q requires %d units",
			ErrStockInvariantViolation,
			item.SKU,
			item.ReservedQuantity,
			reservation.ID,
			reservation.Quantity,
		)
	}

	item.AvailableQuantity += reservation.Quantity
	item.ReservedQuantity -= reservation.Quantity

	releasedAt := s.currentTime()
	reservation.Status = ReservationStatusReleased
	reservation.ReleasedAt = &releasedAt

	return nil
}

func (s *Service) currentTime() time.Time {
	return s.now().UTC()
}
