package inventory

import "context"

type Repository interface {
	SetStock(ctx context.Context, sku string, availableQuantity int) (StockItem, error)
	GetStock(ctx context.Context, sku string) (StockItem, error)
	ListStock(ctx context.Context) ([]StockItem, error)
	Reserve(ctx context.Context, reservationID, sku string, quantity int) (Reservation, error)
	Release(ctx context.Context, reservationID string) (Reservation, error)
	GetReservation(ctx context.Context, reservationID string) (Reservation, error)
	ListReservations(ctx context.Context) ([]Reservation, error)
}
