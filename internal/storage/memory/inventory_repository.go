package memory

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/domain/inventory"
)

type StockSeed struct {
	SKU               string
	AvailableQuantity int
}

func DefaultStockSeed() []StockSeed {
	return []StockSeed{
		{SKU: "sku-red-mug", AvailableQuantity: 120},
		{SKU: "sku-blue-notebook", AvailableQuantity: 80},
		{SKU: "sku-black-bag", AvailableQuantity: 40},
	}
}

type InventoryRepository struct {
	mu           sync.RWMutex
	service      *inventory.Service
	stock        map[string]*inventory.StockItem
	reservations map[string]*inventory.Reservation
}

var _ inventory.Repository = (*InventoryRepository)(nil)

func NewInventoryRepository(service *inventory.Service, seed []StockSeed) (*InventoryRepository, error) {
	if service == nil {
		return nil, fmt.Errorf("%w: inventory service is required", inventory.ErrInvalidArgument)
	}

	repository := &InventoryRepository{
		service:      service,
		stock:        make(map[string]*inventory.StockItem, len(seed)),
		reservations: make(map[string]*inventory.Reservation),
	}

	for _, seedItem := range seed {
		item, err := inventory.NewStockItem(seedItem.SKU, seedItem.AvailableQuantity)
		if err != nil {
			return nil, fmt.Errorf("seed stock item %q: %w", seedItem.SKU, err)
		}
		if _, exists := repository.stock[item.SKU]; exists {
			return nil, fmt.Errorf("%w: duplicate seed stock item %q", inventory.ErrInvalidArgument, item.SKU)
		}

		repository.stock[item.SKU] = item
	}

	return repository, nil
}

func (r *InventoryRepository) SetStock(
	ctx context.Context,
	sku string,
	availableQuantity int,
) (inventory.StockItem, error) {
	if err := ctx.Err(); err != nil {
		return inventory.StockItem{}, err
	}

	item, err := inventory.NewStockItem(sku, availableQuantity)
	if err != nil {
		return inventory.StockItem{}, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if existingItem, exists := r.stock[item.SKU]; exists {
		item.ReservedQuantity = existingItem.ReservedQuantity
	}

	r.stock[item.SKU] = item

	return cloneStockItem(item), nil
}

func (r *InventoryRepository) GetStock(ctx context.Context, sku string) (inventory.StockItem, error) {
	if err := ctx.Err(); err != nil {
		return inventory.StockItem{}, err
	}

	sku, err := normalizedIdentifier("stock item SKU", sku)
	if err != nil {
		return inventory.StockItem{}, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	item, exists := r.stock[sku]
	if !exists {
		return inventory.StockItem{}, fmt.Errorf("%w: sku %q", inventory.ErrStockItemNotFound, sku)
	}

	return cloneStockItem(item), nil
}

func (r *InventoryRepository) ListStock(ctx context.Context) ([]inventory.StockItem, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	items := make([]inventory.StockItem, 0, len(r.stock))
	for _, item := range r.stock {
		items = append(items, cloneStockItem(item))
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].SKU < items[j].SKU
	})

	return items, nil
}

func (r *InventoryRepository) Reserve(
	ctx context.Context,
	reservationID string,
	sku string,
	quantity int,
) (inventory.Reservation, error) {
	if err := ctx.Err(); err != nil {
		return inventory.Reservation{}, err
	}

	reservationID, err := normalizedIdentifier("reservation ID", reservationID)
	if err != nil {
		return inventory.Reservation{}, err
	}
	sku, err = normalizedIdentifier("stock item SKU", sku)
	if err != nil {
		return inventory.Reservation{}, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.reservations[reservationID]; exists {
		return inventory.Reservation{}, fmt.Errorf(
			"%w: reservation %q",
			inventory.ErrReservationAlreadyExists,
			reservationID,
		)
	}

	item, exists := r.stock[sku]
	if !exists {
		return inventory.Reservation{}, fmt.Errorf("%w: sku %q", inventory.ErrStockItemNotFound, sku)
	}

	reservation, err := r.service.Reserve(item, reservationID, quantity)
	if err != nil {
		return inventory.Reservation{}, err
	}

	r.reservations[reservation.ID] = reservation

	return cloneReservation(reservation), nil
}

func (r *InventoryRepository) Release(
	ctx context.Context,
	reservationID string,
) (inventory.Reservation, error) {
	if err := ctx.Err(); err != nil {
		return inventory.Reservation{}, err
	}

	reservationID, err := normalizedIdentifier("reservation ID", reservationID)
	if err != nil {
		return inventory.Reservation{}, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	reservation, exists := r.reservations[reservationID]
	if !exists {
		return inventory.Reservation{}, fmt.Errorf(
			"%w: reservation %q",
			inventory.ErrReservationNotFound,
			reservationID,
		)
	}

	item, exists := r.stock[reservation.SKU]
	if !exists {
		return inventory.Reservation{}, fmt.Errorf("%w: sku %q", inventory.ErrStockItemNotFound, reservation.SKU)
	}

	if err := r.service.Release(item, reservation); err != nil {
		return inventory.Reservation{}, err
	}

	return cloneReservation(reservation), nil
}

func (r *InventoryRepository) GetReservation(
	ctx context.Context,
	reservationID string,
) (inventory.Reservation, error) {
	if err := ctx.Err(); err != nil {
		return inventory.Reservation{}, err
	}

	reservationID, err := normalizedIdentifier("reservation ID", reservationID)
	if err != nil {
		return inventory.Reservation{}, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	reservation, exists := r.reservations[reservationID]
	if !exists {
		return inventory.Reservation{}, fmt.Errorf(
			"%w: reservation %q",
			inventory.ErrReservationNotFound,
			reservationID,
		)
	}

	return cloneReservation(reservation), nil
}

func (r *InventoryRepository) ListReservations(ctx context.Context) ([]inventory.Reservation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	reservations := make([]inventory.Reservation, 0, len(r.reservations))
	for _, reservation := range r.reservations {
		reservations = append(reservations, cloneReservation(reservation))
	}

	sort.Slice(reservations, func(i, j int) bool {
		return reservations[i].ID < reservations[j].ID
	})

	return reservations, nil
}

func normalizedIdentifier(name, value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("%w: %s is required", inventory.ErrInvalidArgument, name)
	}

	return value, nil
}

func cloneStockItem(item *inventory.StockItem) inventory.StockItem {
	return *item
}

func cloneReservation(reservation *inventory.Reservation) inventory.Reservation {
	clonedReservation := *reservation
	if reservation.ReleasedAt != nil {
		releasedAt := *reservation.ReleasedAt
		clonedReservation.ReleasedAt = &releasedAt
	}

	return clonedReservation
}
