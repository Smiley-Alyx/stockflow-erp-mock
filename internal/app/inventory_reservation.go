package app

import (
	"context"
	"errors"
	"time"

	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/domain/inventory"
)

type MessageMetadata struct {
	MessageID      string
	CorrelationID  string
	CausationID    string
	IdempotencyKey string
	OccurredAt     time.Time
	SchemaVersion  int
	RetryCount     int
}

type ReservationRequest struct {
	Metadata      MessageMetadata
	ReservationID string
	OrderID       string
	SKU           string
	Quantity      int
}

type ReservationDecision string

const (
	ReservationDecisionConfirmed ReservationDecision = "confirmed"
	ReservationDecisionRejected  ReservationDecision = "rejected"
)

const ReservationRejectionReasonInsufficientStock = "INSUFFICIENT_STOCK"

type ReservationResult struct {
	Decision        ReservationDecision
	Request         ReservationRequest
	Reservation     inventory.Reservation
	RejectionReason string
}

type ReservationRequestHandler interface {
	HandleReservationRequested(ctx context.Context, request ReservationRequest) (ReservationResult, error)
}

type InventoryReservationHandler struct {
	repository inventory.Repository
}

func NewInventoryReservationHandler(repository inventory.Repository) *InventoryReservationHandler {
	return &InventoryReservationHandler{repository: repository}
}

func (h *InventoryReservationHandler) HandleReservationRequested(
	ctx context.Context,
	request ReservationRequest,
) (ReservationResult, error) {
	reservation, err := h.repository.Reserve(ctx, request.ReservationID, request.SKU, request.Quantity)
	if err != nil {
		if errors.Is(err, inventory.ErrInsufficientStock) || errors.Is(err, inventory.ErrStockItemNotFound) {
			return ReservationResult{
				Decision:        ReservationDecisionRejected,
				Request:         request,
				RejectionReason: ReservationRejectionReasonInsufficientStock,
			}, nil
		}

		return ReservationResult{}, err
	}

	return ReservationResult{
		Decision:    ReservationDecisionConfirmed,
		Request:     request,
		Reservation: reservation,
	}, nil
}
