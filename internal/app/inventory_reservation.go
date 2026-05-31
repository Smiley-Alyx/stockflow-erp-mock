package app

import (
	"context"
	"errors"
	"fmt"
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

var ErrIdempotencyConflict = errors.New("idempotency key conflicts with a different request")

type ReservationResult struct {
	Decision        ReservationDecision
	IdempotencyHit  bool
	Request         ReservationRequest
	Reservation     inventory.Reservation
	RejectionReason string
}

type ReservationRequestHandler interface {
	HandleReservationRequested(ctx context.Context, request ReservationRequest) (ReservationResult, error)
}

type ReservationResultIdempotencyStore interface {
	Execute(
		ctx context.Context,
		key string,
		operation func() (ReservationResult, error),
	) (ReservationResult, bool, error)
}

type InventoryReservationHandler struct {
	repository       inventory.Repository
	idempotencyStore ReservationResultIdempotencyStore
}

func NewInventoryReservationHandler(
	repository inventory.Repository,
	idempotencyStore ReservationResultIdempotencyStore,
) *InventoryReservationHandler {
	return &InventoryReservationHandler{
		repository:       repository,
		idempotencyStore: idempotencyStore,
	}
}

func (h *InventoryReservationHandler) HandleReservationRequested(
	ctx context.Context,
	request ReservationRequest,
) (ReservationResult, error) {
	result, idempotencyHit, err := h.idempotencyStore.Execute(ctx, request.Metadata.IdempotencyKey, func() (ReservationResult, error) {
		return h.reserve(ctx, request)
	})
	if err != nil {
		return ReservationResult{}, err
	}
	if idempotencyHit && !sameReservationRequest(result.Request, request) {
		return ReservationResult{}, fmt.Errorf(
			"%w: idempotency key %q",
			ErrIdempotencyConflict,
			request.Metadata.IdempotencyKey,
		)
	}

	result.IdempotencyHit = idempotencyHit
	result.Request.Metadata = request.Metadata

	return result, nil
}

func (h *InventoryReservationHandler) reserve(ctx context.Context, request ReservationRequest) (ReservationResult, error) {
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

func sameReservationRequest(left, right ReservationRequest) bool {
	return left.ReservationID == right.ReservationID &&
		left.OrderID == right.OrderID &&
		left.SKU == right.SKU &&
		left.Quantity == right.Quantity
}
