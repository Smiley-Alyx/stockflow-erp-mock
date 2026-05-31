package app

import (
	"context"
	"errors"
	"fmt"

	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/domain/inventory"
)

type ReservationReleaseRequest struct {
	Metadata      MessageMetadata
	ReservationID string
	Reason        string
}

type ReservationReleaseDecision string

const (
	ReservationReleaseDecisionReleased ReservationReleaseDecision = "released"
	ReservationReleaseDecisionFailed   ReservationReleaseDecision = "release_failed"
)

const (
	ReservationReleaseFailureReasonNotActive = "RESERVATION_NOT_ACTIVE"
	ReservationReleaseFailureReasonNotFound  = "RESERVATION_NOT_FOUND"
)

type ReservationReleaseResult struct {
	Decision       ReservationReleaseDecision
	FailureReason  string
	IdempotencyHit bool
	Request        ReservationReleaseRequest
	Reservation    inventory.Reservation
}

type ReservationReleaseRequestHandler interface {
	HandleReservationReleaseRequested(
		ctx context.Context,
		request ReservationReleaseRequest,
	) (ReservationReleaseResult, error)
}

type ReservationReleaseResultIdempotencyStore interface {
	Execute(
		ctx context.Context,
		key string,
		operation func() (ReservationReleaseResult, error),
	) (ReservationReleaseResult, bool, error)
}

type InventoryReservationReleaseHandler struct {
	repository       inventory.Repository
	idempotencyStore ReservationReleaseResultIdempotencyStore
}

func NewInventoryReservationReleaseHandler(
	repository inventory.Repository,
	idempotencyStore ReservationReleaseResultIdempotencyStore,
) *InventoryReservationReleaseHandler {
	return &InventoryReservationReleaseHandler{
		repository:       repository,
		idempotencyStore: idempotencyStore,
	}
}

func (h *InventoryReservationReleaseHandler) HandleReservationReleaseRequested(
	ctx context.Context,
	request ReservationReleaseRequest,
) (ReservationReleaseResult, error) {
	result, idempotencyHit, err := h.idempotencyStore.Execute(ctx, request.Metadata.IdempotencyKey, func() (ReservationReleaseResult, error) {
		return h.release(ctx, request)
	})
	if err != nil {
		return ReservationReleaseResult{}, err
	}
	if idempotencyHit && !sameReservationReleaseRequest(result.Request, request) {
		return ReservationReleaseResult{}, fmt.Errorf(
			"%w: idempotency key %q",
			ErrIdempotencyConflict,
			request.Metadata.IdempotencyKey,
		)
	}

	result.IdempotencyHit = idempotencyHit
	result.Request.Metadata = request.Metadata

	return result, nil
}

func (h *InventoryReservationReleaseHandler) release(
	ctx context.Context,
	request ReservationReleaseRequest,
) (ReservationReleaseResult, error) {
	reservation, err := h.repository.Release(ctx, request.ReservationID)
	if err != nil {
		switch {
		case errors.Is(err, inventory.ErrReservationNotFound):
			return ReservationReleaseResult{
				Decision:      ReservationReleaseDecisionFailed,
				FailureReason: ReservationReleaseFailureReasonNotFound,
				Request:       request,
			}, nil
		case errors.Is(err, inventory.ErrReservationNotActive):
			return ReservationReleaseResult{
				Decision:      ReservationReleaseDecisionFailed,
				FailureReason: ReservationReleaseFailureReasonNotActive,
				Request:       request,
			}, nil
		default:
			return ReservationReleaseResult{}, err
		}
	}

	return ReservationReleaseResult{
		Decision:    ReservationReleaseDecisionReleased,
		Request:     request,
		Reservation: reservation,
	}, nil
}

func sameReservationReleaseRequest(left, right ReservationReleaseRequest) bool {
	return left.ReservationID == right.ReservationID &&
		left.Reason == right.Reason
}
