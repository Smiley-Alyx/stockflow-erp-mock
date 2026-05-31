package app

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"
)

type FailureSimulationReservationHandler struct {
	handler          ReservationRequestHandler
	idempotencyStore ReservationResultIdempotencyStore
	failureModes     *FailureModeController
	randomFloat      func() float64
}

type FailureSimulationReservationReleaseHandler struct {
	handler      ReservationReleaseRequestHandler
	failureModes *FailureModeController
}

func NewFailureSimulationReservationHandler(
	handler ReservationRequestHandler,
	idempotencyStore ReservationResultIdempotencyStore,
	failureModes *FailureModeController,
) *FailureSimulationReservationHandler {
	return &FailureSimulationReservationHandler{
		handler:          handler,
		idempotencyStore: idempotencyStore,
		failureModes:     failureModes,
		randomFloat:      rand.Float64,
	}
}

func NewFailureSimulationReservationReleaseHandler(
	handler ReservationReleaseRequestHandler,
	failureModes *FailureModeController,
) *FailureSimulationReservationReleaseHandler {
	return &FailureSimulationReservationReleaseHandler{
		handler:      handler,
		failureModes: failureModes,
	}
}

func (h *FailureSimulationReservationHandler) HandleReservationRequested(
	ctx context.Context,
	request ReservationRequest,
) (ReservationResult, error) {
	settings := h.failureModes.Get()
	if err := waitForProcessingDelay(ctx, settings); err != nil {
		return ReservationResult{}, err
	}

	result, idempotencyHit, err := h.idempotencyStore.Execute(
		ctx,
		request.Metadata.IdempotencyKey,
		func() (ReservationResult, error) {
			if h.shouldReject(settings) {
				return ReservationResult{
					Decision:        ReservationDecisionRejected,
					Request:         request,
					RejectionReason: ReservationRejectionReasonFailureMode,
				}, nil
			}

			return h.handler.HandleReservationRequested(ctx, request)
		},
	)
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

	result.IdempotencyHit = result.IdempotencyHit || idempotencyHit
	result.Request.Metadata = request.Metadata

	return result, nil
}

func (h *FailureSimulationReservationReleaseHandler) HandleReservationReleaseRequested(
	ctx context.Context,
	request ReservationReleaseRequest,
) (ReservationReleaseResult, error) {
	if err := waitForProcessingDelay(ctx, h.failureModes.Get()); err != nil {
		return ReservationReleaseResult{}, err
	}

	return h.handler.HandleReservationReleaseRequested(ctx, request)
}

func (h *FailureSimulationReservationHandler) shouldReject(settings FailureModeSettings) bool {
	switch settings.Mode {
	case FailureModeAlwaysReject:
		return true
	case FailureModeRandomReject:
		return h.randomFloat() < settings.RandomRejectProbability
	default:
		return false
	}
}

func waitForProcessingDelay(ctx context.Context, settings FailureModeSettings) error {
	if settings.Mode != FailureModeProcessingDelay {
		return nil
	}

	timer := time.NewTimer(settings.ProcessingDelay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
