package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/storage/memory"
)

func TestFailureSimulationReservationHandlerAlwaysRejectsWithoutCallingHandler(t *testing.T) {
	controller := NewFailureModeController()
	if err := controller.Set(FailureModeSettings{Mode: FailureModeAlwaysReject}); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	called := false
	handler := NewFailureSimulationReservationHandler(
		reservationRequestHandlerFunc(func(context.Context, ReservationRequest) (ReservationResult, error) {
			called = true
			return ReservationResult{Decision: ReservationDecisionConfirmed}, nil
		}),
		memory.NewIdempotencyStore[ReservationResult](),
		controller,
	)

	result, err := handler.HandleReservationRequested(context.Background(), ReservationRequest{
		Metadata:      MessageMetadata{IdempotencyKey: "reservation:reservation-1:create"},
		ReservationID: "reservation-1",
	})
	if err != nil {
		t.Fatalf("HandleReservationRequested() error = %v", err)
	}
	if called {
		t.Error("wrapped handler was called, want no call")
	}
	if result.Decision != ReservationDecisionRejected {
		t.Errorf("Decision = %q, want %q", result.Decision, ReservationDecisionRejected)
	}
	if result.RejectionReason != ReservationRejectionReasonFailureMode {
		t.Errorf("RejectionReason = %q, want %q", result.RejectionReason, ReservationRejectionReasonFailureMode)
	}
}

func TestFailureSimulationReservationHandlerRandomRejectsByProbability(t *testing.T) {
	controller := NewFailureModeController()
	if err := controller.Set(FailureModeSettings{
		Mode:                    FailureModeRandomReject,
		RandomRejectProbability: 0.5,
	}); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	called := false
	handler := NewFailureSimulationReservationHandler(
		reservationRequestHandlerFunc(func(context.Context, ReservationRequest) (ReservationResult, error) {
			called = true
			return ReservationResult{Decision: ReservationDecisionConfirmed}, nil
		}),
		memory.NewIdempotencyStore[ReservationResult](),
		controller,
	)
	handler.randomFloat = func() float64 { return 0.25 }

	result, err := handler.HandleReservationRequested(context.Background(), ReservationRequest{
		Metadata: MessageMetadata{IdempotencyKey: "reservation:reservation-1:create"},
	})
	if err != nil {
		t.Fatalf("HandleReservationRequested() error = %v", err)
	}
	if called {
		t.Error("wrapped handler was called, want no call")
	}
	if result.Decision != ReservationDecisionRejected {
		t.Errorf("Decision = %q, want %q", result.Decision, ReservationDecisionRejected)
	}
}

func TestFailureSimulationReservationHandlerStopsDelayWhenContextIsCancelled(t *testing.T) {
	controller := NewFailureModeController()
	if err := controller.Set(FailureModeSettings{
		Mode:            FailureModeProcessingDelay,
		ProcessingDelay: time.Second,
	}); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	handler := NewFailureSimulationReservationHandler(
		reservationRequestHandlerFunc(func(context.Context, ReservationRequest) (ReservationResult, error) {
			return ReservationResult{Decision: ReservationDecisionConfirmed}, nil
		}),
		memory.NewIdempotencyStore[ReservationResult](),
		controller,
	)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := handler.HandleReservationRequested(ctx, ReservationRequest{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("HandleReservationRequested() error = %v, want %v", err, context.Canceled)
	}
}

func TestFailureSimulationReservationHandlerReturnsCachedRejection(t *testing.T) {
	controller := NewFailureModeController()
	if err := controller.Set(FailureModeSettings{Mode: FailureModeAlwaysReject}); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	handler := NewFailureSimulationReservationHandler(
		reservationRequestHandlerFunc(func(context.Context, ReservationRequest) (ReservationResult, error) {
			return ReservationResult{Decision: ReservationDecisionConfirmed}, nil
		}),
		memory.NewIdempotencyStore[ReservationResult](),
		controller,
	)
	request := ReservationRequest{
		Metadata:      MessageMetadata{IdempotencyKey: "reservation:reservation-1:create"},
		ReservationID: "reservation-1",
	}
	if _, err := handler.HandleReservationRequested(context.Background(), request); err != nil {
		t.Fatalf("first HandleReservationRequested() error = %v", err)
	}
	if err := controller.Set(FailureModeSettings{Mode: FailureModeNormal}); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	result, err := handler.HandleReservationRequested(context.Background(), request)
	if err != nil {
		t.Fatalf("second HandleReservationRequested() error = %v", err)
	}
	if result.Decision != ReservationDecisionRejected {
		t.Errorf("Decision = %q, want %q", result.Decision, ReservationDecisionRejected)
	}
	if !result.IdempotencyHit {
		t.Error("IdempotencyHit = false, want true")
	}
}

type reservationRequestHandlerFunc func(context.Context, ReservationRequest) (ReservationResult, error)

func (f reservationRequestHandlerFunc) HandleReservationRequested(
	ctx context.Context,
	request ReservationRequest,
) (ReservationResult, error) {
	return f(ctx, request)
}
