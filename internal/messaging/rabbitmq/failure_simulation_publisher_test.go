package rabbitmq

import (
	"context"
	"errors"
	"testing"

	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/app"
)

func TestFailureSimulationPublisherReturnsPublishFailure(t *testing.T) {
	controller := app.NewFailureModeController()
	if err := controller.Set(app.FailureModeSettings{Mode: app.FailureModePublishFailure}); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	delegate := &reservationResultPublisherStub{}
	publisher := NewFailureSimulationPublisher(delegate, controller)

	err := publisher.PublishReservationResult(context.Background(), app.ReservationResult{})

	if !errors.Is(err, ErrSimulatedPublishFailure) {
		t.Fatalf("PublishReservationResult() error = %v, want %v", err, ErrSimulatedPublishFailure)
	}
	if delegate.published != 0 {
		t.Errorf("published = %d, want %d", delegate.published, 0)
	}
}

func TestFailureSimulationPublisherPublishesDuplicateResponse(t *testing.T) {
	controller := app.NewFailureModeController()
	if err := controller.Set(app.FailureModeSettings{Mode: app.FailureModeDuplicateResponse}); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	delegate := &reservationResultPublisherStub{}
	publisher := NewFailureSimulationPublisher(delegate, controller)

	if err := publisher.PublishReservationResult(context.Background(), app.ReservationResult{}); err != nil {
		t.Fatalf("PublishReservationResult() error = %v", err)
	}
	if delegate.published != 2 {
		t.Errorf("published = %d, want %d", delegate.published, 2)
	}
}

func TestFailureSimulationPublisherDoesNotBlockRetryPublish(t *testing.T) {
	controller := app.NewFailureModeController()
	if err := controller.Set(app.FailureModeSettings{Mode: app.FailureModePublishFailure}); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	delegate := &reservationResultPublisherStub{}
	publisher := NewFailureSimulationPublisher(delegate, controller)

	if err := publisher.PublishRetry(context.Background(), validDelivery(), ReservationRequestedRoutingKey, 1); err != nil {
		t.Fatalf("PublishRetry() error = %v", err)
	}
	if delegate.retried != 1 {
		t.Errorf("retried = %d, want %d", delegate.retried, 1)
	}
}
