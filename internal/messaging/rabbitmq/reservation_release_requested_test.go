package rabbitmq

import (
	"context"
	"errors"
	"testing"

	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/app"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestDecodeReservationReleaseRequested(t *testing.T) {
	request, err := decodeReservationReleaseRequested(validReleaseDelivery())
	if err != nil {
		t.Fatalf("decodeReservationReleaseRequested() error = %v", err)
	}

	if request.ReservationID != "res-10001" {
		t.Errorf("ReservationID = %q, want %q", request.ReservationID, "res-10001")
	}
	if request.Reason != "order_cancelled" {
		t.Errorf("Reason = %q, want %q", request.Reason, "order_cancelled")
	}
}

func TestConsumerHandleReleaseDeliveryAcknowledgesProcessedMessage(t *testing.T) {
	acknowledger := &recordingAcknowledger{}
	delivery := validReleaseDelivery()
	delivery.Acknowledger = acknowledger
	delivery.DeliveryTag = 1
	consumer := &Consumer{logger: discardLogger()}
	publisher := &reservationResultPublisherStub{}

	err := consumer.handleReleaseDelivery(context.Background(), delivery, reservationReleaseHandlerStub{}, publisher)

	if err != nil {
		t.Fatalf("handleReleaseDelivery() error = %v", err)
	}
	if acknowledger.acked != 1 {
		t.Errorf("acked = %d, want %d", acknowledger.acked, 1)
	}
	if publisher.releasePublished != 1 {
		t.Errorf("releasePublished = %d, want %d", publisher.releasePublished, 1)
	}
}

func TestDecodeReservationReleaseRequestedRejectsInvalidPayload(t *testing.T) {
	delivery := validReleaseDelivery()
	delivery.Body = []byte(`{"reason":"order_cancelled"}`)

	if _, err := decodeReservationReleaseRequested(delivery); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("decodeReservationReleaseRequested() error = %v, want ErrInvalidMessage", err)
	}
}

func validReleaseDelivery() amqp.Delivery {
	delivery := validDelivery()
	delivery.Headers["idempotency_key"] = "reservation:res-10001:release"
	delivery.Body = []byte(`{"reservation_id":"res-10001","reason":"order_cancelled"}`)

	return delivery
}

type reservationReleaseHandlerStub struct{}

func (reservationReleaseHandlerStub) HandleReservationReleaseRequested(
	context.Context,
	app.ReservationReleaseRequest,
) (app.ReservationReleaseResult, error) {
	return app.ReservationReleaseResult{Decision: app.ReservationReleaseDecisionReleased}, nil
}
