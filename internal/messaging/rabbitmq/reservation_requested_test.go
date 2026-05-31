package rabbitmq

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/app"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestDecodeReservationRequested(t *testing.T) {
	request, err := decodeReservationRequested(validDelivery())
	if err != nil {
		t.Fatalf("decodeReservationRequested() error = %v", err)
	}

	if request.Metadata.MessageID != "58d867f6-69e0-4f2f-b1ee-d587aaa48b6e" {
		t.Errorf("MessageID = %q", request.Metadata.MessageID)
	}
	if request.ReservationID != "res-10001" {
		t.Errorf("ReservationID = %q, want %q", request.ReservationID, "res-10001")
	}
	if request.Quantity != 2 {
		t.Errorf("Quantity = %d, want %d", request.Quantity, 2)
	}
}

func TestDecodeReservationRequestedRejectsInvalidMessages(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*amqp.Delivery)
	}{
		{
			name: "missing header",
			mutate: func(delivery *amqp.Delivery) {
				delete(delivery.Headers, "correlation_id")
			},
		},
		{
			name: "unsupported schema version",
			mutate: func(delivery *amqp.Delivery) {
				delivery.Headers["schema_version"] = int32(2)
			},
		},
		{
			name: "invalid message ID",
			mutate: func(delivery *amqp.Delivery) {
				delivery.Headers["message_id"] = "not-a-uuid"
			},
		},
		{
			name: "unknown payload field",
			mutate: func(delivery *amqp.Delivery) {
				delivery.Body = []byte(`{"reservation_id":"res-10001","order_id":"ord-10001","sku":"sku-red-mug","quantity":2,"unknown":true}`)
			},
		},
		{
			name: "invalid quantity",
			mutate: func(delivery *amqp.Delivery) {
				delivery.Body = []byte(`{"reservation_id":"res-10001","order_id":"ord-10001","sku":"sku-red-mug","quantity":0}`)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delivery := validDelivery()
			tt.mutate(&delivery)

			if _, err := decodeReservationRequested(delivery); !errors.Is(err, ErrInvalidMessage) {
				t.Fatalf("decodeReservationRequested() error = %v, want ErrInvalidMessage", err)
			}
		})
	}
}

func TestConsumerHandleDeliveryAcknowledgesProcessedMessage(t *testing.T) {
	acknowledger := &recordingAcknowledger{}
	delivery := validDelivery()
	delivery.Acknowledger = acknowledger
	delivery.DeliveryTag = 1
	consumer := &Consumer{logger: discardLogger()}

	err := consumer.handleDelivery(context.Background(), delivery, reservationHandlerStub{})

	if err != nil {
		t.Fatalf("handleDelivery() error = %v", err)
	}
	if acknowledger.acked != 1 {
		t.Errorf("acked = %d, want %d", acknowledger.acked, 1)
	}
}

func TestConsumerHandleDeliveryRejectsInvalidMessageWithoutRequeue(t *testing.T) {
	acknowledger := &recordingAcknowledger{}
	delivery := validDelivery()
	delete(delivery.Headers, "message_id")
	delivery.Acknowledger = acknowledger
	delivery.DeliveryTag = 1
	consumer := &Consumer{logger: discardLogger()}

	err := consumer.handleDelivery(context.Background(), delivery, reservationHandlerStub{})

	if err != nil {
		t.Fatalf("handleDelivery() error = %v", err)
	}
	if acknowledger.nacked != 1 {
		t.Errorf("nacked = %d, want %d", acknowledger.nacked, 1)
	}
	if acknowledger.requeue {
		t.Error("requeue = true, want false")
	}
}

func validDelivery() amqp.Delivery {
	return amqp.Delivery{
		Headers: amqp.Table{
			"message_id":      "58d867f6-69e0-4f2f-b1ee-d587aaa48b6e",
			"correlation_id":  "bb8d8f75-5210-4038-98cc-f2237d192ff8",
			"causation_id":    "fa6c60fa-6f72-4d96-9e40-8fc997d72f1e",
			"idempotency_key": "reservation:res-10001:create",
			"occurred_at":     "2026-05-31T09:00:00Z",
			"schema_version":  int32(1),
			"retry_count":     int32(0),
		},
		Body: []byte(`{"reservation_id":"res-10001","order_id":"ord-10001","sku":"sku-red-mug","quantity":2}`),
	}
}

type reservationHandlerStub struct{}

func (reservationHandlerStub) HandleReservationRequested(
	context.Context,
	app.ReservationRequest,
) (app.ReservationResult, error) {
	return app.ReservationResult{Decision: app.ReservationDecisionConfirmed}, nil
}

type recordingAcknowledger struct {
	acked   int
	nacked  int
	requeue bool
}

func (a *recordingAcknowledger) Ack(_ uint64, _ bool) error {
	a.acked++
	return nil
}

func (a *recordingAcknowledger) Nack(_ uint64, _ bool, requeue bool) error {
	a.nacked++
	a.requeue = requeue
	return nil
}

func (a *recordingAcknowledger) Reject(_ uint64, _ bool) error {
	return nil
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
