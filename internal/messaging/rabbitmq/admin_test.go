package rabbitmq

import (
	"context"
	"errors"
	"testing"

	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/app"
)

func TestDeadLetterConfig(t *testing.T) {
	tests := []struct {
		name           string
		queue          app.DeadLetterQueue
		wantName       string
		wantRoutingKey string
	}{
		{
			name:           "reservation requests",
			queue:          app.DeadLetterQueueReservationRequests,
			wantName:       ReservationRequestedDeadLetterQueueName,
			wantRoutingKey: ReservationRequestedRoutingKey,
		},
		{
			name:           "reservation release requests",
			queue:          app.DeadLetterQueueReservationReleaseRequests,
			wantName:       ReservationReleaseRequestedDeadLetterQueueName,
			wantRoutingKey: ReservationReleaseRequestedRoutingKey,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := deadLetterConfig(tt.queue)
			if err != nil {
				t.Fatalf("deadLetterConfig() error = %v", err)
			}
			if config.name != tt.wantName {
				t.Errorf("name = %q, want %q", config.name, tt.wantName)
			}
			if config.routingKey != tt.wantRoutingKey {
				t.Errorf("routing key = %q, want %q", config.routingKey, tt.wantRoutingKey)
			}
		})
	}
}

func TestAdminRequeueDeadLettersRejectsInvalidLimit(t *testing.T) {
	admin := &Admin{}

	_, err := admin.RequeueDeadLetters(context.Background(), app.DeadLetterQueueReservationRequests, maxRequeueLimit+1)
	if !errors.Is(err, app.ErrInvalidRequeueLimit) {
		t.Fatalf("RequeueDeadLetters() error = %v, want %v", err, app.ErrInvalidRequeueLimit)
	}
}

func TestDeadLetterConfigRejectsUnknownQueue(t *testing.T) {
	_, err := deadLetterConfig(app.DeadLetterQueue("unknown"))
	if !errors.Is(err, app.ErrInvalidDeadLetterQueue) {
		t.Fatalf("deadLetterConfig() error = %v, want %v", err, app.ErrInvalidDeadLetterQueue)
	}
}
