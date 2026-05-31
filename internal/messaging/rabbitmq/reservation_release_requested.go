package rabbitmq

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/app"
	amqp "github.com/rabbitmq/amqp091-go"
)

type reservationReleaseRequestedPayload struct {
	ReservationID string `json:"reservation_id"`
	Reason        string `json:"reason,omitempty"`
}

func decodeReservationReleaseRequested(delivery amqp.Delivery) (app.ReservationReleaseRequest, error) {
	metadata, err := decodeMessageMetadata(delivery.Headers)
	if err != nil {
		return app.ReservationReleaseRequest{}, err
	}

	var payload reservationReleaseRequestedPayload
	decoder := json.NewDecoder(bytes.NewReader(delivery.Body))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&payload); err != nil {
		return app.ReservationReleaseRequest{}, fmt.Errorf("%w: decode payload: %v", ErrInvalidMessage, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return app.ReservationReleaseRequest{}, fmt.Errorf("%w: payload must contain a single JSON object", ErrInvalidMessage)
	}
	if strings.TrimSpace(payload.ReservationID) == "" {
		return app.ReservationReleaseRequest{}, fmt.Errorf("%w: reservation_id is required", ErrInvalidMessage)
	}

	return app.ReservationReleaseRequest{
		Metadata:      metadata,
		ReservationID: payload.ReservationID,
		Reason:        payload.Reason,
	}, nil
}
