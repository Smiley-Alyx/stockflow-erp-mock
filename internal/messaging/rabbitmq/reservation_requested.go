package rabbitmq

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/app"
	amqp "github.com/rabbitmq/amqp091-go"
)

var ErrInvalidMessage = errors.New("invalid message")

var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

type reservationRequestedPayload struct {
	ReservationID string `json:"reservation_id"`
	OrderID       string `json:"order_id"`
	SKU           string `json:"sku"`
	Quantity      int    `json:"quantity"`
}

func decodeReservationRequested(delivery amqp.Delivery) (app.ReservationRequest, error) {
	metadata, err := decodeMessageMetadata(delivery.Headers)
	if err != nil {
		return app.ReservationRequest{}, err
	}

	var payload reservationRequestedPayload
	decoder := json.NewDecoder(bytes.NewReader(delivery.Body))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&payload); err != nil {
		return app.ReservationRequest{}, fmt.Errorf("%w: decode payload: %v", ErrInvalidMessage, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return app.ReservationRequest{}, fmt.Errorf("%w: payload must contain a single JSON object", ErrInvalidMessage)
	}
	if strings.TrimSpace(payload.ReservationID) == "" {
		return app.ReservationRequest{}, fmt.Errorf("%w: reservation_id is required", ErrInvalidMessage)
	}
	if strings.TrimSpace(payload.OrderID) == "" {
		return app.ReservationRequest{}, fmt.Errorf("%w: order_id is required", ErrInvalidMessage)
	}
	if strings.TrimSpace(payload.SKU) == "" {
		return app.ReservationRequest{}, fmt.Errorf("%w: sku is required", ErrInvalidMessage)
	}
	if payload.Quantity <= 0 {
		return app.ReservationRequest{}, fmt.Errorf("%w: quantity must be positive", ErrInvalidMessage)
	}

	return app.ReservationRequest{
		Metadata:      metadata,
		ReservationID: payload.ReservationID,
		OrderID:       payload.OrderID,
		SKU:           payload.SKU,
		Quantity:      payload.Quantity,
	}, nil
}

func decodeMessageMetadata(headers amqp.Table) (app.MessageMetadata, error) {
	messageID, err := requiredUUIDHeader(headers, "message_id")
	if err != nil {
		return app.MessageMetadata{}, err
	}
	correlationID, err := requiredUUIDHeader(headers, "correlation_id")
	if err != nil {
		return app.MessageMetadata{}, err
	}
	causationID, err := requiredUUIDHeader(headers, "causation_id")
	if err != nil {
		return app.MessageMetadata{}, err
	}
	idempotencyKey, err := requiredStringHeader(headers, "idempotency_key")
	if err != nil {
		return app.MessageMetadata{}, err
	}
	occurredAtValue, err := requiredStringHeader(headers, "occurred_at")
	if err != nil {
		return app.MessageMetadata{}, err
	}
	occurredAt, err := time.Parse(time.RFC3339, occurredAtValue)
	if err != nil {
		return app.MessageMetadata{}, fmt.Errorf("%w: header %q must be an RFC3339 timestamp", ErrInvalidMessage, "occurred_at")
	}
	schemaVersion, err := requiredIntegerHeader(headers, "schema_version")
	if err != nil {
		return app.MessageMetadata{}, err
	}
	if schemaVersion != 1 {
		return app.MessageMetadata{}, fmt.Errorf("%w: unsupported schema_version %d", ErrInvalidMessage, schemaVersion)
	}
	retryCount, err := requiredIntegerHeader(headers, "retry_count")
	if err != nil {
		return app.MessageMetadata{}, err
	}
	if retryCount < 0 {
		return app.MessageMetadata{}, fmt.Errorf("%w: header %q must not be negative", ErrInvalidMessage, "retry_count")
	}

	return app.MessageMetadata{
		MessageID:      messageID,
		CorrelationID:  correlationID,
		CausationID:    causationID,
		IdempotencyKey: idempotencyKey,
		OccurredAt:     occurredAt,
		SchemaVersion:  schemaVersion,
		RetryCount:     retryCount,
	}, nil
}

func requiredStringHeader(headers amqp.Table, name string) (string, error) {
	value, exists := headers[name]
	if !exists {
		return "", fmt.Errorf("%w: header %q is required", ErrInvalidMessage, name)
	}

	stringValue, ok := value.(string)
	if !ok || strings.TrimSpace(stringValue) == "" {
		return "", fmt.Errorf("%w: header %q must be a non-empty string", ErrInvalidMessage, name)
	}

	return stringValue, nil
}

func requiredUUIDHeader(headers amqp.Table, name string) (string, error) {
	value, err := requiredStringHeader(headers, name)
	if err != nil {
		return "", err
	}
	if !uuidPattern.MatchString(value) {
		return "", fmt.Errorf("%w: header %q must be a UUID", ErrInvalidMessage, name)
	}

	return value, nil
}

func requiredIntegerHeader(headers amqp.Table, name string) (int, error) {
	value, exists := headers[name]
	if !exists {
		return 0, fmt.Errorf("%w: header %q is required", ErrInvalidMessage, name)
	}

	switch integerValue := value.(type) {
	case int:
		return integerValue, nil
	case int32:
		return int(integerValue), nil
	case int64:
		return int(integerValue), nil
	default:
		return 0, fmt.Errorf("%w: header %q must be an integer", ErrInvalidMessage, name)
	}
}
