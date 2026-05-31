package rabbitmq

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/app"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	ReservationConfirmedRoutingKey     = "inventory.reservation.confirmed.v1"
	ReservationRejectedRoutingKey      = "inventory.reservation.rejected.v1"
	ReservationReleasedRoutingKey      = "inventory.reservation.released.v1"
	ReservationReleaseFailedRoutingKey = "inventory.reservation.release_failed.v1"
)

type PublisherConfig struct {
	URL            string
	PublishTimeout time.Duration
}

type Publisher struct {
	connection     *amqp.Connection
	channel        *amqp.Channel
	confirmations  <-chan amqp.Confirmation
	publishTimeout time.Duration
	closeOnce      sync.Once
}

func NewPublisher(config PublisherConfig) (*Publisher, error) {
	connection, err := amqp.Dial(config.URL)
	if err != nil {
		return nil, fmt.Errorf("dial RabbitMQ publisher: %w", err)
	}

	channel, err := connection.Channel()
	if err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("open RabbitMQ publisher channel: %w", err)
	}

	publisher := &Publisher{
		connection:     connection,
		channel:        channel,
		publishTimeout: config.PublishTimeout,
	}
	if err := publisher.declareTopology(); err != nil {
		_ = publisher.Close()
		return nil, err
	}
	if err := channel.Confirm(false); err != nil {
		_ = publisher.Close()
		return nil, fmt.Errorf("enable RabbitMQ publisher confirms: %w", err)
	}

	publisher.confirmations = channel.NotifyPublish(make(chan amqp.Confirmation, 1))

	return publisher, nil
}

func (p *Publisher) PublishReservationResult(ctx context.Context, result app.ReservationResult) error {
	messageID, err := newUUID()
	if err != nil {
		return fmt.Errorf("generate message ID: %w", err)
	}

	occurredAt := time.Now().UTC()
	routingKey, publishing, err := newReservationResultMessage(result, messageID, occurredAt)
	if err != nil {
		return err
	}

	return p.publishWithConfirmation(ctx, InventoryExchangeName, routingKey, publishing)
}

func (p *Publisher) PublishReservationReleaseResult(ctx context.Context, result app.ReservationReleaseResult) error {
	messageID, err := newUUID()
	if err != nil {
		return fmt.Errorf("generate message ID: %w", err)
	}

	occurredAt := time.Now().UTC()
	routingKey, publishing, err := newReservationReleaseResultMessage(result, messageID, occurredAt)
	if err != nil {
		return err
	}

	return p.publishWithConfirmation(ctx, InventoryExchangeName, routingKey, publishing)
}

func (p *Publisher) PublishRetry(
	ctx context.Context,
	delivery amqp.Delivery,
	routingKey string,
	retryCount int,
) error {
	return p.publishWithConfirmation(
		ctx,
		InventoryRetryExchangeName,
		routingKey,
		newForwardedDelivery(delivery, retryCount),
	)
}

func (p *Publisher) PublishDeadLetter(
	ctx context.Context,
	delivery amqp.Delivery,
	routingKey string,
	retryCount int,
) error {
	return p.publishWithConfirmation(
		ctx,
		InventoryDeadLetterExchangeName,
		routingKey,
		newForwardedDelivery(delivery, retryCount),
	)
}

func newReservationReleaseResultMessage(
	result app.ReservationReleaseResult,
	messageID string,
	occurredAt time.Time,
) (string, amqp.Publishing, error) {
	routingKey, payload, err := reservationReleaseResultPayload(result)
	if err != nil {
		return "", amqp.Publishing{}, err
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return "", amqp.Publishing{}, fmt.Errorf("marshal reservation release result payload: %w", err)
	}

	return routingKey, amqp.Publishing{
		Headers: amqp.Table{
			"message_id":      messageID,
			"correlation_id":  result.Request.Metadata.CorrelationID,
			"causation_id":    result.Request.Metadata.MessageID,
			"idempotency_key": result.Request.Metadata.IdempotencyKey + ":" + string(result.Decision),
			"occurred_at":     occurredAt.Format(time.RFC3339Nano),
			"schema_version":  int32(1),
			"retry_count":     int32(0),
		},
		ContentType:   "application/json",
		DeliveryMode:  amqp.Persistent,
		MessageId:     messageID,
		CorrelationId: result.Request.Metadata.CorrelationID,
		Timestamp:     occurredAt,
		Body:          body,
	}, nil
}

func reservationReleaseResultPayload(result app.ReservationReleaseResult) (string, any, error) {
	switch result.Decision {
	case app.ReservationReleaseDecisionReleased:
		if result.Reservation.ReleasedAt == nil {
			return "", nil, fmt.Errorf("released reservation timestamp is required")
		}

		return ReservationReleasedRoutingKey, reservationReleasedPayload{
			ReservationID: result.Request.ReservationID,
			ReleasedAt:    *result.Reservation.ReleasedAt,
		}, nil
	case app.ReservationReleaseDecisionFailed:
		return ReservationReleaseFailedRoutingKey, reservationReleaseFailedPayload{
			ReservationID: result.Request.ReservationID,
			Reason:        result.FailureReason,
			Details:       "Reservation release could not be completed.",
		}, nil
	default:
		return "", nil, fmt.Errorf("unsupported reservation release decision %q", result.Decision)
	}
}

func (p *Publisher) publishWithConfirmation(
	ctx context.Context,
	exchange string,
	routingKey string,
	publishing amqp.Publishing,
) error {
	publishContext, cancel := context.WithTimeout(ctx, p.publishTimeout)
	defer cancel()

	if err := p.channel.PublishWithContext(
		publishContext,
		exchange,
		routingKey,
		false,
		false,
		publishing,
	); err != nil {
		return fmt.Errorf("publish RabbitMQ message: %w", err)
	}

	select {
	case confirmation, open := <-p.confirmations:
		if !open {
			return fmt.Errorf("wait for reservation result confirmation: RabbitMQ channel closed")
		}
		if !confirmation.Ack {
			return fmt.Errorf("wait for reservation result confirmation: message was not acknowledged")
		}

		return nil
	case <-publishContext.Done():
		return fmt.Errorf("wait for reservation result confirmation: %w", publishContext.Err())
	}
}

func (p *Publisher) Close() error {
	var closeError error

	p.closeOnce.Do(func() {
		if err := p.channel.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			closeError = fmt.Errorf("close RabbitMQ publisher channel: %w", err)
		}
		if err := p.connection.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) && closeError == nil {
			closeError = fmt.Errorf("close RabbitMQ publisher connection: %w", err)
		}
	})

	return closeError
}

func (p *Publisher) declareTopology() error {
	for _, exchangeName := range []string{
		InventoryExchangeName,
		InventoryRetryExchangeName,
		InventoryDeadLetterExchangeName,
	} {
		if err := p.channel.ExchangeDeclare(
			exchangeName,
			"topic",
			true,
			false,
			false,
			false,
			nil,
		); err != nil {
			return fmt.Errorf("declare exchange %q for publisher: %w", exchangeName, err)
		}
	}

	return nil
}

func newForwardedDelivery(delivery amqp.Delivery, retryCount int) amqp.Publishing {
	headers := make(amqp.Table, len(delivery.Headers)+1)
	for name, value := range delivery.Headers {
		headers[name] = value
	}
	headers["retry_count"] = int32(retryCount)

	return amqp.Publishing{
		Headers:       headers,
		ContentType:   delivery.ContentType,
		DeliveryMode:  amqp.Persistent,
		CorrelationId: delivery.CorrelationId,
		MessageId:     delivery.MessageId,
		Timestamp:     delivery.Timestamp,
		Body:          delivery.Body,
	}
}

func newReservationResultMessage(
	result app.ReservationResult,
	messageID string,
	occurredAt time.Time,
) (string, amqp.Publishing, error) {
	routingKey, payload, err := reservationResultPayload(result)
	if err != nil {
		return "", amqp.Publishing{}, err
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return "", amqp.Publishing{}, fmt.Errorf("marshal reservation result payload: %w", err)
	}

	return routingKey, amqp.Publishing{
		Headers: amqp.Table{
			"message_id":      messageID,
			"correlation_id":  result.Request.Metadata.CorrelationID,
			"causation_id":    result.Request.Metadata.MessageID,
			"idempotency_key": result.Request.Metadata.IdempotencyKey + ":" + string(result.Decision),
			"occurred_at":     occurredAt.Format(time.RFC3339Nano),
			"schema_version":  int32(1),
			"retry_count":     int32(0),
		},
		ContentType:   "application/json",
		DeliveryMode:  amqp.Persistent,
		MessageId:     messageID,
		CorrelationId: result.Request.Metadata.CorrelationID,
		Timestamp:     occurredAt,
		Body:          body,
	}, nil
}

func reservationResultPayload(result app.ReservationResult) (string, any, error) {
	switch result.Decision {
	case app.ReservationDecisionConfirmed:
		return ReservationConfirmedRoutingKey, reservationConfirmedPayload{
			ReservationID: result.Request.ReservationID,
			OrderID:       result.Request.OrderID,
			SKU:           result.Request.SKU,
			Quantity:      result.Request.Quantity,
			ReservedAt:    result.Reservation.CreatedAt,
		}, nil
	case app.ReservationDecisionRejected:
		return ReservationRejectedRoutingKey, reservationRejectedPayload{
			ReservationID: result.Request.ReservationID,
			OrderID:       result.Request.OrderID,
			SKU:           result.Request.SKU,
			Quantity:      result.Request.Quantity,
			Reason:        result.RejectionReason,
			Details:       "Requested quantity exceeds available stock.",
		}, nil
	default:
		return "", nil, fmt.Errorf("unsupported reservation decision %q", result.Decision)
	}
}

func newUUID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}

	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80

	return fmt.Sprintf(
		"%08x-%04x-%04x-%04x-%012x",
		value[0:4],
		value[4:6],
		value[6:8],
		value[8:10],
		value[10:16],
	), nil
}

type reservationConfirmedPayload struct {
	ReservationID string    `json:"reservation_id"`
	OrderID       string    `json:"order_id"`
	SKU           string    `json:"sku"`
	Quantity      int       `json:"quantity"`
	ReservedAt    time.Time `json:"reserved_at"`
}

type reservationRejectedPayload struct {
	ReservationID string `json:"reservation_id"`
	OrderID       string `json:"order_id"`
	SKU           string `json:"sku"`
	Quantity      int    `json:"quantity"`
	Reason        string `json:"reason"`
	Details       string `json:"details"`
}

type reservationReleasedPayload struct {
	ReservationID string    `json:"reservation_id"`
	ReleasedAt    time.Time `json:"released_at"`
}

type reservationReleaseFailedPayload struct {
	ReservationID string `json:"reservation_id"`
	Reason        string `json:"reason"`
	Details       string `json:"details"`
}
