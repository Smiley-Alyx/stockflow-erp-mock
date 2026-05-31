package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/app"
	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/domain/inventory"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	InventoryDeadLetterExchangeName         = "stockflow.inventory.dlx"
	InventoryExchangeName                   = "stockflow.inventory"
	InventoryRetryExchangeName              = "stockflow.inventory.retry"
	ReservationRequestedDeadLetterQueueName = "stockflow.erp-mock.inventory.reservation.requested.v1.dlq"
	ReservationRequestedQueueName           = "stockflow.erp-mock.inventory.reservation.requested.v1"
	ReservationRequestedRetryQueueName      = "stockflow.erp-mock.inventory.reservation.requested.v1.retry"
	ReservationRequestedRoutingKey          = "inventory.reservation.requested.v1"
)

type ConsumerConfig struct {
	URL           string
	ConsumerTag   string
	MaxRetryCount int
	PrefetchCount int
	RetryDelay    time.Duration
}

type Consumer struct {
	connection    *amqp.Connection
	channel       *amqp.Channel
	consumerTag   string
	logger        *slog.Logger
	maxRetryCount int
	closeOnce     sync.Once
}

type ReservationResultPublisher interface {
	PublishReservationResult(ctx context.Context, result app.ReservationResult) error
	PublishRetry(ctx context.Context, delivery amqp.Delivery, retryCount int) error
	PublishDeadLetter(ctx context.Context, delivery amqp.Delivery, retryCount int) error
}

func NewConsumer(config ConsumerConfig, logger *slog.Logger) (*Consumer, error) {
	connection, err := amqp.Dial(config.URL)
	if err != nil {
		return nil, fmt.Errorf("dial RabbitMQ: %w", err)
	}

	channel, err := connection.Channel()
	if err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("open RabbitMQ channel: %w", err)
	}

	consumer := &Consumer{
		connection:    connection,
		channel:       channel,
		consumerTag:   config.ConsumerTag,
		logger:        logger,
		maxRetryCount: config.MaxRetryCount,
	}
	if err := consumer.declareTopology(config.PrefetchCount, config.RetryDelay); err != nil {
		_ = consumer.Close()
		return nil, err
	}

	return consumer, nil
}

func (c *Consumer) Consume(
	ctx context.Context,
	handler app.ReservationRequestHandler,
	publisher ReservationResultPublisher,
) error {
	deliveries, err := c.channel.Consume(
		ReservationRequestedQueueName,
		c.consumerTag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("start RabbitMQ consumer: %w", err)
	}

	c.logger.Info(
		"rabbitmq consumer started",
		"queue", ReservationRequestedQueueName,
		"routing_key", ReservationRequestedRoutingKey,
	)

	for {
		select {
		case <-ctx.Done():
			if err := c.channel.Cancel(c.consumerTag, false); err != nil && !errors.Is(err, amqp.ErrClosed) {
				return fmt.Errorf("cancel RabbitMQ consumer: %w", err)
			}

			return nil
		case delivery, open := <-deliveries:
			if !open {
				if ctx.Err() != nil {
					return nil
				}

				return fmt.Errorf("RabbitMQ deliveries channel closed")
			}

			if err := c.handleDelivery(ctx, delivery, handler, publisher); err != nil {
				return err
			}
		}
	}
}

func (c *Consumer) Close() error {
	var closeError error

	c.closeOnce.Do(func() {
		if err := c.channel.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			closeError = fmt.Errorf("close RabbitMQ channel: %w", err)
		}
		if err := c.connection.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) && closeError == nil {
			closeError = fmt.Errorf("close RabbitMQ connection: %w", err)
		}
	})

	return closeError
}

func (c *Consumer) declareTopology(prefetchCount int, retryDelay time.Duration) error {
	if err := c.channel.ExchangeDeclare(
		InventoryExchangeName,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("declare inventory exchange: %w", err)
	}

	if err := c.channel.ExchangeDeclare(
		InventoryRetryExchangeName,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("declare inventory retry exchange: %w", err)
	}

	if err := c.channel.ExchangeDeclare(
		InventoryDeadLetterExchangeName,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("declare inventory dead-letter exchange: %w", err)
	}

	if _, err := c.channel.QueueDeclare(
		ReservationRequestedQueueName,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-dead-letter-exchange":    InventoryDeadLetterExchangeName,
			"x-dead-letter-routing-key": ReservationRequestedRoutingKey,
		},
	); err != nil {
		return fmt.Errorf("declare reservation requested queue: %w", err)
	}

	if err := c.channel.QueueBind(
		ReservationRequestedQueueName,
		ReservationRequestedRoutingKey,
		InventoryExchangeName,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("bind reservation requested queue: %w", err)
	}

	if _, err := c.channel.QueueDeclare(
		ReservationRequestedRetryQueueName,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-dead-letter-exchange":    InventoryExchangeName,
			"x-dead-letter-routing-key": ReservationRequestedRoutingKey,
			"x-message-ttl":             retryDelay.Milliseconds(),
		},
	); err != nil {
		return fmt.Errorf("declare reservation requested retry queue: %w", err)
	}

	if err := c.channel.QueueBind(
		ReservationRequestedRetryQueueName,
		ReservationRequestedRoutingKey,
		InventoryRetryExchangeName,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("bind reservation requested retry queue: %w", err)
	}

	if _, err := c.channel.QueueDeclare(
		ReservationRequestedDeadLetterQueueName,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("declare reservation requested dead-letter queue: %w", err)
	}

	if err := c.channel.QueueBind(
		ReservationRequestedDeadLetterQueueName,
		ReservationRequestedRoutingKey,
		InventoryDeadLetterExchangeName,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("bind reservation requested dead-letter queue: %w", err)
	}

	if err := c.channel.Qos(prefetchCount, 0, false); err != nil {
		return fmt.Errorf("configure RabbitMQ prefetch: %w", err)
	}

	return nil
}

func (c *Consumer) handleDelivery(
	ctx context.Context,
	delivery amqp.Delivery,
	handler app.ReservationRequestHandler,
	publisher ReservationResultPublisher,
) error {
	request, err := decodeReservationRequested(delivery)
	if err != nil {
		c.logger.Warn("reject invalid RabbitMQ message", "error", err)
		return nack(delivery, false)
	}

	result, err := handler.HandleReservationRequested(ctx, request)
	if err != nil {
		c.logger.Error(
			"process reservation request",
			"message_id", request.Metadata.MessageID,
			"correlation_id", request.Metadata.CorrelationID,
			"reservation_id", request.ReservationID,
			"error", err,
		)

		if isPermanentHandlerError(err) {
			return nack(delivery, false)
		}

		return c.retryOrDeadLetter(ctx, delivery, request.Metadata.RetryCount, publisher)
	}

	if err := publisher.PublishReservationResult(ctx, result); err != nil {
		c.logger.Error(
			"publish reservation result",
			"message_id", request.Metadata.MessageID,
			"correlation_id", request.Metadata.CorrelationID,
			"reservation_id", request.ReservationID,
			"decision", result.Decision,
			"error", err,
		)

		return c.retryOrDeadLetter(ctx, delivery, request.Metadata.RetryCount, publisher)
	}

	c.logger.Info(
		"reservation request processed",
		"message_id", request.Metadata.MessageID,
		"correlation_id", request.Metadata.CorrelationID,
		"reservation_id", request.ReservationID,
		"decision", result.Decision,
		"idempotency_hit", result.IdempotencyHit,
	)

	return ack(delivery)
}

func (c *Consumer) retryOrDeadLetter(
	ctx context.Context,
	delivery amqp.Delivery,
	retryCount int,
	publisher ReservationResultPublisher,
) error {
	nextRetryCount := retryCount + 1
	if nextRetryCount > c.maxRetryCount {
		if err := publisher.PublishDeadLetter(ctx, delivery, retryCount); err != nil {
			c.logger.Error("publish message to DLQ", "retry_count", retryCount, "error", err)
			return nack(delivery, true)
		}

		c.logger.Warn("message moved to DLQ", "retry_count", retryCount)

		return ack(delivery)
	}

	if err := publisher.PublishRetry(ctx, delivery, nextRetryCount); err != nil {
		c.logger.Error("publish message for retry", "retry_count", nextRetryCount, "error", err)
		return nack(delivery, true)
	}

	c.logger.Warn("message scheduled for retry", "retry_count", nextRetryCount)

	return ack(delivery)
}

func isPermanentHandlerError(err error) bool {
	return errors.Is(err, inventory.ErrInvalidArgument) ||
		errors.Is(err, inventory.ErrReservationAlreadyExists) ||
		errors.Is(err, app.ErrIdempotencyConflict)
}

func nack(delivery amqp.Delivery, requeue bool) error {
	if err := delivery.Nack(false, requeue); err != nil {
		return fmt.Errorf("nack RabbitMQ delivery: %w", err)
	}

	return nil
}

func ack(delivery amqp.Delivery) error {
	if err := delivery.Ack(false); err != nil {
		return fmt.Errorf("ack RabbitMQ delivery: %w", err)
	}

	return nil
}
