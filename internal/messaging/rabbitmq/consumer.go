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
	InventoryDeadLetterExchangeName                = "stockflow.inventory.dlx"
	InventoryExchangeName                          = "stockflow.inventory"
	InventoryRetryExchangeName                     = "stockflow.inventory.retry"
	ReservationReleaseRequestedDeadLetterQueueName = "stockflow.erp-mock.inventory.reservation.release.requested.v1.dlq"
	ReservationReleaseRequestedQueueName           = "stockflow.erp-mock.inventory.reservation.release.requested.v1"
	ReservationReleaseRequestedRetryQueueName      = "stockflow.erp-mock.inventory.reservation.release.requested.v1.retry"
	ReservationReleaseRequestedRoutingKey          = "inventory.reservation.release.requested.v1"
	ReservationRequestedDeadLetterQueueName        = "stockflow.erp-mock.inventory.reservation.requested.v1.dlq"
	ReservationRequestedQueueName                  = "stockflow.erp-mock.inventory.reservation.requested.v1"
	ReservationRequestedRetryQueueName             = "stockflow.erp-mock.inventory.reservation.requested.v1.retry"
	ReservationRequestedRoutingKey                 = "inventory.reservation.requested.v1"
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
	metrics       ConsumerMetrics
	closeOnce     sync.Once
}

type ConsumerMetrics interface {
	ObserveProcessed(messageType, outcome string, duration time.Duration)
	IncrementFailed(messageType, reason string)
	IncrementRejectedReservation()
	IncrementConfirmedReservation()
	IncrementReleasedReservation()
	IncrementIdempotencyHit()
}

type noopConsumerMetrics struct{}

type ReservationResultPublisher interface {
	PublishReservationResult(ctx context.Context, result app.ReservationResult) error
	PublishReservationReleaseResult(ctx context.Context, result app.ReservationReleaseResult) error
	PublishRetry(ctx context.Context, delivery amqp.Delivery, routingKey string, retryCount int) error
	PublishDeadLetter(ctx context.Context, delivery amqp.Delivery, routingKey string, retryCount int) error
}

func (noopConsumerMetrics) ObserveProcessed(string, string, time.Duration) {}
func (noopConsumerMetrics) IncrementFailed(string, string)                 {}
func (noopConsumerMetrics) IncrementRejectedReservation()                  {}
func (noopConsumerMetrics) IncrementConfirmedReservation()                 {}
func (noopConsumerMetrics) IncrementReleasedReservation()                  {}
func (noopConsumerMetrics) IncrementIdempotencyHit()                       {}

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

func (c *Consumer) SetMetrics(metrics ConsumerMetrics) {
	c.metrics = metrics
}

func (c *Consumer) Consume(
	ctx context.Context,
	handler app.ReservationRequestHandler,
	releaseHandler app.ReservationReleaseRequestHandler,
	publisher ReservationResultPublisher,
) error {
	reservationConsumerTag := c.consumerTag + "-reservation-requested"
	deliveries, err := c.channel.Consume(
		ReservationRequestedQueueName,
		reservationConsumerTag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("start RabbitMQ reservation requested consumer: %w", err)
	}

	releaseConsumerTag := c.consumerTag + "-reservation-release-requested"
	releaseDeliveries, err := c.channel.Consume(
		ReservationReleaseRequestedQueueName,
		releaseConsumerTag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("start RabbitMQ reservation release requested consumer: %w", err)
	}

	c.logger.Info(
		"rabbitmq consumer started",
		"queue", ReservationRequestedQueueName,
		"routing_key", ReservationRequestedRoutingKey,
	)
	c.logger.Info(
		"rabbitmq consumer started",
		"queue", ReservationReleaseRequestedQueueName,
		"routing_key", ReservationReleaseRequestedRoutingKey,
	)

	for {
		select {
		case <-ctx.Done():
			if err := c.channel.Cancel(reservationConsumerTag, false); err != nil && !errors.Is(err, amqp.ErrClosed) {
				return fmt.Errorf("cancel RabbitMQ reservation requested consumer: %w", err)
			}
			if err := c.channel.Cancel(releaseConsumerTag, false); err != nil && !errors.Is(err, amqp.ErrClosed) {
				return fmt.Errorf("cancel RabbitMQ reservation release requested consumer: %w", err)
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
		case delivery, open := <-releaseDeliveries:
			if !open {
				if ctx.Err() != nil {
					return nil
				}

				return fmt.Errorf("RabbitMQ reservation release deliveries channel closed")
			}

			if err := c.handleReleaseDelivery(ctx, delivery, releaseHandler, publisher); err != nil {
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

	if err := c.declareRequestTopology(
		ReservationRequestedQueueName,
		ReservationRequestedRetryQueueName,
		ReservationRequestedDeadLetterQueueName,
		ReservationRequestedRoutingKey,
		retryDelay,
	); err != nil {
		return fmt.Errorf("declare reservation requested topology: %w", err)
	}

	if err := c.declareRequestTopology(
		ReservationReleaseRequestedQueueName,
		ReservationReleaseRequestedRetryQueueName,
		ReservationReleaseRequestedDeadLetterQueueName,
		ReservationReleaseRequestedRoutingKey,
		retryDelay,
	); err != nil {
		return fmt.Errorf("declare reservation release requested topology: %w", err)
	}

	if err := c.channel.Qos(prefetchCount, 0, false); err != nil {
		return fmt.Errorf("configure RabbitMQ prefetch: %w", err)
	}

	return nil
}

func (c *Consumer) declareRequestTopology(
	queueName string,
	retryQueueName string,
	deadLetterQueueName string,
	routingKey string,
	retryDelay time.Duration,
) error {
	if _, err := c.channel.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-dead-letter-exchange":    InventoryDeadLetterExchangeName,
			"x-dead-letter-routing-key": routingKey,
		},
	); err != nil {
		return fmt.Errorf("declare queue %q: %w", queueName, err)
	}

	if err := c.channel.QueueBind(queueName, routingKey, InventoryExchangeName, false, nil); err != nil {
		return fmt.Errorf("bind queue %q: %w", queueName, err)
	}

	if _, err := c.channel.QueueDeclare(
		retryQueueName,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-dead-letter-exchange":    InventoryExchangeName,
			"x-dead-letter-routing-key": routingKey,
			"x-message-ttl":             retryDelay.Milliseconds(),
		},
	); err != nil {
		return fmt.Errorf("declare retry queue %q: %w", retryQueueName, err)
	}

	if err := c.channel.QueueBind(retryQueueName, routingKey, InventoryRetryExchangeName, false, nil); err != nil {
		return fmt.Errorf("bind retry queue %q: %w", retryQueueName, err)
	}

	if _, err := c.channel.QueueDeclare(deadLetterQueueName, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare dead-letter queue %q: %w", deadLetterQueueName, err)
	}

	if err := c.channel.QueueBind(deadLetterQueueName, routingKey, InventoryDeadLetterExchangeName, false, nil); err != nil {
		return fmt.Errorf("bind dead-letter queue %q: %w", deadLetterQueueName, err)
	}

	return nil
}

func (c *Consumer) handleDelivery(
	ctx context.Context,
	delivery amqp.Delivery,
	handler app.ReservationRequestHandler,
	publisher ReservationResultPublisher,
) error {
	startedAt := time.Now()
	request, err := decodeReservationRequested(delivery)
	if err != nil {
		c.logger.Warn("reject invalid RabbitMQ message", "error", err)
		c.observeFailure(ReservationRequestedRoutingKey, "invalid_message", startedAt)
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
		c.observeFailure(ReservationRequestedRoutingKey, "handler_error", startedAt)

		if isPermanentHandlerError(err) {
			return nack(delivery, false)
		}

		return c.retryOrDeadLetter(ctx, delivery, ReservationRequestedRoutingKey, request.Metadata.RetryCount, publisher)
	}
	if result.IdempotencyHit {
		c.consumerMetrics().IncrementIdempotencyHit()
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
		c.observeFailure(ReservationRequestedRoutingKey, "publisher_error", startedAt)

		return c.retryOrDeadLetter(ctx, delivery, ReservationRequestedRoutingKey, request.Metadata.RetryCount, publisher)
	}

	c.logger.Info(
		"reservation request processed",
		"message_id", request.Metadata.MessageID,
		"correlation_id", request.Metadata.CorrelationID,
		"reservation_id", request.ReservationID,
		"decision", result.Decision,
		"idempotency_hit", result.IdempotencyHit,
	)
	c.consumerMetrics().ObserveProcessed(ReservationRequestedRoutingKey, string(result.Decision), time.Since(startedAt))
	switch result.Decision {
	case app.ReservationDecisionConfirmed:
		c.consumerMetrics().IncrementConfirmedReservation()
	case app.ReservationDecisionRejected:
		c.consumerMetrics().IncrementRejectedReservation()
	}

	return ack(delivery)
}

func (c *Consumer) handleReleaseDelivery(
	ctx context.Context,
	delivery amqp.Delivery,
	handler app.ReservationReleaseRequestHandler,
	publisher ReservationResultPublisher,
) error {
	startedAt := time.Now()
	request, err := decodeReservationReleaseRequested(delivery)
	if err != nil {
		c.logger.Warn("reject invalid RabbitMQ release message", "error", err)
		c.observeFailure(ReservationReleaseRequestedRoutingKey, "invalid_message", startedAt)
		return nack(delivery, false)
	}

	result, err := handler.HandleReservationReleaseRequested(ctx, request)
	if err != nil {
		c.logger.Error(
			"process reservation release request",
			"message_id", request.Metadata.MessageID,
			"correlation_id", request.Metadata.CorrelationID,
			"reservation_id", request.ReservationID,
			"error", err,
		)
		c.observeFailure(ReservationReleaseRequestedRoutingKey, "handler_error", startedAt)

		if isPermanentHandlerError(err) {
			return nack(delivery, false)
		}

		return c.retryOrDeadLetter(
			ctx,
			delivery,
			ReservationReleaseRequestedRoutingKey,
			request.Metadata.RetryCount,
			publisher,
		)
	}
	if result.IdempotencyHit {
		c.consumerMetrics().IncrementIdempotencyHit()
	}

	if err := publisher.PublishReservationReleaseResult(ctx, result); err != nil {
		c.logger.Error(
			"publish reservation release result",
			"message_id", request.Metadata.MessageID,
			"correlation_id", request.Metadata.CorrelationID,
			"reservation_id", request.ReservationID,
			"decision", result.Decision,
			"error", err,
		)
		c.observeFailure(ReservationReleaseRequestedRoutingKey, "publisher_error", startedAt)

		return c.retryOrDeadLetter(
			ctx,
			delivery,
			ReservationReleaseRequestedRoutingKey,
			request.Metadata.RetryCount,
			publisher,
		)
	}

	c.logger.Info(
		"reservation release request processed",
		"message_id", request.Metadata.MessageID,
		"correlation_id", request.Metadata.CorrelationID,
		"reservation_id", request.ReservationID,
		"decision", result.Decision,
		"idempotency_hit", result.IdempotencyHit,
	)
	c.consumerMetrics().ObserveProcessed(
		ReservationReleaseRequestedRoutingKey,
		string(result.Decision),
		time.Since(startedAt),
	)
	if result.Decision == app.ReservationReleaseDecisionReleased {
		c.consumerMetrics().IncrementReleasedReservation()
	}

	return ack(delivery)
}

func (c *Consumer) observeFailure(messageType, reason string, startedAt time.Time) {
	c.consumerMetrics().IncrementFailed(messageType, reason)
	c.consumerMetrics().ObserveProcessed(messageType, "failed", time.Since(startedAt))
}

func (c *Consumer) consumerMetrics() ConsumerMetrics {
	if c.metrics == nil {
		return noopConsumerMetrics{}
	}

	return c.metrics
}

func (c *Consumer) retryOrDeadLetter(
	ctx context.Context,
	delivery amqp.Delivery,
	routingKey string,
	retryCount int,
	publisher ReservationResultPublisher,
) error {
	nextRetryCount := retryCount + 1
	if nextRetryCount > c.maxRetryCount {
		if err := publisher.PublishDeadLetter(ctx, delivery, routingKey, retryCount); err != nil {
			c.logger.Error("publish message to DLQ", "retry_count", retryCount, "error", err)
			return nack(delivery, true)
		}

		c.logger.Warn("message moved to DLQ", "retry_count", retryCount)

		return ack(delivery)
	}

	if err := publisher.PublishRetry(ctx, delivery, routingKey, nextRetryCount); err != nil {
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
