package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/app"
	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/domain/inventory"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	InventoryExchangeName          = "stockflow.inventory"
	ReservationRequestedQueueName  = "stockflow.erp-mock.inventory.reservation.requested.v1"
	ReservationRequestedRoutingKey = "inventory.reservation.requested.v1"
)

type ConsumerConfig struct {
	URL           string
	ConsumerTag   string
	PrefetchCount int
}

type Consumer struct {
	connection  *amqp.Connection
	channel     *amqp.Channel
	consumerTag string
	logger      *slog.Logger
	closeOnce   sync.Once
}

type ReservationResultPublisher interface {
	PublishReservationResult(ctx context.Context, result app.ReservationResult) error
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
		connection:  connection,
		channel:     channel,
		consumerTag: config.ConsumerTag,
		logger:      logger,
	}
	if err := consumer.declareTopology(config.PrefetchCount); err != nil {
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

func (c *Consumer) declareTopology(prefetchCount int) error {
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

	if _, err := c.channel.QueueDeclare(
		ReservationRequestedQueueName,
		true,
		false,
		false,
		false,
		nil,
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
		requeue := !isPermanentHandlerError(err)
		c.logger.Error(
			"process reservation request",
			"message_id", request.Metadata.MessageID,
			"correlation_id", request.Metadata.CorrelationID,
			"reservation_id", request.ReservationID,
			"requeue", requeue,
			"error", err,
		)

		return nack(delivery, requeue)
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

		return nack(delivery, true)
	}

	c.logger.Info(
		"reservation request processed",
		"message_id", request.Metadata.MessageID,
		"correlation_id", request.Metadata.CorrelationID,
		"reservation_id", request.ReservationID,
		"decision", result.Decision,
		"idempotency_hit", result.IdempotencyHit,
	)

	if err := delivery.Ack(false); err != nil {
		return fmt.Errorf("ack RabbitMQ delivery: %w", err)
	}

	return nil
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
