package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/app"
	amqp "github.com/rabbitmq/amqp091-go"
)

const maxRequeueLimit = 100

type Admin struct {
	connection     *amqp.Connection
	channel        *amqp.Channel
	confirmations  <-chan amqp.Confirmation
	publishTimeout time.Duration
	closeOnce      sync.Once
	mu             sync.Mutex
}

type deadLetterQueueConfig struct {
	name       string
	routingKey string
}

var _ app.DeadLetterAdmin = (*Admin)(nil)

func NewAdmin(config PublisherConfig) (*Admin, error) {
	connection, err := amqp.Dial(config.URL)
	if err != nil {
		return nil, fmt.Errorf("dial RabbitMQ admin: %w", err)
	}

	channel, err := connection.Channel()
	if err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("open RabbitMQ admin channel: %w", err)
	}

	admin := &Admin{
		connection:     connection,
		channel:        channel,
		publishTimeout: config.PublishTimeout,
	}
	if err := channel.Confirm(false); err != nil {
		_ = admin.Close()
		return nil, fmt.Errorf("enable RabbitMQ admin publisher confirms: %w", err)
	}
	admin.confirmations = channel.NotifyPublish(make(chan amqp.Confirmation, 1))

	return admin, nil
}

func (a *Admin) DLQDepth(ctx context.Context) (map[app.DeadLetterQueue]int, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	depth := make(map[app.DeadLetterQueue]int, 2)
	for _, queue := range []app.DeadLetterQueue{
		app.DeadLetterQueueReservationRequests,
		app.DeadLetterQueueReservationReleaseRequests,
	} {
		config, err := deadLetterConfig(queue)
		if err != nil {
			return nil, err
		}

		inspection, err := a.channel.QueueInspect(config.name)
		if err != nil {
			return nil, fmt.Errorf("inspect RabbitMQ dead-letter queue %q: %w", config.name, err)
		}
		depth[queue] = inspection.Messages
	}

	return depth, nil
}

func (a *Admin) RequeueDeadLetters(ctx context.Context, queue app.DeadLetterQueue, limit int) (int, error) {
	config, err := deadLetterConfig(queue)
	if err != nil {
		return 0, err
	}
	if limit <= 0 || limit > maxRequeueLimit {
		return 0, fmt.Errorf("%w: limit must be between 1 and %d", app.ErrInvalidRequeueLimit, maxRequeueLimit)
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	requeued := 0
	for requeued < limit {
		if err := ctx.Err(); err != nil {
			return requeued, err
		}

		delivery, ok, err := a.channel.Get(config.name, false)
		if err != nil {
			return requeued, fmt.Errorf("get RabbitMQ dead-letter message: %w", err)
		}
		if !ok {
			return requeued, nil
		}

		if err := a.publishWithConfirmation(ctx, config.routingKey, newForwardedDelivery(delivery, 0)); err != nil {
			if nackErr := delivery.Nack(false, true); nackErr != nil {
				return requeued, fmt.Errorf("%w; nack RabbitMQ dead-letter message: %v", err, nackErr)
			}

			return requeued, err
		}
		if err := delivery.Ack(false); err != nil {
			return requeued, fmt.Errorf("ack RabbitMQ dead-letter message: %w", err)
		}

		requeued++
	}

	return requeued, nil
}

func (a *Admin) Close() error {
	var closeError error

	a.closeOnce.Do(func() {
		if err := a.channel.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			closeError = fmt.Errorf("close RabbitMQ admin channel: %w", err)
		}
		if err := a.connection.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) && closeError == nil {
			closeError = fmt.Errorf("close RabbitMQ admin connection: %w", err)
		}
	})

	return closeError
}

func (a *Admin) publishWithConfirmation(ctx context.Context, routingKey string, publishing amqp.Publishing) error {
	publishContext, cancel := context.WithTimeout(ctx, a.publishTimeout)
	defer cancel()

	if err := a.channel.PublishWithContext(
		publishContext,
		InventoryExchangeName,
		routingKey,
		false,
		false,
		publishing,
	); err != nil {
		return fmt.Errorf("requeue RabbitMQ dead-letter message: %w", err)
	}

	select {
	case confirmation, open := <-a.confirmations:
		if !open {
			return fmt.Errorf("wait for RabbitMQ dead-letter requeue confirmation: channel closed")
		}
		if !confirmation.Ack {
			return fmt.Errorf("wait for RabbitMQ dead-letter requeue confirmation: message was not acknowledged")
		}

		return nil
	case <-publishContext.Done():
		return fmt.Errorf("wait for RabbitMQ dead-letter requeue confirmation: %w", publishContext.Err())
	}
}

func deadLetterConfig(queue app.DeadLetterQueue) (deadLetterQueueConfig, error) {
	switch queue {
	case app.DeadLetterQueueReservationRequests:
		return deadLetterQueueConfig{
			name:       ReservationRequestedDeadLetterQueueName,
			routingKey: ReservationRequestedRoutingKey,
		}, nil
	case app.DeadLetterQueueReservationReleaseRequests:
		return deadLetterQueueConfig{
			name:       ReservationReleaseRequestedDeadLetterQueueName,
			routingKey: ReservationReleaseRequestedRoutingKey,
		}, nil
	default:
		return deadLetterQueueConfig{}, fmt.Errorf("%w: %q", app.ErrInvalidDeadLetterQueue, queue)
	}
}
