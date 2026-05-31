//go:build integration

package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/app"
	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/domain/inventory"
	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/storage/memory"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type integrationEnvironment struct {
	t                *testing.T
	url              string
	repository       *memory.InventoryRepository
	consumer         *Consumer
	publisher        *Publisher
	connection       *amqp.Connection
	publishChannel   *amqp.Channel
	consumeChannel   *amqp.Channel
	cancelConsumer   context.CancelFunc
	consumerDone     chan error
	consumerTag      string
}

type integrationMessageHeaders struct {
	MessageID      string
	CorrelationID  string
	CausationID    string
	IdempotencyKey string
	OccurredAt     time.Time
	RetryCount     int
}

type integrationReservationRequestedPayload struct {
	ReservationID string `json:"reservation_id"`
	OrderID       string `json:"order_id"`
	SKU           string `json:"sku"`
	Quantity      int    `json:"quantity"`
}

type integrationReservationReleaseRequestedPayload struct {
	ReservationID string `json:"reservation_id"`
	Reason        string `json:"reason"`
}

func newIntegrationEnvironment(t *testing.T) *integrationEnvironment {
	t.Helper()

	url := requireIntegrationRabbitURL(t)
	consumerTag := "integration-" + uuid.NewString()

	repository, err := memory.NewInventoryRepository(inventory.NewService(), memory.DefaultStockSeed())
	if err != nil {
		t.Fatalf("NewInventoryRepository() error = %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	consumer, err := NewConsumer(ConsumerConfig{
		URL:           url,
		ConsumerTag:   consumerTag,
		MaxRetryCount: 3,
		PrefetchCount: 10,
		RetryDelay:    500 * time.Millisecond,
	}, logger)
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}

	publisher, err := NewPublisher(PublisherConfig{
		URL:            url,
		PublishTimeout: integrationPublishTimeout,
	})
	if err != nil {
		closeIntegration(t, consumer, publisher, nil, nil, nil)
		t.Fatalf("NewPublisher() error = %v", err)
	}

	connection, err := amqp.Dial(url)
	if err != nil {
		closeIntegration(t, consumer, publisher, nil, nil, nil)
		t.Fatalf("amqp.Dial() error = %v", err)
	}

	publishChannel, err := connection.Channel()
	if err != nil {
		_ = connection.Close()
		closeIntegration(t, consumer, publisher, nil, nil, nil)
		t.Fatalf("open publish channel: %v", err)
	}

	consumeChannel, err := connection.Channel()
	if err != nil {
		_ = publishChannel.Close()
		_ = connection.Close()
		closeIntegration(t, consumer, publisher, nil, nil, nil)
		t.Fatalf("open consume channel: %v", err)
	}

	if err := purgeIntegrationQueues(publishChannel); err != nil {
		closeIntegration(t, consumer, publisher, connection, publishChannel, consumeChannel)
		t.Fatalf("purge integration queues: %v", err)
	}

	reservationIdempotencyStore := memory.NewIdempotencyStore[app.ReservationResult]()
	releaseIdempotencyStore := memory.NewIdempotencyStore[app.ReservationReleaseResult]()

	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	consumerDone := make(chan error, 1)
	go func() {
		consumerDone <- consumer.Consume(
			consumerCtx,
			app.NewInventoryReservationHandler(repository, reservationIdempotencyStore),
			app.NewInventoryReservationReleaseHandler(repository, releaseIdempotencyStore),
			publisher,
		)
	}()

	env := &integrationEnvironment{
		t:              t,
		url:            url,
		repository:     repository,
		consumer:       consumer,
		publisher:      publisher,
		connection:     connection,
		publishChannel: publishChannel,
		consumeChannel: consumeChannel,
		cancelConsumer: cancelConsumer,
		consumerDone:   consumerDone,
		consumerTag:    consumerTag,
	}

	t.Cleanup(func() {
		cancelConsumer()

		select {
		case err := <-env.consumerDone:
			if err != nil && err != context.Canceled {
				t.Errorf("consumer stopped with error: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("consumer shutdown timed out")
		}

		closeIntegration(t, consumer, publisher, connection, publishChannel, consumeChannel)
	})

	return env
}

func closeIntegration(
	t *testing.T,
	consumer *Consumer,
	publisher *Publisher,
	connection *amqp.Connection,
	publishChannel *amqp.Channel,
	consumeChannel *amqp.Channel,
) {
	t.Helper()

	if publishChannel != nil {
		if err := publishChannel.Close(); err != nil {
			t.Errorf("close publish channel: %v", err)
		}
	}
	if consumeChannel != nil {
		if err := consumeChannel.Close(); err != nil {
			t.Errorf("close consume channel: %v", err)
		}
	}
	if connection != nil {
		if err := connection.Close(); err != nil {
			t.Errorf("close connection: %v", err)
		}
	}
	if publisher != nil {
		if err := publisher.Close(); err != nil {
			t.Errorf("close publisher: %v", err)
		}
	}
	if consumer != nil {
		if err := consumer.Close(); err != nil {
			t.Errorf("close consumer: %v", err)
		}
	}
}

func purgeIntegrationQueues(channel *amqp.Channel) error {
	for _, queueName := range []string{
		ReservationRequestedQueueName,
		ReservationReleaseRequestedQueueName,
		ReservationRequestedRetryQueueName,
		ReservationReleaseRequestedRetryQueueName,
		ReservationRequestedDeadLetterQueueName,
		ReservationReleaseRequestedDeadLetterQueueName,
	} {
		if _, err := channel.QueuePurge(queueName, false); err != nil {
			return fmt.Errorf("purge queue %q: %w", queueName, err)
		}
	}

	return nil
}

func (env *integrationEnvironment) bindResultQueue(routingKey string) string {
	env.t.Helper()

	queue, err := env.consumeChannel.QueueDeclare(
		"",
		false,
		true,
		true,
		false,
		nil,
	)
	if err != nil {
		env.t.Fatalf("QueueDeclare() error = %v", err)
	}

	if err := env.consumeChannel.QueueBind(queue.Name, routingKey, InventoryExchangeName, false, nil); err != nil {
		env.t.Fatalf("QueueBind() error = %v", err)
	}

	return queue.Name
}

func (env *integrationEnvironment) publishReservationRequested(
	headers integrationMessageHeaders,
	payload integrationReservationRequestedPayload,
) {
	env.t.Helper()
	env.publishInbound(ReservationRequestedRoutingKey, headers, payload)
}

func (env *integrationEnvironment) publishReservationReleaseRequested(
	headers integrationMessageHeaders,
	payload integrationReservationReleaseRequestedPayload,
) {
	env.t.Helper()
	env.publishInbound(ReservationReleaseRequestedRoutingKey, headers, payload)
}

func (env *integrationEnvironment) publishInbound(
	routingKey string,
	headers integrationMessageHeaders,
	payload any,
) {
	env.t.Helper()

	body, err := json.Marshal(payload)
	if err != nil {
		env.t.Fatalf("Marshal() error = %v", err)
	}

	if headers.MessageID == "" {
		headers.MessageID = uuid.NewString()
	}
	if headers.CorrelationID == "" {
		headers.CorrelationID = uuid.NewString()
	}
	if headers.CausationID == "" {
		headers.CausationID = uuid.NewString()
	}
	if headers.IdempotencyKey == "" {
		headers.IdempotencyKey = "integration:" + uuid.NewString()
	}
	if headers.OccurredAt.IsZero() {
		headers.OccurredAt = time.Now().UTC()
	}

	publishCtx, cancel := context.WithTimeout(context.Background(), integrationPublishTimeout)
	defer cancel()

	err = env.publishChannel.PublishWithContext(
		publishCtx,
		InventoryExchangeName,
		routingKey,
		false,
		false,
		amqp.Publishing{
			Headers: amqp.Table{
				"message_id":      headers.MessageID,
				"correlation_id":  headers.CorrelationID,
				"causation_id":    headers.CausationID,
				"idempotency_key": headers.IdempotencyKey,
				"occurred_at":     headers.OccurredAt.Format(time.RFC3339),
				"schema_version":  int32(1),
				"retry_count":     int32(headers.RetryCount),
			},
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		},
	)
	if err != nil {
		env.t.Fatalf("PublishWithContext() error = %v", err)
	}
}

func (env *integrationEnvironment) waitForDelivery(queueName string, timeout time.Duration) amqp.Delivery {
	env.t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		delivery, ok, err := env.consumeChannel.Get(queueName, true)
		if err != nil {
			env.t.Fatalf("Get() error = %v", err)
		}
		if ok {
			return delivery
		}

		time.Sleep(50 * time.Millisecond)
	}

	env.t.Fatalf("timed out waiting for message on queue %q", queueName)
	return amqp.Delivery{}
}

func newIntegrationHeaders(idempotencyKey string) integrationMessageHeaders {
	return integrationMessageHeaders{
		MessageID:      uuid.NewString(),
		CorrelationID:  uuid.NewString(),
		CausationID:    uuid.NewString(),
		IdempotencyKey: idempotencyKey,
		OccurredAt:     time.Now().UTC(),
	}
}

func stockQuantity(env *integrationEnvironment, sku string) inventory.StockItem {
	env.t.Helper()

	item, err := env.repository.GetStock(context.Background(), sku)
	if err != nil {
		env.t.Fatalf("GetStock(%q) error = %v", sku, err)
	}

	return item
}
