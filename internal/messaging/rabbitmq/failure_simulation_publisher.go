package rabbitmq

import (
	"context"
	"errors"
	"fmt"

	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/app"
	amqp "github.com/rabbitmq/amqp091-go"
)

var ErrSimulatedPublishFailure = errors.New("simulated result publish failure")

type FailureSimulationPublisher struct {
	publisher    ReservationResultPublisher
	failureModes *app.FailureModeController
}

var _ ReservationResultPublisher = (*FailureSimulationPublisher)(nil)

func NewFailureSimulationPublisher(
	publisher ReservationResultPublisher,
	failureModes *app.FailureModeController,
) *FailureSimulationPublisher {
	return &FailureSimulationPublisher{
		publisher:    publisher,
		failureModes: failureModes,
	}
}

func (p *FailureSimulationPublisher) PublishReservationResult(ctx context.Context, result app.ReservationResult) error {
	return p.publishResult(func() error {
		return p.publisher.PublishReservationResult(ctx, result)
	})
}

func (p *FailureSimulationPublisher) PublishReservationReleaseResult(
	ctx context.Context,
	result app.ReservationReleaseResult,
) error {
	return p.publishResult(func() error {
		return p.publisher.PublishReservationReleaseResult(ctx, result)
	})
}

func (p *FailureSimulationPublisher) PublishRetry(
	ctx context.Context,
	delivery amqp.Delivery,
	routingKey string,
	retryCount int,
) error {
	return p.publisher.PublishRetry(ctx, delivery, routingKey, retryCount)
}

func (p *FailureSimulationPublisher) PublishDeadLetter(
	ctx context.Context,
	delivery amqp.Delivery,
	routingKey string,
	retryCount int,
) error {
	return p.publisher.PublishDeadLetter(ctx, delivery, routingKey, retryCount)
}

func (p *FailureSimulationPublisher) publishResult(publish func() error) error {
	switch p.failureModes.Get().Mode {
	case app.FailureModePublishFailure:
		return ErrSimulatedPublishFailure
	case app.FailureModeDuplicateResponse:
		if err := publish(); err != nil {
			return err
		}
		if err := publish(); err != nil {
			return fmt.Errorf("publish duplicate result: %w", err)
		}

		return nil
	default:
		return publish()
	}
}
