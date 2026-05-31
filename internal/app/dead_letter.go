package app

import (
	"context"
	"errors"
)

type DeadLetterQueue string

const (
	DeadLetterQueueReservationRequests        DeadLetterQueue = "reservation_requests"
	DeadLetterQueueReservationReleaseRequests DeadLetterQueue = "reservation_release_requests"
)

var (
	ErrInvalidDeadLetterQueue = errors.New("invalid dead-letter queue")
	ErrInvalidRequeueLimit    = errors.New("invalid requeue limit")
)

type DeadLetterAdmin interface {
	DLQDepth(ctx context.Context) (map[DeadLetterQueue]int, error)
	RequeueDeadLetters(ctx context.Context, queue DeadLetterQueue, limit int) (int, error)
}
