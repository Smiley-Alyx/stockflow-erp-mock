package memory

import (
	"context"
	"errors"
	"strings"
	"sync"
)

var ErrInvalidIdempotencyKey = errors.New("invalid idempotency key")

type IdempotencyStore[T any] struct {
	mu       sync.Mutex
	entries  map[string]T
	inFlight map[string]*idempotencyCall[T]
}

type idempotencyCall[T any] struct {
	done   chan struct{}
	result T
	err    error
}

func NewIdempotencyStore[T any]() *IdempotencyStore[T] {
	return &IdempotencyStore[T]{
		entries:  make(map[string]T),
		inFlight: make(map[string]*idempotencyCall[T]),
	}
}

func (s *IdempotencyStore[T]) Execute(
	ctx context.Context,
	key string,
	operation func() (T, error),
) (T, bool, error) {
	var zero T

	key = strings.TrimSpace(key)
	if key == "" {
		return zero, false, ErrInvalidIdempotencyKey
	}
	if err := ctx.Err(); err != nil {
		return zero, false, err
	}

	s.mu.Lock()

	if result, exists := s.entries[key]; exists {
		s.mu.Unlock()
		return result, true, nil
	}
	if call, exists := s.inFlight[key]; exists {
		s.mu.Unlock()

		select {
		case <-call.done:
			return call.result, true, call.err
		case <-ctx.Done():
			return zero, true, ctx.Err()
		}
	}

	call := &idempotencyCall[T]{done: make(chan struct{})}
	s.inFlight[key] = call
	s.mu.Unlock()

	result, err := operation()

	s.mu.Lock()
	if err == nil {
		s.entries[key] = result
	}
	call.result = result
	call.err = err
	close(call.done)
	delete(s.inFlight, key)
	s.mu.Unlock()

	return result, false, err
}
