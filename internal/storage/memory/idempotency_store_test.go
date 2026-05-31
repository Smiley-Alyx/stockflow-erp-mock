package memory

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestIdempotencyStoreReturnsCachedResult(t *testing.T) {
	store := NewIdempotencyStore[string]()
	var calls int

	firstResult, firstHit, err := store.Execute(context.Background(), "key-1", func() (string, error) {
		calls++
		return "result-1", nil
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	secondResult, secondHit, err := store.Execute(context.Background(), "key-1", func() (string, error) {
		calls++
		return "result-2", nil
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	if firstResult != "result-1" || firstHit {
		t.Errorf("first Execute() = (%q, %t), want (%q, false)", firstResult, firstHit, "result-1")
	}
	if secondResult != "result-1" || !secondHit {
		t.Errorf("second Execute() = (%q, %t), want (%q, true)", secondResult, secondHit, "result-1")
	}
	if calls != 1 {
		t.Errorf("calls = %d, want %d", calls, 1)
	}
}

func TestIdempotencyStoreDoesNotCacheErrors(t *testing.T) {
	store := NewIdempotencyStore[string]()
	expectedError := errors.New("operation failed")

	if _, _, err := store.Execute(context.Background(), "key-1", func() (string, error) {
		return "", expectedError
	}); !errors.Is(err, expectedError) {
		t.Fatalf("Execute() error = %v, want %v", err, expectedError)
	}

	result, hit, err := store.Execute(context.Background(), "key-1", func() (string, error) {
		return "result-1", nil
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if result != "result-1" || hit {
		t.Errorf("Execute() = (%q, %t), want (%q, false)", result, hit, "result-1")
	}
}

func TestIdempotencyStoreCoordinatesConcurrentCalls(t *testing.T) {
	const attempts = 100

	store := NewIdempotencyStore[string]()
	start := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int64
	var hits atomic.Int64
	var waitGroup sync.WaitGroup

	for range attempts {
		waitGroup.Add(1)

		go func() {
			defer waitGroup.Done()
			<-start

			result, hit, err := store.Execute(context.Background(), "key-1", func() (string, error) {
				calls.Add(1)
				<-release
				return "result-1", nil
			})
			if err != nil {
				t.Errorf("Execute() error = %v", err)
			}
			if result != "result-1" {
				t.Errorf("result = %q, want %q", result, "result-1")
			}
			if hit {
				hits.Add(1)
			}
		}()
	}

	close(start)
	close(release)
	waitGroup.Wait()

	if calls.Load() != 1 {
		t.Errorf("calls = %d, want %d", calls.Load(), 1)
	}
	if hits.Load() != attempts-1 {
		t.Errorf("hits = %d, want %d", hits.Load(), attempts-1)
	}
}

func TestIdempotencyStoreRejectsEmptyKey(t *testing.T) {
	store := NewIdempotencyStore[string]()

	if _, _, err := store.Execute(context.Background(), " ", func() (string, error) {
		return "result-1", nil
	}); !errors.Is(err, ErrInvalidIdempotencyKey) {
		t.Fatalf("Execute() error = %v, want ErrInvalidIdempotencyKey", err)
	}
}
