package observability

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/app"
	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/domain/inventory"
	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/storage/memory"
)

func TestMetricsHandler(t *testing.T) {
	repository, err := memory.NewInventoryRepository(inventory.NewService(), []memory.StockSeed{
		{SKU: "sku-1", AvailableQuantity: 10},
	})
	if err != nil {
		t.Fatalf("NewInventoryRepository() error = %v", err)
	}
	if _, err := repository.Reserve(context.Background(), "reservation-1", "sku-1", 4); err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}

	metrics, err := New(repository, fakeDeadLetterAdmin{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	metrics.ObserveProcessed("inventory.reservation.requested.v1", "confirmed", 25*time.Millisecond)
	metrics.IncrementConfirmedReservation()
	metrics.IncrementIdempotencyHit()

	recorder := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusOK)
	}

	body := recorder.Body.String()
	for _, metric := range []string{
		`stockflow_erp_mock_active_reservations 1`,
		`stockflow_erp_mock_confirmed_reservations_total 1`,
		`stockflow_erp_mock_current_stock{sku="sku-1"} 6`,
		`stockflow_erp_mock_dlq_depth{queue="reservation_requests"} 2`,
		`stockflow_erp_mock_idempotency_hits_total 1`,
		`stockflow_erp_mock_processed_messages_total{message_type="inventory.reservation.requested.v1",outcome="confirmed"} 1`,
	} {
		if !strings.Contains(body, metric) {
			t.Errorf("metrics body does not contain %q", metric)
		}
	}
}

type fakeDeadLetterAdmin struct{}

func (fakeDeadLetterAdmin) DLQDepth(context.Context) (map[app.DeadLetterQueue]int, error) {
	return map[app.DeadLetterQueue]int{
		app.DeadLetterQueueReservationRequests:        2,
		app.DeadLetterQueueReservationReleaseRequests: 1,
	}, nil
}

func (fakeDeadLetterAdmin) RequeueDeadLetters(context.Context, app.DeadLetterQueue, int) (int, error) {
	return 0, nil
}
