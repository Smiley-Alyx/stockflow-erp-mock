package httpapi

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/app"
	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/domain/inventory"
	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/storage/memory"
)

func TestHealth(t *testing.T) {
	server, _ := newTestServer(t, nil)
	recorder := httptest.NewRecorder()

	server.httpServer.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/health", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusOK)
	}
	if body := recorder.Body.String(); body != "{\"status\":\"ok\"}\n" {
		t.Errorf("body = %q, want %q", body, "{\"status\":\"ok\"}\n")
	}
}

func TestReady(t *testing.T) {
	server, _ := newTestServer(t, nil)

	t.Run("not ready", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		server.httpServer.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/ready", nil))

		if recorder.Code != http.StatusServiceUnavailable {
			t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
		}
	})

	t.Run("ready", func(t *testing.T) {
		server.SetReady(true)
		recorder := httptest.NewRecorder()
		server.httpServer.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/ready", nil))

		if recorder.Code != http.StatusOK {
			t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusOK)
		}
	})
}

func TestListStock(t *testing.T) {
	server, _ := newTestServer(t, []memory.StockSeed{{SKU: "sku-1", AvailableQuantity: 10}})
	recorder := httptest.NewRecorder()

	server.httpServer.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/stock", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusOK)
	}
	if body := recorder.Body.String(); body != "{\"items\":[{\"sku\":\"sku-1\",\"available_quantity\":10,\"reserved_quantity\":0}]}\n" {
		t.Errorf("body = %q", body)
	}
}

func TestSetStock(t *testing.T) {
	server, _ := newTestServer(t, nil)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/stock", bytes.NewBufferString(
		`{"sku":"sku-1","available_quantity":25}`,
	))

	server.httpServer.Handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusOK)
	}
	if body := recorder.Body.String(); body != "{\"sku\":\"sku-1\",\"available_quantity\":25,\"reserved_quantity\":0}\n" {
		t.Errorf("body = %q", body)
	}
}

func TestSetStockRejectsUnknownJSONField(t *testing.T) {
	server, _ := newTestServer(t, nil)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/stock", bytes.NewBufferString(
		`{"sku":"sku-1","available_quantity":25,"unexpected":true}`,
	))

	server.httpServer.Handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
}

func TestListAndGetReservations(t *testing.T) {
	server, repository := newTestServer(t, []memory.StockSeed{{SKU: "sku-1", AvailableQuantity: 10}})
	if _, err := repository.Reserve(context.Background(), "reservation-1", "sku-1", 4); err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}

	t.Run("list reservations", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		server.httpServer.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/reservations", nil))

		if recorder.Code != http.StatusOK {
			t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusOK)
		}
		if body := recorder.Body.String(); !bytes.Contains([]byte(body), []byte(`"id":"reservation-1"`)) {
			t.Errorf("body = %q, want reservation ID", body)
		}
	})

	t.Run("get reservation", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		server.httpServer.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/reservations/reservation-1", nil))

		if recorder.Code != http.StatusOK {
			t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusOK)
		}
		if body := recorder.Body.String(); !bytes.Contains([]byte(body), []byte(`"status":"active"`)) {
			t.Errorf("body = %q, want active status", body)
		}
	})
}

func TestGetReservationReturnsNotFound(t *testing.T) {
	server, _ := newTestServer(t, nil)
	recorder := httptest.NewRecorder()

	server.httpServer.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/reservations/missing", nil))

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusNotFound)
	}
	if body := recorder.Body.String(); !bytes.Contains([]byte(body), []byte(`"code":"not_found"`)) {
		t.Errorf("body = %q, want not_found code", body)
	}
}

func TestSetFailureMode(t *testing.T) {
	server, _ := newTestServer(t, nil)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/debug/failure-mode", bytes.NewBufferString(
		`{"mode":"processing_delay","processing_delay_ms":250}`,
	))

	server.httpServer.Handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusOK)
	}
	if body := recorder.Body.String(); body != "{\"mode\":\"processing_delay\",\"processing_delay_ms\":250}\n" {
		t.Errorf("body = %q", body)
	}
}

func TestSetFailureModeRejectsInvalidSettings(t *testing.T) {
	server, _ := newTestServer(t, nil)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/debug/failure-mode", bytes.NewBufferString(
		`{"mode":"random_reject"}`,
	))

	server.httpServer.Handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
}

func TestMetrics(t *testing.T) {
	server, _ := newTestServer(t, nil)
	recorder := httptest.NewRecorder()

	server.httpServer.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusOK)
	}
	if body := recorder.Body.String(); body != "test_metric 1\n" {
		t.Errorf("body = %q, want test metric", body)
	}
}

func TestRequeueDeadLetters(t *testing.T) {
	server, _ := newTestServer(t, nil)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/debug/dlq/requeue", bytes.NewBufferString(
		`{"queue":"reservation_requests","limit":10}`,
	))

	server.httpServer.Handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusOK)
	}
	if body := recorder.Body.String(); body != "{\"requeued_messages\":2}\n" {
		t.Errorf("body = %q", body)
	}
}

func newTestServer(t *testing.T, seed []memory.StockSeed) (*Server, *memory.InventoryRepository) {
	t.Helper()

	repository, err := memory.NewInventoryRepository(inventory.NewService(), seed)
	if err != nil {
		t.Fatalf("NewInventoryRepository() error = %v", err)
	}

	metricsHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "test_metric 1\n")
	})

	return New(
		":8080",
		discardLogger(),
		repository,
		app.NewFailureModeController(),
		metricsHandler,
		fakeDeadLetterAdmin{},
	), repository
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type fakeDeadLetterAdmin struct{}

func (fakeDeadLetterAdmin) DLQDepth(context.Context) (map[app.DeadLetterQueue]int, error) {
	return map[app.DeadLetterQueue]int{}, nil
}

func (fakeDeadLetterAdmin) RequeueDeadLetters(context.Context, app.DeadLetterQueue, int) (int, error) {
	return 2, nil
}
