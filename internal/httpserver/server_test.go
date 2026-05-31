package httpserver

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealth(t *testing.T) {
	server := New(":8080", discardLogger())
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
	server := New(":8080", discardLogger())

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

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
