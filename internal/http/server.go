package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/app"
	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/domain/inventory"
)

const maxRequestBodyBytes = 1 << 20

type Server struct {
	httpServer          *http.Server
	inventoryRepository inventory.Repository
	failureModes        *app.FailureModeController
	ready               atomic.Bool
}

type statusResponse struct {
	Status string `json:"status"`
}

type stockItemResponse struct {
	SKU               string `json:"sku"`
	AvailableQuantity int    `json:"available_quantity"`
	ReservedQuantity  int    `json:"reserved_quantity"`
}

type stockListResponse struct {
	Items []stockItemResponse `json:"items"`
}

type setStockRequest struct {
	SKU               string `json:"sku"`
	AvailableQuantity int    `json:"available_quantity"`
}

type reservationResponse struct {
	ID         string                      `json:"id"`
	SKU        string                      `json:"sku"`
	Quantity   int                         `json:"quantity"`
	Status     inventory.ReservationStatus `json:"status"`
	CreatedAt  time.Time                   `json:"created_at"`
	ReleasedAt *time.Time                  `json:"released_at,omitempty"`
}

type reservationListResponse struct {
	Items []reservationResponse `json:"items"`
}

type failureModeRequest struct {
	Mode                    app.FailureMode `json:"mode"`
	RandomRejectProbability float64         `json:"random_reject_probability,omitempty"`
	ProcessingDelayMS       int64           `json:"processing_delay_ms,omitempty"`
}

type failureModeResponse struct {
	Mode                    app.FailureMode `json:"mode"`
	RandomRejectProbability float64         `json:"random_reject_probability,omitempty"`
	ProcessingDelayMS       int64           `json:"processing_delay_ms,omitempty"`
}

type errorResponse struct {
	Error apiError `json:"error"`
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func New(
	address string,
	logger *slog.Logger,
	inventoryRepository inventory.Repository,
	failureModes *app.FailureModeController,
) *Server {
	server := &Server{
		inventoryRepository: inventoryRepository,
		failureModes:        failureModes,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", server.handleHealth)
	mux.HandleFunc("GET /ready", server.handleReady)
	mux.HandleFunc("GET /stock", server.handleListStock)
	mux.HandleFunc("POST /stock", server.handleSetStock)
	mux.HandleFunc("GET /reservations", server.handleListReservations)
	mux.HandleFunc("GET /reservations/{id}", server.handleGetReservation)
	mux.HandleFunc("POST /debug/failure-mode", server.handleSetFailureMode)

	server.httpServer = &http.Server{
		Addr:    address,
		Handler: requestLogger(logger, mux),
	}

	return server
}

func (s *Server) ListenAndServe() error {
	return s.httpServer.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) SetReady(ready bool) {
	s.ready.Store(ready)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, statusResponse{Status: "ok"})
}

func (s *Server) handleReady(w http.ResponseWriter, _ *http.Request) {
	if !s.ready.Load() {
		writeJSON(w, http.StatusServiceUnavailable, statusResponse{Status: "not_ready"})
		return
	}

	writeJSON(w, http.StatusOK, statusResponse{Status: "ready"})
}

func (s *Server) handleListStock(w http.ResponseWriter, r *http.Request) {
	items, err := s.inventoryRepository.ListStock(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}

	responseItems := make([]stockItemResponse, 0, len(items))
	for _, item := range items {
		responseItems = append(responseItems, newStockItemResponse(item))
	}

	writeJSON(w, http.StatusOK, stockListResponse{Items: responseItems})
}

func (s *Server) handleSetStock(w http.ResponseWriter, r *http.Request) {
	var request setStockRequest
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, err)
		return
	}

	item, err := s.inventoryRepository.SetStock(r.Context(), request.SKU, request.AvailableQuantity)
	if err != nil {
		writeError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, newStockItemResponse(item))
}

func (s *Server) handleListReservations(w http.ResponseWriter, r *http.Request) {
	reservations, err := s.inventoryRepository.ListReservations(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}

	responseItems := make([]reservationResponse, 0, len(reservations))
	for _, reservation := range reservations {
		responseItems = append(responseItems, newReservationResponse(reservation))
	}

	writeJSON(w, http.StatusOK, reservationListResponse{Items: responseItems})
}

func (s *Server) handleGetReservation(w http.ResponseWriter, r *http.Request) {
	reservation, err := s.inventoryRepository.GetReservation(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, newReservationResponse(reservation))
}

func (s *Server) handleSetFailureMode(w http.ResponseWriter, r *http.Request) {
	var request failureModeRequest
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, err)
		return
	}

	settings := app.FailureModeSettings{
		Mode:                    request.Mode,
		RandomRejectProbability: request.RandomRejectProbability,
		ProcessingDelay:         time.Duration(request.ProcessingDelayMS) * time.Millisecond,
	}
	if err := s.failureModes.Set(settings); err != nil {
		writeError(w, fmt.Errorf("%w: %v", inventory.ErrInvalidArgument, err))
		return
	}

	writeJSON(w, http.StatusOK, newFailureModeResponse(settings))
}

func newStockItemResponse(item inventory.StockItem) stockItemResponse {
	return stockItemResponse{
		SKU:               item.SKU,
		AvailableQuantity: item.AvailableQuantity,
		ReservedQuantity:  item.ReservedQuantity,
	}
}

func newReservationResponse(reservation inventory.Reservation) reservationResponse {
	return reservationResponse{
		ID:         reservation.ID,
		SKU:        reservation.SKU,
		Quantity:   reservation.Quantity,
		Status:     reservation.Status,
		CreatedAt:  reservation.CreatedAt,
		ReleasedAt: reservation.ReleasedAt,
	}
}

func newFailureModeResponse(settings app.FailureModeSettings) failureModeResponse {
	return failureModeResponse{
		Mode:                    settings.Mode,
		RandomRejectProbability: settings.RandomRejectProbability,
		ProcessingDelayMS:       settings.ProcessingDelay.Milliseconds(),
	}
}

func decodeJSON(w http.ResponseWriter, r *http.Request, destination any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("%w: decode request body: %v", inventory.ErrInvalidArgument, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: request body must contain a single JSON object", inventory.ErrInvalidArgument)
	}

	return nil
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	code := "internal_error"

	switch {
	case errors.Is(err, inventory.ErrInvalidArgument):
		status = http.StatusBadRequest
		code = "invalid_argument"
	case errors.Is(err, inventory.ErrStockItemNotFound), errors.Is(err, inventory.ErrReservationNotFound):
		status = http.StatusNotFound
		code = "not_found"
	case errors.Is(err, inventory.ErrReservationAlreadyExists):
		status = http.StatusConflict
		code = "conflict"
	}

	writeJSON(w, status, errorResponse{
		Error: apiError{
			Code:    code,
			Message: err.Error(),
		},
	})
}

func requestLogger(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.Info("http request", "method", r.Method, "path", r.URL.Path)
		next.ServeHTTP(w, r)
	})
}
