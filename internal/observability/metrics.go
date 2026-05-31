package observability

import (
	"context"
	"net/http"
	"time"

	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/app"
	"github.com/Smiley-Alyx/stockflow-erp-mock/internal/domain/inventory"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const namespace = "stockflow_erp_mock"

type Metrics struct {
	registry              *prometheus.Registry
	processedMessages     *prometheus.CounterVec
	failedMessages        *prometheus.CounterVec
	rejectedReservations  prometheus.Counter
	confirmedReservations prometheus.Counter
	releasedReservations  prometheus.Counter
	idempotencyHits       prometheus.Counter
	processingDuration    *prometheus.HistogramVec
}

type runtimeCollector struct {
	repository      inventory.Repository
	deadLetterAdmin app.DeadLetterAdmin
	currentStock    *prometheus.Desc
	active          *prometheus.Desc
	dlqDepth        *prometheus.Desc
}

func New(repository inventory.Repository, deadLetterAdmin app.DeadLetterAdmin) (*Metrics, error) {
	metrics := &Metrics{
		registry: prometheus.NewRegistry(),
		processedMessages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "processed_messages_total",
			Help:      "Number of messages processed by the ERP mock.",
		}, []string{"message_type", "outcome"}),
		failedMessages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "failed_messages_total",
			Help:      "Number of messages that failed processing.",
		}, []string{"message_type", "reason"}),
		rejectedReservations: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "rejected_reservations_total",
			Help:      "Number of rejected inventory reservations.",
		}),
		confirmedReservations: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "confirmed_reservations_total",
			Help:      "Number of confirmed inventory reservations.",
		}),
		releasedReservations: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "released_reservations_total",
			Help:      "Number of released inventory reservations.",
		}),
		idempotencyHits: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "idempotency_hits_total",
			Help:      "Number of safely reused idempotent processing results.",
		}),
		processingDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "message_processing_duration_seconds",
			Help:      "Message processing duration in seconds.",
		}, []string{"message_type"}),
	}

	collector := newRuntimeCollector(repository, deadLetterAdmin)
	if err := metrics.registry.Register(collector); err != nil {
		return nil, err
	}
	for _, collector := range []prometheus.Collector{
		metrics.processedMessages,
		metrics.failedMessages,
		metrics.rejectedReservations,
		metrics.confirmedReservations,
		metrics.releasedReservations,
		metrics.idempotencyHits,
		metrics.processingDuration,
	} {
		if err := metrics.registry.Register(collector); err != nil {
			return nil, err
		}
	}

	return metrics, nil
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) ObserveProcessed(messageType, outcome string, duration time.Duration) {
	m.processedMessages.WithLabelValues(messageType, outcome).Inc()
	m.processingDuration.WithLabelValues(messageType).Observe(duration.Seconds())
}

func (m *Metrics) IncrementFailed(messageType, reason string) {
	m.failedMessages.WithLabelValues(messageType, reason).Inc()
}

func (m *Metrics) IncrementRejectedReservation() {
	m.rejectedReservations.Inc()
}

func (m *Metrics) IncrementConfirmedReservation() {
	m.confirmedReservations.Inc()
}

func (m *Metrics) IncrementReleasedReservation() {
	m.releasedReservations.Inc()
}

func (m *Metrics) IncrementIdempotencyHit() {
	m.idempotencyHits.Inc()
}

func newRuntimeCollector(repository inventory.Repository, deadLetterAdmin app.DeadLetterAdmin) *runtimeCollector {
	return &runtimeCollector{
		repository:      repository,
		deadLetterAdmin: deadLetterAdmin,
		currentStock: prometheus.NewDesc(
			namespace+"_current_stock",
			"Current available stock quantity.",
			[]string{"sku"},
			nil,
		),
		active: prometheus.NewDesc(
			namespace+"_active_reservations",
			"Current number of active inventory reservations.",
			nil,
			nil,
		),
		dlqDepth: prometheus.NewDesc(
			namespace+"_dlq_depth",
			"Current number of messages in a dead-letter queue.",
			[]string{"queue"},
			nil,
		),
	}
}

func (c *runtimeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.currentStock
	ch <- c.active
	ch <- c.dlqDepth
}

func (c *runtimeCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	stock, err := c.repository.ListStock(ctx)
	if err != nil {
		ch <- prometheus.NewInvalidMetric(c.currentStock, err)
	} else {
		for _, item := range stock {
			ch <- prometheus.MustNewConstMetric(
				c.currentStock,
				prometheus.GaugeValue,
				float64(item.AvailableQuantity),
				item.SKU,
			)
		}
	}

	reservations, err := c.repository.ListReservations(ctx)
	if err != nil {
		ch <- prometheus.NewInvalidMetric(c.active, err)
	} else {
		active := 0
		for _, reservation := range reservations {
			if reservation.Status == inventory.ReservationStatusActive {
				active++
			}
		}
		ch <- prometheus.MustNewConstMetric(c.active, prometheus.GaugeValue, float64(active))
	}

	depth, err := c.deadLetterAdmin.DLQDepth(ctx)
	if err != nil {
		ch <- prometheus.NewInvalidMetric(c.dlqDepth, err)
	} else {
		for queue, messages := range depth {
			ch <- prometheus.MustNewConstMetric(c.dlqDepth, prometheus.GaugeValue, float64(messages), string(queue))
		}
	}
}
